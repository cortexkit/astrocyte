use astrocyte_core::broca::record_scope_in_tx;
use rusqlite::{params, OptionalExtension};

use super::{BrocaSource, PullError, SourceClient, SourceCursor, BROCA_SOURCE_KEY};
use crate::state::SharedState;

impl BrocaSource {
    pub(super) fn backfill_scope_page(
        &self,
        state: &SharedState,
        limit: u32,
    ) -> Result<usize, PullError> {
        let pending = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT r.lineage,r.consumed_ordinal,
                            CASE WHEN b.lineage=r.lineage THEN b.ordinal ELSE 0 END
                     FROM source_runtime r LEFT JOIN broca_scope_backfill b ON b.id=1
                     WHERE r.source_key=?1 AND r.lineage IS NOT NULL
                       AND r.consumed_ordinal IS NOT NULL AND b.done IS NOT 1",
                    [BROCA_SOURCE_KEY],
                    |row| {
                        Ok((
                            SourceCursor {
                                lineage: row.get(0)?,
                                ordinal: row.get(1)?,
                            },
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(|error| error.to_string())
            })
            .map_err(PullError::Store)?;
        let Some((target, start)) = pending else {
            return Ok(0);
        };

        // Read only on the Broca connection, and release it before opening the
        // Astrocyte transaction. Gaps in the retained export need no ledger work.
        let page = self.pull(
            Some(SourceCursor {
                lineage: target.lineage.clone(),
                ordinal: start,
            }),
            limit,
        )?;
        self.refuse_regression(Some(&target), &page)?;
        if page.cursor.lineage != target.lineage {
            // The file changed between the normal drain and this read. Wait
            // until the reader commits a cursor on that lineage.
            return Ok(0);
        }
        let through = page.cursor.ordinal.min(target.ordinal);
        state
            .ordered_tail_write(|tx| {
                let mut recorded = 0;
                for (ordinal, fact) in page.broca_facts.iter().take_while(|(n, _)| *n <= through) {
                    let (id, kind, stamp) = fact.scope();
                    recorded += record_scope_in_tx(tx, id, kind, stamp)
                        .map_err(|error| format!("backfill BROCA scope at {ordinal}: {error:?}"))?;
                }
                tx.execute(
                    "INSERT INTO broca_scope_backfill VALUES(1,?1,?2,?3)
                     ON CONFLICT(id) DO UPDATE SET lineage=excluded.lineage,
                         ordinal=excluded.ordinal,done=excluded.done",
                    params![target.lineage, through, through == target.ordinal],
                )
                .map_err(|error| error.to_string())?;
                Ok((recorded, recorded != 0))
            })
            .map_err(PullError::Store)
    }
}
