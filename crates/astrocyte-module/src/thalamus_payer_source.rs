//! Durable payer facts mirrored before any THALAMUS transcript is read.

use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};
use std::time::SystemTime;

use astrocyte_core::{
    attempt_usage::decode_attempt_usage, fact::ChargeBasis, ingest::EventRecordRequest,
};
use rusqlite::{params, OptionalExtension, Transaction};

use super::{
    harness_drain::{self, DrainMode, HarnessSource},
    SetAsideRecord, SourceDrainError,
};
use crate::state::SharedState;

pub(crate) const SOURCE_KEY: &str = "thalamus-payer";
const PAGE_LIMIT: i64 = 512;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Stamp(Option<(u64, SystemTime)>, Option<(u64, SystemTime)>);

fn stamp(path: &Path) -> Result<Stamp, String> {
    fn one(path: &Path) -> Result<Option<(u64, SystemTime)>, String> {
        match astrocyte_readers::read_io::metadata(path) {
            Ok(meta) => Ok(Some((
                meta.len(),
                meta.modified().map_err(|e| e.to_string())?,
            ))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("inspect payer store {}: {e}", path.display())),
        }
    }
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    Ok(Stamp(one(path)?, one(Path::new(&wal))?))
}

pub(crate) struct ThalamusPayerSource {
    path: PathBuf,
    last_read: Mutex<Option<Stamp>>,
}

impl ThalamusPayerSource {
    pub(crate) fn from_data_home(home: PathBuf) -> Self {
        Self {
            path: home.join("cortexkit/thalamus-harness/payer/payer.db"),
            last_read: Mutex::default(),
        }
    }

    fn drain(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
        mode: DrainMode,
    ) -> Result<(), SourceDrainError> {
        if !self.path.is_absolute() {
            return Err("payer store data home must be absolute".to_string().into());
        }
        let observed = stamp(&self.path)?;
        let mut last = self
            .last_read
            .lock()
            .map_err(|_| "payer read hint lock poisoned".to_string())?;
        if !mode.is_full() && last.as_ref() == Some(&observed) {
            return Ok(());
        }
        if observed.0.is_none() {
            // Absence is a complete empty read. Future rows are written after
            // this pass, so the same safety margin permits session cleanup.
            if mode.is_full() {
                state.ordered_write(|tx| {
                    tx.execute(
                        "DELETE FROM source_runtime WHERE source_key=?1",
                        [SOURCE_KEY],
                    )
                    .map_err(sql)?;
                    store_watermark_in_tx(tx, at)?;
                    Ok(((), true))
                })?;
            }
            *last = Some(observed);
            return Ok(());
        }
        mode.ensure_runtime(state, SOURCE_KEY)?;
        loop {
            if stop.load(Ordering::Relaxed) {
                return Err(SourceDrainError::Stopped);
            }
            let cursor = state.with_conn(|conn| {
                conn.query_row(
                    "SELECT incarnation,through_seq FROM thalamus_payer_cursor WHERE singleton=1",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()
                .map_err(sql)
            })?;
            // The external read ends, including its read transaction and file handle,
            // before the ledger write starts.
            let page = read_page(&self.path, cursor.as_ref())?;
            if stop.load(Ordering::Relaxed) {
                return Err(SourceDrainError::Stopped);
            }
            let complete = page.through >= page.high_water;
            mode.write(state, |tx| {
                harness_drain::ensure_runtime_row_in_tx(tx,SOURCE_KEY).map_err(sql)?;
                for session in &page.sessions {
                    tx.execute("INSERT OR IGNORE INTO thalamus_payer_session(harness,session,owner_principal,declared_at_ms) VALUES (?1,?2,?3,?4)",params![session.harness,session.session,session.owner,session.declared_at]).map_err(sql)?;
                }
                let mut evidence = page.evidence.clone();
                for record in &page.evidence {
                    count_coverage(tx,"set_aside",&record.locator)?;
                }
                for record in &page.rows {
                    if !mirror(tx, &page.incarnation, record)? { continue; }
                    if let Some(reason) = record_row(tx,record,at)? {
                        evidence.push(SetAsideRecord {locator:format!("{}:seq:{}:attempt:{}",self.path.display(),record.seq,record.attempt_id),reason});
                        count_coverage(tx,"set_aside",&record.attempt_id)?;
                    }
                }
                harness_drain::record_set_aside_in_tx(tx,SOURCE_KEY,&evidence,at)?;
                tx.execute("INSERT INTO thalamus_payer_cursor(singleton,incarnation,through_seq) VALUES (1,?1,?2) ON CONFLICT(singleton) DO UPDATE SET incarnation=excluded.incarnation,through_seq=excluded.through_seq",params![page.incarnation,page.through]).map_err(sql)?;
                tx.execute("UPDATE source_runtime SET lineage=?2,consumed_ordinal=?3,high_water_ordinal=?4,last_success_ms=?5,pending_known=?6,pending_unknown=0 WHERE source_key=?1",params![SOURCE_KEY,page.incarnation,page.through,page.high_water,at,page.high_water.saturating_sub(page.through)]).map_err(sql)?;
                if complete {
                    store_watermark_in_tx(tx,at)?;
                }
                Ok(((),true))
            })?;
            if complete {
                *last = Some(observed);
                return Ok(());
            }
        }
    }
}

impl HarnessSource for ThalamusPayerSource {
    fn source_key(&self) -> &'static str {
        SOURCE_KEY
    }
    fn drain_for_health(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        self.drain(state, at, stop, DrainMode::Full)?;
        Ok(None)
    }
    fn drain_fast(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<(), SourceDrainError> {
        self.drain(state, at, stop, DrainMode::Fast)
    }
}

#[derive(Clone, Debug)]
struct Session {
    harness: String,
    session: String,
    owner: Option<String>,
    declared_at: i64,
}

#[derive(Clone, Debug)]
struct Record {
    seq: i64,
    attempt_id: String,
    harness: String,
    session: String,
    request_kind: String,
    step_ref: Option<String>,
    recorded_at: i64,
    provider: String,
    model: String,
    charge_basis: Option<String>,
    auth_selection: String,
    account_ref: Option<String>,
    outcome: Option<String>,
    provider_request_id: Option<String>,
    provider_response_id: Option<String>,
    usage_json: Option<String>,
}

struct Page {
    incarnation: String,
    through: i64,
    high_water: i64,
    sessions: Vec<Session>,
    rows: Vec<Record>,
    evidence: Vec<SetAsideRecord>,
}

fn sql(error: rusqlite::Error) -> String {
    error.to_string()
}

fn store_watermark_in_tx(tx: &Transaction<'_>, at: i64) -> Result<(), String> {
    tx.execute("INSERT INTO read_watermark(source_key,watermark_ms,updated_at_ms) VALUES (?1,?2,?3) ON CONFLICT(source_key) DO UPDATE SET watermark_ms=MAX(watermark_ms,excluded.watermark_ms),updated_at_ms=excluded.updated_at_ms",params![SOURCE_KEY,at.saturating_sub(60_000),at]).map_err(sql)?;
    Ok(())
}

fn read_page(path: &Path, cursor: Option<&(String, i64)>) -> Result<Page, String> {
    let conn =
        astrocyte_readers::read_io::connect(path.to_str().ok_or("payer store path is not UTF-8")?)
            .map_err(sql)?;
    let tx = conn.unchecked_transaction().map_err(sql)?;
    let meta = |key: &str| {
        tx.query_row("SELECT value FROM payer_meta WHERE key=?1", [key], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql)
    };
    let version = meta("schema_version")?;
    if version != "1" {
        return Err(format!("unknown payer schema_version {version}"));
    }
    let incarnation = meta("incarnation")?;
    if incarnation.is_empty() {
        return Err("payer incarnation is empty".into());
    }
    let after = cursor
        .filter(|(old, _)| old == &incarnation)
        .map_or(0, |(_, seq)| *seq);
    let high_water = tx
        .query_row("SELECT COALESCE(MAX(seq),0) FROM payer_record", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(sql)?
        .max(after);
    let sessions = tx
        .prepare("SELECT harness,session,owner_principal,declared_at_ms FROM payer_session")
        .map_err(sql)?
        .query_map([], |row| {
            Ok(Session {
                harness: row.get(0)?,
                session: row.get(1)?,
                owner: row.get(2)?,
                declared_at: row.get(3)?,
            })
        })
        .map_err(sql)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql)?;
    let mut statement = tx.prepare("SELECT seq,attempt_id,harness,session,request_kind,step_ref,recorded_at_ms,provider,model,charge_basis,auth_selection,account_ref,outcome,provider_request_id,provider_response_id,usage_json FROM payer_record WHERE seq>?1 ORDER BY seq LIMIT ?2").map_err(sql)?;
    let mut query = statement.query(params![after, PAGE_LIMIT]).map_err(sql)?;
    let mut through = after;
    let mut rows = Vec::new();
    let mut evidence = Vec::new();
    while let Some(row) = query.next().map_err(sql)? {
        let seq: i64 = row.get(0).map_err(sql)?;
        through = seq;
        let decoded = (|| {
            Ok::<_, rusqlite::Error>(Record {
                seq,
                attempt_id: row.get(1)?,
                harness: row.get(2)?,
                session: row.get(3)?,
                request_kind: row.get(4)?,
                step_ref: row.get(5)?,
                recorded_at: row.get(6)?,
                provider: row.get(7)?,
                model: row.get(8)?,
                charge_basis: row.get(9)?,
                auth_selection: row.get(10)?,
                account_ref: row.get(11)?,
                outcome: row.get(12)?,
                provider_request_id: row.get(13)?,
                provider_response_id: row.get(14)?,
                usage_json: row.get(15)?,
            })
        })();
        match decoded {
            Ok(record) => rows.push(record),
            Err(error) => evidence.push(SetAsideRecord {
                locator: format!("{}:incarnation:{incarnation}:seq:{seq}", path.display()),
                reason: error.to_string(),
            }),
        }
    }
    Ok(Page {
        incarnation,
        through,
        high_water,
        sessions,
        rows,
        evidence,
    })
}

fn mirror(tx: &Transaction<'_>, incarnation: &str, record: &Record) -> Result<bool, String> {
    let exists: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM thalamus_payer_record WHERE attempt_id=?1)",
            [&record.attempt_id],
            |row| row.get(0),
        )
        .map_err(sql)?;
    if exists {
        return Ok(false);
    }
    tx.execute("INSERT INTO thalamus_payer_record(attempt_id,seq,incarnation,harness,session,request_kind,step_ref,recorded_at_ms,provider,model,charge_basis,auth_selection,account_ref,outcome,provider_request_id,provider_response_id,usage_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",params![record.attempt_id,record.seq,incarnation,record.harness,record.session,record.request_kind,record.step_ref,record.recorded_at,record.provider,record.model,record.charge_basis,record.auth_selection,record.account_ref,record.outcome,record.provider_request_id,record.provider_response_id,record.usage_json]).map_err(sql)?;
    Ok(true)
}

pub(crate) fn count_coverage(
    tx: &Transaction<'_>,
    kind: &str,
    identity: &str,
) -> Result<(), String> {
    tx.execute(
        "INSERT OR IGNORE INTO thalamus_payer_coverage(kind,identity) VALUES (?1,?2)",
        params![kind, identity],
    )
    .map_err(sql)?;
    Ok(())
}

fn record_row(tx: &Transaction<'_>, record: &Record, at: i64) -> Result<Option<String>, String> {
    if !matches!(record.harness.as_str(), "claude-code" | "opencode" | "pi") {
        return Ok(Some("unknown payer harness".into()));
    }
    if record.request_kind == "main" {
        if record.usage_json.is_some() {
            return Ok(Some("main row must not carry usage_json".into()));
        }
        if let Some(step) = &record.step_ref {
            mark_conflicts(tx, &record.harness, step, at)?;
            let producer = match record.harness.as_str() {
                "claude-code" => "claude-code",
                "opencode" => "opencode",
                "pi" => "pi",
                _ => return Ok(Some("unknown payer harness".into())),
            };
            let recorded: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM usage_event WHERE producer=?1 AND producer_ref=?2)",params![producer,step],|row|row.get(0)).map_err(sql)?;
            if recorded {
                count_coverage(tx, "late", &record.attempt_id)?;
            }
        } else {
            count_coverage(tx, "unmatched_main", &record.attempt_id)?;
        }
        return Ok(None);
    }
    if !matches!(
        record.request_kind.as_str(),
        "keep_warm"
            | "prime"
            | "resend_first_attempt"
            | "failed_before_row"
            | "title"
            | "compaction_hook"
            | "generate"
    ) {
        return Ok(Some(format!(
            "unknown request_kind {}",
            record.request_kind
        )));
    }
    if record.step_ref.is_some() {
        return Ok(Some("row-less row must not carry step_ref".into()));
    }
    if record.attempt_id.is_empty() {
        return Ok(Some(
            "row-less payer attempt_id cannot form an empty producer reference".into(),
        ));
    }
    let Some(usage) = &record.usage_json else {
        return Ok(Some("row-less row requires usage_json".into()));
    };
    let decoded = match decode_attempt_usage(usage, &record.model) {
        Ok(decoded) => decoded,
        Err(reason) => return Ok(Some(reason)),
    };
    let owner: Option<String> = tx
        .query_row(
            "SELECT owner_principal FROM thalamus_payer_session WHERE harness=?1 AND session=?2",
            params![record.harness, record.session],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql)?
        .flatten();
    for (index, usage) in decoded.models.into_iter().enumerate() {
        let request = EventRecordRequest {
            producer: "thalamus-harness".into(),
            producer_lineage: Some(record.session.clone()),
            producer_ref: if decoded.by_model {
                format!("{}#{}", record.attempt_id, index + 1)
            } else {
                record.attempt_id.clone()
            },
            prior_event_ref: None,
            harness: Some(record.harness.clone()),
            project_root: None,
            session: Some(record.session.clone()),
            run_ref: None,
            flow_ref: None,
            work_class: owner.clone(),
            occurred_at_ms: Some(record.recorded_at),
            emitted_at_ms: None,
            terminal_status: record.outcome.clone(),
            attempt_ordinal: None,
            vendor_request_id: record.provider_request_id.clone(),
            provider: Some(record.provider.clone()),
            model: Some(usage.model),
            account_ref: record.account_ref.clone(),
            auth_selection: Some(record.auth_selection.clone()),
            charge_basis: basis(record.charge_basis.as_deref()),
            charge_basis_mapping_version: None,
            charge_basis_overrides: None,
            measurements: usage.measurements,
            step_detail: Default::default(),
        };
        astrocyte_core::ingest::record_event_in_tx(
            tx,
            &request,
            at,
            astrocyte_core::ingest::DeliveryPath::Read,
        )
        .map_err(|error| format!("record payer call: {error:?}"))?;
    }
    Ok(None)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PayerMatch {
    Missing,
    Conflict,
    Found {
        basis: ChargeBasis,
        auth: String,
        account: Option<String>,
    },
}

type MirroredPayer = (String, Option<String>, String, Option<String>);

fn payer_rows(
    tx: &Transaction<'_>,
    harness: &str,
    step: &str,
) -> Result<Vec<MirroredPayer>, String> {
    tx.prepare("SELECT attempt_id,charge_basis,auth_selection,account_ref FROM thalamus_payer_record WHERE harness=?1 AND step_ref=?2 AND request_kind='main' AND usage_json IS NULL ORDER BY attempt_id").map_err(sql)?.query_map(params![harness,step],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).map_err(sql)?.collect::<rusqlite::Result<Vec<_>>>().map_err(sql)
}

pub(crate) fn payer_match(
    tx: &Transaction<'_>,
    harness: &str,
    step: &str,
) -> Result<PayerMatch, String> {
    let rows = payer_rows(tx, harness, step)?;
    let Some((_, charge, auth, account)) = rows.first() else {
        return Ok(PayerMatch::Missing);
    };
    if rows
        .iter()
        .any(|(_, other_charge, other_auth, other_account)| {
            (other_charge, other_auth, other_account) != (charge, auth, account)
        })
    {
        return Ok(PayerMatch::Conflict);
    }
    Ok(PayerMatch::Found {
        basis: basis(charge.as_deref()),
        auth: auth.clone(),
        account: account.clone(),
    })
}

pub(crate) fn mark_conflicts(
    tx: &Transaction<'_>,
    harness: &str,
    step: &str,
    at: i64,
) -> Result<(), String> {
    if payer_match(tx, harness, step)? != PayerMatch::Conflict {
        return Ok(());
    }
    for (attempt, _, _, _) in payer_rows(tx, harness, step)? {
        harness_drain::record_set_aside_in_tx(
            tx,
            SOURCE_KEY,
            &[SetAsideRecord {
                locator: format!("payer.db:attempt:{attempt}"),
                reason: format!("conflicting payer facts for {harness} step {step}"),
            }],
            at,
        )?;
        count_coverage(tx, "set_aside", &attempt)?;
    }
    Ok(())
}

pub(crate) fn basis(value: Option<&str>) -> ChargeBasis {
    match value {
        Some("subscription_included") => ChargeBasis::SubscriptionIncluded,
        Some("metered_api") => ChargeBasis::MeteredApi,
        _ => ChargeBasis::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_state;
    use rusqlite::Connection;
    use tempfile::TempDir;

    const DDL: &str = r#"
CREATE TABLE payer_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE payer_session (harness TEXT NOT NULL, session TEXT NOT NULL,
 owner_principal TEXT, declared_at_ms INTEGER NOT NULL, PRIMARY KEY(harness,session));
CREATE TABLE payer_record (seq INTEGER PRIMARY KEY AUTOINCREMENT, attempt_id TEXT NOT NULL UNIQUE,
 harness TEXT NOT NULL, session TEXT NOT NULL, request_kind TEXT NOT NULL, step_ref TEXT,
 recorded_at_ms INTEGER NOT NULL, provider TEXT NOT NULL, model TEXT NOT NULL, charge_basis TEXT,
 auth_selection TEXT NOT NULL, account_ref TEXT, outcome TEXT, provider_request_id TEXT,
 provider_response_id TEXT, usage_json TEXT);
INSERT INTO payer_meta VALUES ('schema_version','1'),('incarnation','first');
INSERT INTO payer_session VALUES ('pi','session-a','owner-a',1000);
"#;

    fn fixture() -> (TempDir, ThalamusPayerSource, Connection, SharedState) {
        let home = TempDir::new().unwrap();
        let source = ThalamusPayerSource::from_data_home(home.path().to_path_buf());
        std::fs::create_dir_all(source.path.parent().unwrap()).unwrap();
        let store = Connection::open(&source.path).unwrap();
        store.execute_batch(DDL).unwrap();
        (home, source, store, test_state())
    }

    fn insert(store: &Connection, attempt: &str) {
        store.execute("INSERT INTO payer_record(attempt_id,harness,session,request_kind,recorded_at_ms,provider,model,charge_basis,auth_selection,account_ref,usage_json) VALUES (?1,'pi','session-a','keep_warm',1000,'anthropic','model-a','metered_api','login-a','account-a',?2)", params![attempt,r#"{"usage":{"input_tokens":17,"output_tokens":11,"reasoning_tokens":4,"cache_write_tokens":12},"cache_write_5m_tokens":5,"cache_write_1h_tokens":7,"reasoning_included_in_output":true}"#]).unwrap();
    }

    fn session_watermark(state: &SharedState, at: i64) {
        state.ordered_write(|tx| {
            tx.execute("INSERT INTO read_watermark VALUES ('opencode-thalamus',?1,?1) ON CONFLICT(source_key) DO UPDATE SET watermark_ms=excluded.watermark_ms",[at]).map_err(sql)?;
            Ok(((),true))
        }).unwrap();
    }

    fn served_watermark(state: &SharedState) -> serde_json::Value {
        let principal = subc_protocol::Principal::Reserved {
            module_id: "thalamus-harness".into(),
        };
        crate::ops::watermark_get(state, Some(&principal), serde_json::json!({})).unwrap()
            ["watermark_ms"]
            .clone()
    }

    #[test]
    fn payer_watermark_absent_store_full_pass_serves_session_progress_without_a_lane() {
        let home = TempDir::new().unwrap();
        let source = ThalamusPayerSource::from_data_home(home.path().to_path_buf());
        let state = test_state();
        session_watermark(&state, 40_000);
        assert!(served_watermark(&state).is_null());
        let health_before = state.public_projection().health(100_000);
        astrocyte_readers::read_io::reset_counts();
        assert_eq!(
            source
                .drain_for_health(&state, 100_000, &AtomicBool::new(false))
                .unwrap(),
            None
        );
        assert_eq!(served_watermark(&state), serde_json::json!(40_000));
        assert_eq!(astrocyte_readers::read_io::counts().connection_opens, 0);
        let health_after = state.public_projection().health(100_000);
        assert_eq!(health_after.status, health_before.status);
        assert_eq!(health_after.degraded_reason, health_before.degraded_reason);
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row(
                        "SELECT COUNT(*) FROM source_runtime WHERE source_key=?1",
                        [SOURCE_KEY],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                    0
                );
                Ok(())
            })
            .unwrap();
        let attempts = crate::state::transaction_attempts_for_test();
        source
            .drain_fast(&state, 200_000, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(crate::state::transaction_attempts_for_test(), attempts);
        assert_eq!(served_watermark(&state), serde_json::json!(40_000));
        session_watermark(&state, 120_000);
        source
            .drain_for_health(&state, 200_000, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(served_watermark(&state), serde_json::json!(120_000));
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row(
                        "SELECT watermark_ms FROM read_watermark WHERE source_key=?1",
                        [SOURCE_KEY],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                    140_000
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn payer_watermark_excluded_source_serves_only_session_progress() {
        let state = test_state();
        session_watermark(&state, 40_000);
        let mut config = state.desired_config();
        config.harness_ingest.exclude.push(SOURCE_KEY.into());
        state.publish_desired(config, 1);
        assert_eq!(served_watermark(&state), serde_json::json!(40_000));
        state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT INTO read_watermark VALUES (?1,1000,61000)",
                    [SOURCE_KEY],
                )
                .map_err(sql)?;
                Ok(((), true))
            })
            .unwrap();
        assert_eq!(served_watermark(&state), serde_json::json!(40_000));
        let mut config = state.desired_config();
        config.harness_ingest.exclude.clear();
        state.publish_desired(config, 2);
        assert_eq!(served_watermark(&state), serde_json::json!(1000));
    }

    #[test]
    fn payer_watermark_store_appearing_after_absence_cannot_pass_its_uncommitted_first_row() {
        let home = TempDir::new().unwrap();
        let source = ThalamusPayerSource::from_data_home(home.path().to_path_buf());
        let state = test_state();
        session_watermark(&state, 1_000_000);
        source
            .drain_for_health(&state, 100_000, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(served_watermark(&state), serde_json::json!(40_000));
        std::fs::create_dir_all(source.path.parent().unwrap()).unwrap();
        let store = Connection::open(&source.path).unwrap();
        store.execute_batch(DDL).unwrap();
        store.execute("INSERT INTO payer_record(attempt_id,harness,session,request_kind,step_ref,recorded_at_ms,provider,model,auth_selection) VALUES ('first','pi','session-a','main',NULL,150000,'anthropic','model-a','login')",[]).unwrap();
        assert_eq!(served_watermark(&state), serde_json::json!(40_000));
        state.ordered_write(|tx| {
            tx.execute_batch("CREATE TRIGGER reject_first_payer BEFORE INSERT ON thalamus_payer_record BEGIN SELECT RAISE(ABORT,'injected mirror failure'); END;").map_err(sql)?;
            Ok(((),true))
        }).unwrap();
        assert!(source
            .drain_for_health(&state, 200_000, &AtomicBool::new(false))
            .is_err());
        assert_eq!(served_watermark(&state), serde_json::json!(40_000));
        state
            .ordered_write(|tx| {
                assert_eq!(
                    tx.query_row("SELECT COUNT(*) FROM thalamus_payer_record", [], |row| row
                        .get::<_, i64>(
                        0
                    ))
                    .unwrap(),
                    0
                );
                tx.execute_batch("DROP TRIGGER reject_first_payer;")
                    .map_err(sql)?;
                Ok(((), true))
            })
            .unwrap();
        source
            .drain_for_health(&state, 300_000, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(served_watermark(&state), serde_json::json!(240_000));
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row(
                        "SELECT attempt_id,recorded_at_ms FROM thalamus_payer_record",
                        [],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                    )
                    .unwrap(),
                    ("first".into(), 150_000)
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn thalamus_payer_rowless_records_and_mirrors_once_with_exact_payer_and_usage() {
        let (_home, source, store, state) = fixture();
        insert(&store, "attempt-a");
        source
            .drain_for_health(&state, 100_000, &AtomicBool::new(false))
            .unwrap();
        source
            .drain_for_health(&state, 101_000, &AtomicBool::new(false))
            .unwrap();
        state.with_conn(|conn| {
            let row = conn.query_row("SELECT producer,producer_lineage,producer_ref,session,work_class,charge_basis,auth_selection,account_ref,model FROM usage_event", [], |row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,String>(2)?,row.get::<_,String>(3)?,row.get::<_,String>(4)?,row.get::<_,String>(5)?,row.get::<_,String>(6)?,row.get::<_,String>(7)?,row.get::<_,String>(8)?))).unwrap();
            assert_eq!(row,("thalamus-harness".into(),"session-a".into(),"attempt-a".into(),"session-a".into(),"owner-a".into(),"metered_api".into(),"login-a".into(),"account-a".into(),"model-a".into()));
            let measurements = conn.prepare("SELECT role,unit,quantity_units FROM measurement ORDER BY role,unit").unwrap().query_map([],|row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,i64>(2)?))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
            assert_eq!(measurements,vec![("cache_write".into(),"token_cache_1h".into(),7),("cache_write".into(),"token_cache_5m".into(),5),("input".into(),"token".into(),17),("output".into(),"token".into(),7),("reasoning".into(),"token".into(),4)]);
            assert_eq!(conn.query_row("SELECT COUNT(*) FROM usage_event",[],|r|r.get::<_,i64>(0)).unwrap(),1);
            assert_eq!(conn.query_row("SELECT COUNT(*) FROM thalamus_payer_record",[],|r|r.get::<_,i64>(0)).unwrap(),1);
            assert_eq!(conn.query_row("SELECT COUNT(*) FROM thalamus_payer_session",[],|r|r.get::<_,i64>(0)).unwrap(),1);
            assert_eq!(conn.query_row("SELECT through_seq FROM thalamus_payer_cursor",[],|r|r.get::<_,i64>(0)).unwrap(),1);
            Ok(())
        }).unwrap();
    }

    #[test]
    fn thalamus_payer_fallback_records_and_prices_each_model_once() {
        use astrocyte_core::catalog_client::{CatalogCallError, CatalogClient, CatalogConnection};
        use fusiform_protocol::{
            CatalogGetRequest, CatalogGetResponse, StatusRequest, StatusResponse,
        };
        use serde_json::json;
        use std::sync::Arc;
        struct Catalog;
        impl CatalogConnection for Catalog {
            fn catalog_status(&self, _: StatusRequest) -> Result<StatusResponse, CatalogCallError> {
                Ok(serde_json::from_value(json!({"source":"fixture","catalog_version":1,"model_count":2,"era_count":1,"recent_polls":[]})).unwrap())
            }
            fn catalog_get(
                &self,
                request: CatalogGetRequest,
            ) -> Result<CatalogGetResponse, CatalogCallError> {
                if request.at_ms == Some(0) {
                    return Err(CatalogCallError {
                        code: "no_coverage".into(),
                        message: "fusiform's history begins at 1".into(),
                    });
                }
                let rate = |units| json!({"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
                Ok(serde_json::from_value(json!({"source":"fixture","resolved_at_ms":request.at_ms,"catalog_version":1,"models":{"anthropic/model-a":{"rate.input":rate(3_000_000_000_i64),"rate.output":rate(6_000_000_000_i64)},"anthropic/model-b":{"rate.input":rate(5_000_000_000_i64),"rate.output":rate(7_000_000_000_i64)}},"withheld":[],"uncertain":[],"overridden":[]})).unwrap())
            }
        }
        let (_home, source, store, state) = fixture();
        insert(&store, "fallback");
        store.execute("UPDATE payer_record SET usage_json=?1",[r#"{"usage_by_model":[{"model_used":"model-a","usage":{"input_tokens":2,"output_tokens":3}},{"model_used":"model-b","usage":{"input_tokens":4,"output_tokens":5}}]}"#]).unwrap();
        state
            .install_catalog(Some(CatalogClient::connect(Arc::new(Catalog)).unwrap()))
            .unwrap();
        source
            .drain_for_health(&state, 100_000, &AtomicBool::new(false))
            .unwrap();
        source
            .drain_for_health(&state, 101_000, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(
            super::super::price_unpriced_events(&state, 102_000, &AtomicBool::new(false)).unwrap(),
            2
        );
        state.with_conn(|conn| {
            let actual = conn.prepare("SELECT producer_ref,model,SUM(charge_nanos) FROM usage_event JOIN priced_charge USING(event_key) GROUP BY event_key ORDER BY producer_ref").unwrap().query_map([],|row|Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,i64>(2)?))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
            assert_eq!(actual,vec![("fallback#1".into(),"model-a".into(),24_000),("fallback#2".into(),"model-b".into(),55_000)]);
            Ok(())
        }).unwrap();
    }

    #[test]
    fn thalamus_payer_absence_removes_runtime_without_opening_a_store() {
        let (_home, source, store, state) = fixture();
        drop(store);
        std::fs::remove_file(&source.path).unwrap();
        harness_drain::ensure_runtime_row(&state, SOURCE_KEY).unwrap();
        astrocyte_readers::read_io::reset_counts();
        assert_eq!(
            source
                .drain_for_health(&state, 100_000, &AtomicBool::new(false))
                .unwrap(),
            None
        );
        assert_eq!(astrocyte_readers::read_io::counts().connection_opens, 0);
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row(
                        "SELECT COUNT(*) FROM source_runtime WHERE source_key=?1",
                        [SOURCE_KEY],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                    0
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn thalamus_payer_unknown_schema_reads_nothing_and_malformed_usage_is_evidence() {
        let (_home, source, store, state) = fixture();
        insert(&store, "bad");
        store
            .execute(
                "UPDATE payer_meta SET value='2' WHERE key='schema_version'",
                [],
            )
            .unwrap();
        assert!(source
            .drain_for_health(&state, 100_000, &AtomicBool::new(false))
            .is_err());
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM thalamus_payer_record", [], |row| row
                        .get::<_, i64>(
                        0
                    ))
                    .unwrap(),
                    0
                );
                Ok(())
            })
            .unwrap();
        store.execute_batch("UPDATE payer_meta SET value='1' WHERE key='schema_version'; UPDATE payer_record SET usage_json='not JSON';").unwrap();
        insert(&store, "good");
        source
            .drain_for_health(&state, 101_000, &AtomicBool::new(false))
            .unwrap();
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM thalamus_payer_record", [], |row| row
                        .get::<_, i64>(
                        0
                    ))
                    .unwrap(),
                    2
                );
                assert_eq!(
                    conn.query_row("SELECT producer_ref FROM usage_event", [], |row| row
                        .get::<_, String>(0))
                        .unwrap(),
                    "good"
                );
                let (location, reason): (String, String) = conn
                    .query_row(
                        "SELECT locator,reason FROM set_aside_record WHERE source_key=?1",
                        [SOURCE_KEY],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .unwrap();
                assert!(location.contains("seq:1:attempt:bad"));
                assert!(!reason.is_empty());
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn thalamus_payer_unreadable_attempt_identity_keeps_later_rows_and_progress() {
        let (_home, source, store, state) = fixture();
        insert(&store, "");
        insert(&store, "valid");
        source
            .drain_for_health(&state, 100_000, &AtomicBool::new(false))
            .unwrap();
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row("SELECT producer_ref FROM usage_event", [], |row| row
                        .get::<_, String>(0))
                        .unwrap(),
                    "valid"
                );
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM thalamus_payer_record", [], |row| row
                        .get::<_, i64>(
                        0
                    ))
                    .unwrap(),
                    2
                );
                assert_eq!(
                    conn.query_row("SELECT through_seq FROM thalamus_payer_cursor", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                    2
                );
                assert_eq!(
                    conn.query_row(
                        "SELECT COUNT(*) FROM set_aside_record WHERE source_key=?1",
                        [SOURCE_KEY],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                    1
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn thalamus_payer_watermark_never_passes_an_uncommitted_mirror() {
        let (_home, source, store, state) = fixture();
        source
            .drain_for_health(&state, 100_000, &AtomicBool::new(false))
            .unwrap();
        insert(&store, "attempt-a");
        store
            .execute("UPDATE payer_record SET recorded_at_ms=150000", [])
            .unwrap();
        state.with_conn(|conn| {
            conn.execute_batch("CREATE TRIGGER refuse_payer_call BEFORE INSERT ON usage_event BEGIN SELECT RAISE(ABORT,'injected commit failure'); END;").unwrap();
            Ok(())
        }).unwrap();
        assert!(source
            .drain_for_health(&state, 200_000, &AtomicBool::new(false))
            .is_err());
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row(
                        "SELECT watermark_ms FROM read_watermark WHERE source_key=?1",
                        [SOURCE_KEY],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                    40_000
                );
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM thalamus_payer_record", [], |row| row
                        .get::<_, i64>(
                        0
                    ))
                    .unwrap(),
                    0
                );
                assert_eq!(
                    conn.query_row("SELECT through_seq FROM thalamus_payer_cursor", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                    0
                );
                conn.execute_batch("DROP TRIGGER refuse_payer_call;")
                    .unwrap();
                Ok(())
            })
            .unwrap();
        source
            .drain_for_health(&state, 200_000, &AtomicBool::new(false))
            .unwrap();
        state.with_conn(|conn| {
            assert_eq!(conn.query_row("SELECT watermark_ms FROM read_watermark WHERE source_key=?1",[SOURCE_KEY],|row| row.get::<_,i64>(0)).unwrap(),140_000);
            assert_eq!(conn.query_row("SELECT COUNT(*) FROM thalamus_payer_record",[],|row| row.get::<_,i64>(0)).unwrap(),1);
            assert!(conn.execute("INSERT OR REPLACE INTO thalamus_payer_record SELECT * FROM thalamus_payer_record",[]).is_err());
            Ok(())
        }).unwrap();
    }

    #[test]
    fn thalamus_payer_fast_idle_opens_nothing_and_changed_wal_is_read() {
        let (_home, source, store, state) = fixture();
        store.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        source
            .drain_for_health(&state, 100_000, &AtomicBool::new(false))
            .unwrap();
        source
            .drain_for_health(&state, 100_001, &AtomicBool::new(false))
            .unwrap();
        astrocyte_readers::read_io::reset_counts();
        source
            .drain_fast(&state, 101_000, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(astrocyte_readers::read_io::counts().connection_opens, 0);
        insert(&store, "attempt-a");
        source
            .drain_fast(&state, 102_000, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(astrocyte_readers::read_io::counts().connection_opens, 1);
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM usage_event", [], |row| row
                        .get::<_, i64>(0))
                        .unwrap(),
                    1
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn thalamus_payer_incarnation_reset_replays_low_sequences_without_duplicates() {
        let (_home, source, store, state) = fixture();
        insert(&store, "attempt-a");
        source
            .drain_for_health(&state, 100_000, &AtomicBool::new(false))
            .unwrap();
        store.execute_batch("DELETE FROM payer_record; DELETE FROM sqlite_sequence WHERE name='payer_record'; UPDATE payer_meta SET value='restored' WHERE key='incarnation';").unwrap();
        insert(&store, "attempt-a");
        insert(&store, "attempt-b");
        source
            .drain_for_health(&state, 101_000, &AtomicBool::new(false))
            .unwrap();
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM usage_event", [], |r| r
                        .get::<_, i64>(0))
                        .unwrap(),
                    2
                );
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM thalamus_payer_record", [], |r| r
                        .get::<_, i64>(0))
                        .unwrap(),
                    2
                );
                assert_eq!(
                    conn.query_row(
                        "SELECT incarnation,through_seq FROM thalamus_payer_cursor",
                        [],
                        |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
                    )
                    .unwrap(),
                    ("restored".into(), 2)
                );
                Ok(())
            })
            .unwrap();
    }
}
