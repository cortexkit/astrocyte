use rusqlite::{Connection, OpenFlags};
use std::path::Path;

/// Open a harness-owned store without write authority, inside one deferred
/// snapshot. Callers retain their own cancellation and transaction completion.
pub(super) fn open_read_snapshot(
    uri: String,
    path: &Path,
    harness: &str,
) -> Result<Connection, String> {
    let connection = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|error| {
        format!(
            "open {harness} database {} read-only: {error}",
            path.display()
        )
    })?;
    connection
        .execute_batch("PRAGMA query_only=ON; BEGIN DEFERRED")
        .map_err(|error| format!("begin {harness} read snapshot {}: {error}", path.display()))?;
    Ok(connection)
}
