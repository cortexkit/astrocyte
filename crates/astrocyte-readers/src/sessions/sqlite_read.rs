use rusqlite::Connection;
use std::path::Path;

/// Open a harness-owned store without write authority, inside one deferred
/// snapshot. Callers retain their own cancellation and transaction completion.
pub(super) fn open_read_snapshot(
    uri: String,
    path: &Path,
    harness: &str,
) -> Result<Connection, String> {
    let connection = open_read_connection(uri, path, harness)?;
    connection
        .execute_batch("BEGIN DEFERRED")
        .map_err(|error| format!("begin {harness} read snapshot {}: {error}", path.display()))?;
    Ok(connection)
}

pub(super) fn open_read_connection(
    uri: String,
    path: &Path,
    harness: &str,
) -> Result<Connection, String> {
    let connection = crate::read_io::connect(&uri).map_err(|error| {
        format!(
            "open {harness} database {} read-only: {error}",
            path.display()
        )
    })?;
    crate::read_io::execute(&connection, "PRAGMA query_only=ON")
        .map_err(|error| format!("begin {harness} read snapshot {}: {error}", path.display()))?;
    Ok(connection)
}
