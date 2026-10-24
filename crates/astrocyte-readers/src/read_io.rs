//! Shared read operations with thread-local counters available only to tests.

use std::fs::{File, Metadata, ReadDir};
use std::path::Path;

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub connection_opens: usize,
    pub statement_prepares: usize,
    pub directory_reads: usize,
    pub file_opens: usize,
    pub metadata_calls: usize,
}

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static COUNTS: std::cell::Cell<Counts> = const { std::cell::Cell::new(Counts {
        connection_opens: 0, statement_prepares: 0, directory_reads: 0,
        file_opens: 0, metadata_calls: 0,
    }) };
}

#[cfg(any(test, feature = "test-support"))]
pub fn counts() -> Counts {
    COUNTS.with(std::cell::Cell::get)
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_counts() {
    COUNTS.with(|counts| counts.set(Counts::default()));
}

#[cfg(any(test, feature = "test-support"))]
fn note(update: impl FnOnce(&mut Counts)) {
    COUNTS.with(|counts| {
        let mut next = counts.get();
        update(&mut next);
        counts.set(next);
    });
}

pub fn open(path: impl AsRef<Path>) -> std::io::Result<File> {
    #[cfg(any(test, feature = "test-support"))]
    note(|counts| counts.file_opens += 1);
    File::open(path)
}

pub fn metadata(path: impl AsRef<Path>) -> std::io::Result<Metadata> {
    #[cfg(any(test, feature = "test-support"))]
    note(|counts| counts.metadata_calls += 1);
    std::fs::metadata(path)
}

pub fn file_metadata(file: &File) -> std::io::Result<Metadata> {
    #[cfg(any(test, feature = "test-support"))]
    note(|counts| counts.metadata_calls += 1);
    file.metadata()
}

pub fn symlink_metadata(path: impl AsRef<Path>) -> std::io::Result<Metadata> {
    #[cfg(any(test, feature = "test-support"))]
    note(|counts| counts.metadata_calls += 1);
    std::fs::symlink_metadata(path)
}

pub fn read_dir(path: impl AsRef<Path>) -> std::io::Result<ReadDir> {
    #[cfg(any(test, feature = "test-support"))]
    note(|counts| counts.directory_reads += 1);
    std::fs::read_dir(path)
}

pub fn connect(uri: &str) -> rusqlite::Result<rusqlite::Connection> {
    #[cfg(any(test, feature = "test-support"))]
    note(|counts| counts.connection_opens += 1);
    rusqlite::Connection::open_with_flags(
        uri,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
}

pub fn prepare_cached<'a>(
    connection: &'a rusqlite::Connection,
    sql: &str,
) -> rusqlite::Result<ReadStatement<'a>> {
    let statement = connection.prepare_cached(sql)?;
    #[cfg(any(test, feature = "test-support"))]
    if statement.get_status(rusqlite::StatementStatus::Run) == 0 {
        note(|counts| counts.statement_prepares += 1);
    }
    Ok(ReadStatement(Some(statement)))
}

pub struct ReadStatement<'a>(Option<rusqlite::CachedStatement<'a>>);

impl<'a> std::ops::Deref for ReadStatement<'a> {
    type Target = rusqlite::Statement<'a>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("live read statement")
    }
}

impl std::ops::DerefMut for ReadStatement<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("live read statement")
    }
}

impl Drop for ReadStatement<'_> {
    fn drop(&mut self) {
        let statement = self.0.take().expect("live read statement");
        // Only executed statements enter the cache. Therefore a zero SQLite
        // run count on acquisition means a fresh prepare, not an unused hit.
        if statement.get_status(rusqlite::StatementStatus::Run) == 0 {
            statement.discard();
        }
    }
}

pub fn execute(connection: &rusqlite::Connection, sql: &str) -> rusqlite::Result<()> {
    prepare_cached(connection, sql)?.execute([])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepares_count_cache_misses_including_unexecuted_statements() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        reset_counts();
        drop(prepare_cached(&connection, "SELECT 1").unwrap());
        assert_eq!(counts().statement_prepares, 1);
        assert_eq!(
            prepare_cached(&connection, "SELECT 1")
                .unwrap()
                .query_row([], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(counts().statement_prepares, 2);
        assert_eq!(
            prepare_cached(&connection, "SELECT 1")
                .unwrap()
                .query_row([], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(counts().statement_prepares, 2);
    }
}
