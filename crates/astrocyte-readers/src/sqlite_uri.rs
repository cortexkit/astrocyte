//! Read-only SQLite URIs with filenames kept out of the authority and query.

/// Percent-encode a UTF-8 filename without introducing a URI authority.
///
/// Bundled SQLite's `sqlite3ParseUri` checks literal `//` for an authority
/// before decoding `%HH`, and rejects authorities other than empty/localhost
/// unless `SQLITE_ALLOW_URI_AUTHORITY` is enabled. Escaping the leading slashes
/// of a UNC name keeps its server in the filename, then decodes back to `//`.
pub fn sqlite_readonly_uri(path: &str) -> String {
    let mut encoded = String::with_capacity(path.len());
    for (index, byte) in path.bytes().enumerate() {
        if index < 2 && path.starts_with("//") {
            encoded.push_str("%2F");
            continue;
        }
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'/' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    format!("file:{encoded}?mode=ro")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{Connection, OpenFlags};

    #[test]
    fn paths_are_encoded_without_changing_the_filename() {
        for (path, expected) in [
            ("/home/a/x.db", "file:/home/a/x.db?mode=ro"),
            ("/home/a b/#?%.db", "file:/home/a%20b/%23%3F%25.db?mode=ro"),
            (
                r"C:\Users\a b\x.db",
                "file:C%3A%5CUsers%5Ca%20b%5Cx.db?mode=ro",
            ),
            (
                r"\\server\share\x.db",
                "file:%5C%5Cserver%5Cshare%5Cx.db?mode=ro",
            ),
        ] {
            assert_eq!(sqlite_readonly_uri(path), expected);
        }
    }

    #[test]
    fn forward_slash_unc_has_no_uri_authority() {
        assert_eq!(
            sqlite_readonly_uri("//server/share/x.db"),
            "file:%2F%2Fserver/share/x.db?mode=ro"
        );
    }

    #[test]
    fn encoded_uri_opens_the_real_database_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a #%.db");
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch("CREATE TABLE sample(value); INSERT INTO sample VALUES(42);")
            .unwrap();
        drop(writer);
        let reader = Connection::open_with_flags(
            sqlite_readonly_uri(path.to_str().unwrap()),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap();
        assert_eq!(
            reader
                .query_row("SELECT value FROM sample", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            42
        );
        assert!(reader.execute("INSERT INTO sample VALUES(43)", []).is_err());
    }
}
