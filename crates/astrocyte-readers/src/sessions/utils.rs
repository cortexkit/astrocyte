// Copied from tokscale v4.17.0,
// crates/tokscale-core/src/sessions/utils.rs.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../../LICENSE-tokscale for the full upstream license text.
// ASTROCYTE DIVERGENCE: only helpers required by the Pi reader and their
// upstream tests are retained, avoiding unrelated SQLite and parser machinery.
// ASTROCYTE DIVERGENCE: upstream's `file_modified_timestamp_ms` is dropped
// with its only caller, the Pi reader's mtime stand-in for a missing record
// timestamp; do not restore it on an upstream sync.

use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::io::BufRead;
use std::ops::ControlFlow;
use std::path::Path;

// Generate `add` by summing each listed field. Each harness reader lists its
// own counters, keeping counts of records, token roles and diagnostics distinct.
macro_rules! add_parse_counts {
    ($($field:ident),+ $(,)?) => {
        pub fn add(&mut self, other: &Self) {
            $(self.$field += other.$field;)+
        }
    };
}
pub(super) use add_parse_counts;

#[cfg(test)]
thread_local! {
    static JSON_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn json_decodes() -> usize {
    JSON_DECODES.with(std::cell::Cell::get)
}

/// Read one line into `buf`, stripping the line terminator and a leading BOM.
fn read_line_payload<R: BufRead>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    at_start: &mut bool,
) -> Option<usize> {
    buf.clear();
    match reader.read_until(b'\n', buf) {
        Ok(0) => None,
        Ok(_) => {
            if buf.last() == Some(&b'\n') {
                buf.pop();
                if buf.last() == Some(&b'\r') {
                    buf.pop();
                }
            }

            let bom = "\u{feff}".as_bytes();
            if std::mem::take(at_start) && buf.starts_with(bom) {
                Some(bom.len())
            } else {
                Some(0)
            }
        }
        // A hard I/O error does not consume input, so retrying would spin.
        Err(_) => None,
    }
}

/// One JSONL line borrowed out of the driver's reusable buffer.
pub(crate) struct JsonLineBytes<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) trimmed: &'a str,
    pub(crate) valid_utf8: bool,
}

/// Read line-delimited JSON, retaining exact bytes and UTF-8 validity.
pub(crate) fn for_each_json_line_with_bytes(
    path: &Path,
    sink: &mut dyn FnMut(JsonLineBytes<'_>) -> ControlFlow<()>,
) {
    let Ok(file) = std::fs::File::open(path) else {
        return;
    };

    let mut reader = std::io::BufReader::new(file);
    let mut buf = Vec::new();
    let mut at_start = true;

    while let Some(start) = read_line_payload(&mut reader, &mut buf, &mut at_start) {
        let bytes = &buf[start..];
        let text = String::from_utf8_lossy(bytes);
        let trimmed = text.trim();
        if trimmed.is_empty() {
            continue;
        }
        let line = JsonLineBytes {
            bytes,
            trimmed,
            valid_utf8: matches!(text, Cow::Borrowed(_)),
        };
        if sink(line).is_break() {
            break;
        }
    }
}

/// Decode one JSONL line into `T`, reusing `buffer` as simd-json scratch space.
pub(crate) fn parse_json_line<T: DeserializeOwned>(line: &str, buffer: &mut Vec<u8>) -> Option<T> {
    #[cfg(test)]
    JSON_DECODES.with(|count| count.set(count.get() + 1));
    buffer.clear();
    buffer.extend_from_slice(line.as_bytes());
    simd_json::from_slice(buffer).ok()
}

/// Decode the JSON bytes once. If the typed record is unreadable, restart
/// deserialization on the same tape to recover only its discriminator.
pub(crate) fn parse_json_line_with_probe<T: DeserializeOwned, P: DeserializeOwned>(
    line: &str,
    buffer: &mut Vec<u8>,
) -> Result<T, Option<P>> {
    #[cfg(test)]
    JSON_DECODES.with(|count| count.set(count.get() + 1));
    buffer.clear();
    buffer.extend_from_slice(line.as_bytes());
    let mut deserializer = simd_json::Deserializer::from_slice(buffer).map_err(|_| None)?;
    T::deserialize(&mut deserializer).map_err(|_| {
        deserializer.restart();
        P::deserialize(&mut deserializer).ok()
    })
}

pub(crate) fn parse_rfc3339_millis(timestamp: Option<&str>) -> Option<i64> {
    timestamp
        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| dt.timestamp_millis())
}

/// SHA-256 of exactly `len` bytes starting at `start`, or `None` if the file
/// cannot supply them all. Add every byte hashed to `*hashed`, including failed
/// attempts. This does not advance a parser offset or decide whether a final
/// line without a newline is visible to the calling reader.
pub(crate) fn hash_file_range(
    file: &mut std::fs::File,
    start: u64,
    len: u64,
    hashed: &mut u64,
) -> Option<[u8; 32]> {
    use std::io::{Read, Seek, SeekFrom};

    file.seek(SeekFrom::Start(start)).ok()?;
    let mut hasher = Sha256::new();
    let mut remaining = len;
    let mut chunk = vec![0_u8; 64 * 1024].into_boxed_slice();
    let chunk_len = u64::try_from(chunk.len()).ok()?;
    while remaining > 0 {
        let want = usize::try_from(remaining.min(chunk_len)).unwrap_or(chunk.len());
        match file.read(&mut chunk[..want]) {
            Ok(0) | Err(_) => return None,
            Ok(read) => {
                hasher.update(&chunk[..read]);
                let read = u64::try_from(read).ok()?;
                *hashed += read;
                remaining -= read;
            }
        }
    }
    Some(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_each_json_line_with_bytes_preserves_distinct_invalid_sequences() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("transcript.jsonl");
        std::fs::write(
            &path,
            b"\xef\xbb\xbf {\"clean\":1} \r\n\n  \na\xff\na\xfe\n",
        )
        .unwrap();

        let mut lines = Vec::new();
        for_each_json_line_with_bytes(&path, &mut |line| {
            lines.push((
                line.bytes.to_vec(),
                line.trimmed.to_string(),
                line.valid_utf8,
            ));
            ControlFlow::Continue(())
        });

        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].0, b" {\"clean\":1} ");
        assert_eq!(lines[0].1, r#"{"clean":1}"#);
        assert!(lines[0].2);
        assert_eq!(lines[1].1, lines[2].1);
        assert_eq!(lines[1].0, b"a\xff");
        assert_eq!(lines[2].0, b"a\xfe");
        assert!(!lines[1].2);
        assert!(!lines[2].2);
    }

    #[test]
    fn for_each_json_line_with_bytes_stops_on_break() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("transcript.jsonl");
        std::fs::write(&path, b"first\nsecond\nthird\n").unwrap();

        let mut seen = Vec::new();
        for_each_json_line_with_bytes(&path, &mut |line| {
            seen.push(line.trimmed.to_string());
            if line.trimmed == "second" {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });

        assert_eq!(seen, vec!["first", "second"]);
    }
}
