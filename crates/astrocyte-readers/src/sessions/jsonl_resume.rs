//! Resumable reading of an append-only JSONL file.
//!
//! Astrocyte code, not copied from tokscale: tokscale has no resumable read
//! for these harnesses. The prefix check descends from tokscale's Codex
//! incremental cache (`codex_prefix_matches` in `message_cache.rs`), as the
//! Pi reader's does: a SHA-256 over the consumed bytes, compared on the next
//! read, and a consumed offset that always ends on a newline.
//!
//! A reader supplies its per-line parser and the parser state that must
//! survive between reads; this module owns the byte bookkeeping. On the next
//! read, a shrunk file or a changed inode forces a full re-read from an empty
//! state, then [`PrefixCheck`] decides how much of the consumed prefix is
//! re-hashed before the stored offset is trusted. A full re-read is safe for
//! ingest only because every reader using this derives each event's identity
//! and content from the file alone, so re-read events are exact
//! redeliveries.
//!
//! Only newline-terminated lines are handed to the parser. An unterminated
//! tail (a writer mid-append) is neither parsed nor consumed; the next read
//! starts at it again. The Pi reader keeps its own copy of this machinery
//! because it also parses an unterminated tail against a scratch state and
//! discards a whole transcript whose header it rejects.

use super::utils::try_hash_file_range;
use crate::file_identity::{inode_of, same_identity};
use sha2::{Digest, Sha256};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

/// Bytes of the already-consumed prefix that a [`PrefixCheck::Window`]
/// resume re-hashes: the window ending at the stored offset. 64 KiB is one
/// read and a few microseconds of SHA-256, and still catches the rewrite
/// shapes append-only logs suffer (truncate-and-rewrite, tail edits) on the
/// very next read.
pub const PREFIX_WINDOW_BYTES: u64 = 64 * 1024;
const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

/// How much of a file's already-consumed prefix to re-hash before trusting a
/// resume point. `Window` on ordinary ticks, `Full` on the periodic cadence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefixCheck {
    /// Re-hash only the trailing [`PREFIX_WINDOW_BYTES`] of the consumed
    /// prefix. Catches truncate-and-rewrite and tail edits at once; a
    /// rewrite of older middle bytes waits for the next `Full` check.
    Window,
    /// Re-hash every consumed byte.
    Full,
}

/// Where a previous [`read_appended_lines`] stopped, and the parser state
/// the consumed bytes produced.
#[derive(Debug, Clone)]
pub struct JsonlResumePoint<S> {
    cursor: ResumeCursor,
    pub(crate) state: S,
}

impl<S> JsonlResumePoint<S> {
    /// Bytes of the file already consumed; always just past a newline.
    pub(crate) fn offset(&self) -> u64 {
        self.cursor.offset
    }
}

/// The byte bookkeeping of a resume point, without the parser state.
#[derive(Debug, Clone)]
pub(crate) struct ResumeCursor {
    offset: u64,
    /// Running SHA-256 of every consumed byte, finalized only by a `Full`
    /// check.
    prefix_hasher: Sha256,
    window_start: u64,
    window_sha256: [u8; 32],
    inode: Option<(u64, u64)>,
    /// File length seen at the end of the read, including an unterminated
    /// tail.
    observed_len: u64,
}

impl ResumeCursor {
    pub(crate) fn with_state<S>(self, state: S) -> JsonlResumePoint<S> {
        JsonlResumePoint {
            cursor: self,
            state,
        }
    }
}

/// Result of one [`read_appended_lines`] call.
pub(crate) struct JsonlRead<S> {
    /// True when the previous point's prefix verified and reading started at
    /// its offset; false for a full read from the start.
    pub resumed: bool,
    /// The parser state after every consumed line: the previous point's
    /// state when resumed, else the fresh one.
    pub state: S,
    /// Verified anchor for the next read.
    pub cursor: ResumeCursor,
    /// Bytes fed to SHA-256 by this call. A steady-state resume is
    /// O(window + appended), never O(file): the deterministic cost counter.
    pub hashed_bytes: u64,
    /// File length seen by this read, including an unterminated tail.
    pub observed_len: u64,
    /// `observed_len` of the previous point, when this read resumed from it.
    pub previous_observed_len: Option<u64>,
}

pub(crate) struct ReadOptions<'a> {
    pub check: PrefixCheck,
    /// A closed writer can leave a complete final record without a newline.
    pub final_tail: bool,
    pub stop: &'a AtomicBool,
}

impl<'a> ReadOptions<'a> {
    pub(crate) fn live(check: PrefixCheck, stop: &'a AtomicBool) -> Self {
        Self {
            check,
            final_tail: false,
            stop,
        }
    }
}

/// Reads complete records after the verified resume offset. The callback gets
/// `None` instead of bytes for an oversized record, and a range only for an
/// explicitly final unterminated tail. No result is accepted after an I/O error.
pub(crate) fn read_appended_lines<S>(
    path: &Path,
    previous: Option<JsonlResumePoint<S>>,
    options: ReadOptions<'_>,
    fresh: impl FnOnce() -> S,
    mut on_line: impl FnMut(&mut S, Option<&[u8]>, Option<std::ops::Range<u64>>),
) -> io::Result<JsonlRead<S>> {
    check_stop(options.stop)?;
    let mut file = crate::read_io::open(path)?;

    let mut hashed_bytes = 0_u64;
    let mut resumed = None;
    let stop_check = || check_stop(options.stop);
    if let Some(previous) = previous {
        let cursor = &previous.cursor;
        let metadata = crate::read_io::file_metadata(&file)?;
        let long_enough = metadata.len() >= cursor.offset;
        let same_inode = same_identity(inode_of(&file, &metadata), cursor.inode);
        let prefix_matches = long_enough
            && same_inode
            && match options.check {
                PrefixCheck::Window => {
                    try_hash_file_range(
                        &mut file,
                        cursor.window_start,
                        cursor.offset - cursor.window_start,
                        &mut hashed_bytes,
                        stop_check,
                    )? == cursor.window_sha256
                }
                PrefixCheck::Full => {
                    try_hash_file_range(&mut file, 0, cursor.offset, &mut hashed_bytes, stop_check)?
                        == <[u8; 32]>::from(cursor.prefix_hasher.clone().finalize())
                }
            };
        if prefix_matches {
            resumed = Some(previous);
        }
    }
    let was_resumed = resumed.is_some();
    let previous_window = resumed
        .as_ref()
        .map(|previous| (previous.cursor.window_start, previous.cursor.window_sha256));
    let (start, mut hasher, mut state, previous_observed_len) = match resumed {
        Some(previous) => (
            previous.cursor.offset,
            previous.cursor.prefix_hasher,
            previous.state,
            Some(previous.cursor.observed_len),
        ),
        None => (0, Sha256::new(), fresh(), None),
    };
    file.seek(SeekFrom::Start(start))?;

    let mut reader = BufReader::new(file);
    let mut raw = Vec::new();
    let mut consumed = start;
    let mut observed_len = start;
    let bom = "\u{feff}".as_bytes();
    'lines: loop {
        raw.clear();
        let mut oversized_hasher: Option<Sha256> = None;
        let terminated = loop {
            check_stop(options.stop)?;
            let bytes = reader.fill_buf()?;
            if bytes.is_empty() {
                if observed_len == consumed || !options.final_tail {
                    // A writer may still be appending this tail. Leave the
                    // resume offset and prefix hash at its starting byte.
                    if oversized_hasher.is_some() {
                        hashed_bytes += observed_len - consumed;
                    }
                    break 'lines;
                }
                break false;
            }
            let newline = bytes.iter().position(|&byte| byte == b'\n');
            let len = newline.map_or(bytes.len(), |index| index + 1);
            let chunk = &bytes[..len];
            if let Some(hasher) = oversized_hasher.as_mut() {
                hasher.update(chunk);
            } else if raw.len() + len > MAX_LINE_BYTES {
                // Preserve the resume digest for a skipped line without ever
                // buffering the whole record or consuming an unfinished tail.
                let mut partial = hasher.clone();
                partial.update(&raw);
                partial.update(chunk);
                oversized_hasher = Some(partial);
                raw.clear();
            } else {
                raw.extend_from_slice(chunk);
            }
            reader.consume(len);
            observed_len += len as u64;
            if newline.is_some() {
                break true;
            }
        };
        let payload = oversized_hasher.is_none().then(|| {
            let bytes = &raw[..raw.len() - usize::from(terminated)];
            let bytes = bytes.strip_suffix(b"\r").unwrap_or(bytes);
            if consumed == 0 {
                bytes.strip_prefix(bom).unwrap_or(bytes)
            } else {
                bytes
            }
        });
        on_line(
            &mut state,
            payload,
            (!terminated).then_some(consumed..observed_len),
        );
        if let Some(partial) = oversized_hasher {
            hasher = partial;
        } else {
            hasher.update(&raw);
        }
        hashed_bytes += observed_len - consumed;
        consumed = observed_len;
    }

    let mut file = reader.into_inner();
    // With no complete line consumed, the window just verified (or covered by
    // the full-prefix check) is unchanged. Keep its digest rather than hashing
    // the same bytes twice on every read of a quiet file or partial tail.
    let (window_start, window_sha256) =
        if let Some(window) = previous_window.filter(|_| consumed == start) {
            window
        } else {
            let window_start = consumed.saturating_sub(PREFIX_WINDOW_BYTES);
            let digest = try_hash_file_range(
                &mut file,
                window_start,
                consumed - window_start,
                &mut hashed_bytes,
                stop_check,
            )?;
            (window_start, digest)
        };
    let metadata = crate::read_io::file_metadata(&file)?;
    let cursor = ResumeCursor {
        offset: consumed,
        prefix_hasher: hasher,
        window_start,
        window_sha256,
        inode: inode_of(&file, &metadata),
        observed_len,
    };
    Ok(JsonlRead {
        resumed: was_resumed,
        state,
        cursor,
        hashed_bytes,
        observed_len,
        previous_observed_len,
    })
}

fn check_stop(stop: &AtomicBool) -> io::Result<()> {
    if stop.load(Ordering::Relaxed) {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "JSONL read stopped",
        ))
    } else {
        Ok(())
    }
}

// The fixed 64 KiB resume window fits usize on supported 32-bit targets.
#[allow(clippy::cast_possible_truncation)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_jsonl_read_does_not_report_directory_as_empty_transcript() {
        let directory = tempfile::tempdir().unwrap();
        let read = read_appended_lines(
            directory.path(),
            None,
            ReadOptions::live(PrefixCheck::Window, &AtomicBool::new(false)),
            Vec::<Vec<u8>>::new,
            |lines, payload, _| lines.push(payload.unwrap().to_vec()),
        );
        assert!(read.is_err(), "a read error is not an empty transcript");
    }
    use std::io::Write;

    #[test]
    fn shared_jsonl_line_limit_ignores_oversized_line_and_keeps_later_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.jsonl");
        let mut data = vec![b'x'; MAX_LINE_BYTES + 1];
        std::fs::write(&path, &data).unwrap();
        let first = lines(&path, None, PrefixCheck::Window);
        assert!(first.state.is_empty());
        assert_eq!(first.cursor.offset, 0);
        data.extend_from_slice(b"\nvalid\n");
        std::fs::write(&path, &data).unwrap();
        let second = lines(&path, resume(first), PrefixCheck::Window);
        assert!(second.state.iter().all(|line| line.len() <= MAX_LINE_BYTES));
        assert_eq!(second.state.last().unwrap(), "valid");
        assert_eq!(second.cursor.offset, data.len() as u64);
    }

    #[test]
    fn shared_jsonl_read_checks_stop_inside_the_file() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.jsonl");
        std::fs::write(&path, b"first\nsecond\n").unwrap();
        let stop = AtomicBool::new(false);
        let mut lines_seen = 0;
        let result = read_appended_lines(
            &path,
            None,
            ReadOptions::live(PrefixCheck::Window, &stop),
            || (),
            |_, _, _| {
                lines_seen += 1;
                stop.store(true, Ordering::Relaxed);
            },
        );
        assert_eq!(
            result.err().unwrap().kind(),
            std::io::ErrorKind::Interrupted
        );
        assert_eq!(lines_seen, 1);
    }

    fn lines(
        path: &Path,
        previous: Option<JsonlResumePoint<Vec<String>>>,
        check: PrefixCheck,
    ) -> JsonlRead<Vec<String>> {
        read_appended_lines(
            path,
            previous,
            ReadOptions::live(check, &AtomicBool::new(false)),
            Vec::new,
            |seen, line, _| {
                seen.push(line.map_or_else(
                    || "<oversized>".into(),
                    |line| String::from_utf8_lossy(line).into_owned(),
                ))
            },
        )
        .unwrap()
    }

    fn resume(read: JsonlRead<Vec<String>>) -> Option<JsonlResumePoint<Vec<String>>> {
        let state = read.state;
        Some(read.cursor.with_state(state))
    }

    #[test]
    fn unknown_identity_restarts_from_a_fresh_parser_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.jsonl");
        std::fs::write(&path, "a\nb\n").unwrap();
        let mut point = resume(lines(&path, None, PrefixCheck::Window)).unwrap();
        point.cursor.inode = None;
        point.state = vec!["stale".to_string()];
        let read = lines(&path, Some(point), PrefixCheck::Window);
        assert!(!read.resumed);
        assert_eq!(read.state, ["a", "b"]);
    }

    #[cfg(unix)]
    #[test]
    fn another_device_with_the_same_inode_restarts_and_discards_cached_state() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.jsonl");
        std::fs::write(&path, "a\nb\n").unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        assert_ne!(metadata.dev(), 0, "fixture must distinguish the devices");
        let mut point = resume(lines(&path, None, PrefixCheck::Window)).unwrap();
        // An inode number can be reused on a different device even when the
        // verified bytes match. That cannot authorize keeping cached state.
        point.cursor.inode = Some((0, metadata.ino()));
        point.state = vec!["stale".to_string()];
        let read = lines(&path, Some(point), PrefixCheck::Window);
        assert_eq!(read.state, ["a", "b"]);
        assert!(!read.resumed);
    }

    #[test]
    fn replacing_a_file_with_the_same_tail_restarts_before_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.jsonl");
        let tail = "a\n".repeat(PREFIX_WINDOW_BYTES as usize);
        std::fs::write(&path, format!("old\n{tail}")).unwrap();
        let point = resume(lines(&path, None, PrefixCheck::Window)).unwrap();
        let held = std::fs::File::open(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, format!("new\n{tail}")).unwrap();
        let read = lines(&path, Some(point), PrefixCheck::Window);
        assert!(!read.resumed);
        assert_eq!(read.state[0], "new");
        drop(held);
    }

    #[test]
    fn a_resume_reads_only_appended_complete_lines_and_keeps_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.jsonl");
        std::fs::write(&path, "\u{feff}a\r\nb\nparti").unwrap();
        let first = lines(&path, None, PrefixCheck::Window);
        assert!(!first.resumed);
        assert_eq!(first.state, ["a", "b"]);
        assert_eq!(first.observed_len, 13);
        let point = resume(first).unwrap();
        assert_eq!(point.offset(), 8);

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"al\nc\n").unwrap();
        let second = lines(&path, Some(point), PrefixCheck::Full);
        assert!(second.resumed);
        assert_eq!(second.previous_observed_len, Some(13));
        assert_eq!(second.state, ["a", "b", "partial", "c"]);
    }

    #[test]
    fn unchanged_resumes_hash_the_verified_prefix_only_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.jsonl");
        // Longer than the window, so a full-prefix check cannot accidentally
        // pass as a window check. A partial tail must not advance the offset.
        let complete = "a\n".repeat(PREFIX_WINDOW_BYTES as usize);
        for tail in ["", "partial"] {
            std::fs::write(&path, format!("{complete}{tail}")).unwrap();
            let mut point = resume(lines(&path, None, PrefixCheck::Window)).unwrap();
            let offset = point.offset();
            assert_eq!(offset, complete.len() as u64);
            for check in [PrefixCheck::Window, PrefixCheck::Full] {
                let expected = match check {
                    PrefixCheck::Window => PREFIX_WINDOW_BYTES,
                    PrefixCheck::Full => offset,
                };
                for _ in 0..100 {
                    let read = lines(&path, Some(point), check);
                    assert!(read.resumed);
                    assert_eq!(read.hashed_bytes, expected, "{check:?}, tail {tail:?}");
                    assert_eq!(read.state.len(), PREFIX_WINDOW_BYTES as usize);
                    assert_eq!(read.observed_len, offset + tail.len() as u64);
                    point = resume(read).unwrap();
                    assert_eq!(point.offset(), offset);
                }
            }
        }
    }

    #[test]
    fn full_prefix_check_still_rejects_a_rewrite_before_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.jsonl");
        let complete = "a\n".repeat(PREFIX_WINDOW_BYTES as usize);
        std::fs::write(&path, &complete).unwrap();
        let point = resume(lines(&path, None, PrefixCheck::Window)).unwrap();
        let quiet = lines(&path, Some(point), PrefixCheck::Window);
        assert!(quiet.resumed);
        let point = resume(quiet).unwrap();
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all(b"b").unwrap();
        let unchanged_tail = lines(&path, Some(point), PrefixCheck::Window);
        assert!(unchanged_tail.resumed);
        assert_eq!(unchanged_tail.state[0], "a");
        let rewritten = lines(&path, resume(unchanged_tail), PrefixCheck::Full);
        assert!(!rewritten.resumed);
        assert_eq!(rewritten.state[0], "b");
        assert_eq!(rewritten.state.len(), PREFIX_WINDOW_BYTES as usize);
    }

    #[test]
    fn a_rewritten_or_shrunk_file_is_read_again_from_a_fresh_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.jsonl");
        std::fs::write(&path, "a\nb\n").unwrap();
        let point = resume(lines(&path, None, PrefixCheck::Window)).unwrap();
        std::fs::write(&path, "x\ny\n").unwrap();
        let rewritten = lines(&path, Some(point), PrefixCheck::Window);
        assert!(!rewritten.resumed);
        assert_eq!(rewritten.state, ["x", "y"]);

        let point = resume(rewritten).unwrap();
        std::fs::write(&path, "x\n").unwrap();
        let shrunk = lines(&path, Some(point), PrefixCheck::Window);
        assert!(!shrunk.resumed);
        assert_eq!(shrunk.state, ["x"]);
    }
}
