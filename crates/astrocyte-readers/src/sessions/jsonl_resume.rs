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

use super::utils::hash_file_range;
use crate::file_identity::{inode_of, same_identity};
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;

/// Bytes of the already-consumed prefix that a [`PrefixCheck::Window`]
/// resume re-hashes: the window ending at the stored offset. 64 KiB is one
/// read and a few microseconds of SHA-256, and still catches the rewrite
/// shapes append-only logs suffer (truncate-and-rewrite, tail edits) on the
/// very next read.
pub const PREFIX_WINDOW_BYTES: u64 = 64 * 1024;

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
    /// Where the next read may resume, or `None` when the bytes that anchor
    /// the next window check could not be read back, so the next read starts
    /// over.
    pub cursor: Option<ResumeCursor>,
    /// Bytes fed to SHA-256 by this call. A steady-state resume is
    /// O(window + appended), never O(file): the deterministic cost counter.
    pub hashed_bytes: u64,
    /// File length seen by this read, including an unterminated tail.
    pub observed_len: u64,
    /// `observed_len` of the previous point, when this read resumed from it.
    pub previous_observed_len: Option<u64>,
}

/// Reads the complete lines of `path` that `previous` has not consumed yet,
/// handing each to `on_line` with the parser state.
///
/// `previous` is trusted only when the file is at least as long as its
/// offset, has the same inode, and the bytes `check` re-hashes are
/// unchanged; otherwise reading starts from byte 0 with `fresh()`. Each line
/// reaches `on_line` without its `\n`, a trailing `\r`, or (on the file's
/// first line) a UTF-8 BOM, and may be empty or whitespace.
///
/// Returns `None` when the file cannot be opened or positioned, in which
/// case nothing was parsed.
pub(crate) fn read_appended_lines<S>(
    path: &Path,
    previous: Option<JsonlResumePoint<S>>,
    check: PrefixCheck,
    fresh: impl FnOnce() -> S,
    on_line: impl FnMut(&mut S, &[u8]),
) -> Option<JsonlRead<S>> {
    read_appended_lines_inner(path, previous, check, fresh, on_line, false).ok()
}

/// Fallible read for callers that must distinguish an unreadable file from an
/// empty one. No parser output or resume point is accepted after an I/O failure.
pub(crate) fn try_read_appended_lines<S>(
    path: &Path,
    previous: Option<JsonlResumePoint<S>>,
    check: PrefixCheck,
    fresh: impl FnOnce() -> S,
    on_line: impl FnMut(&mut S, &[u8]),
) -> std::io::Result<JsonlRead<S>> {
    read_appended_lines_inner(path, previous, check, fresh, on_line, true)
}

fn read_appended_lines_inner<S>(
    path: &Path,
    previous: Option<JsonlResumePoint<S>>,
    check: PrefixCheck,
    fresh: impl FnOnce() -> S,
    mut on_line: impl FnMut(&mut S, &[u8]),
    fail_on_io_error: bool,
) -> std::io::Result<JsonlRead<S>> {
    let mut file = crate::read_io::open(path)?;

    let mut hashed_bytes = 0_u64;
    let mut resumed = None;
    if let Some(previous) = previous {
        let cursor = &previous.cursor;
        let metadata = crate::read_io::file_metadata(&file).ok();
        let long_enough = metadata
            .as_ref()
            .is_some_and(|metadata| metadata.len() >= cursor.offset);
        let same_inode = same_identity(
            metadata
                .as_ref()
                .and_then(|metadata| inode_of(&file, metadata)),
            cursor.inode,
        );
        let prefix_matches = long_enough
            && same_inode
            && match check {
                PrefixCheck::Window => hash_file_range(
                    &mut file,
                    cursor.window_start,
                    cursor.offset - cursor.window_start,
                    &mut hashed_bytes,
                )
                .is_some_and(|digest| digest == cursor.window_sha256),
                PrefixCheck::Full => {
                    hash_file_range(&mut file, 0, cursor.offset, &mut hashed_bytes).is_some_and(
                        |digest| {
                            digest == <[u8; 32]>::from(cursor.prefix_hasher.clone().finalize())
                        },
                    )
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
    loop {
        raw.clear();
        match reader.read_until(b'\n', &mut raw) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) if fail_on_io_error => return Err(error),
            Err(_) => break,
        }
        observed_len += raw.len() as u64;
        if raw.last() != Some(&b'\n') {
            // An unterminated tail: a writer mid-append. Not consumed.
            break;
        }
        let mut payload: &[u8] = &raw[..raw.len() - 1];
        if payload.last() == Some(&b'\r') {
            payload = &payload[..payload.len() - 1];
        }
        if consumed == 0 && payload.starts_with(bom) {
            payload = &payload[bom.len()..];
        }
        on_line(&mut state, payload);
        hasher.update(&raw);
        hashed_bytes += raw.len() as u64;
        consumed += raw.len() as u64;
    }

    let mut file = reader.into_inner();
    // With no complete line consumed, the window just verified (or covered by
    // the full-prefix check) is unchanged. Keep its digest rather than hashing
    // the same bytes twice on every read of a quiet file or partial tail.
    let unchanged_window = previous_window.filter(|_| consumed == start);
    let window_start = consumed.saturating_sub(PREFIX_WINDOW_BYTES);
    let cursor = unchanged_window
        .or_else(|| {
            hash_file_range(
                &mut file,
                window_start,
                consumed - window_start,
                &mut hashed_bytes,
            )
            .map(|digest| (window_start, digest))
        })
        .map(|(window_start, window_sha256)| ResumeCursor {
            offset: consumed,
            prefix_hasher: hasher,
            window_start,
            window_sha256,
            inode: crate::read_io::file_metadata(&file)
                .ok()
                .as_ref()
                .and_then(|metadata| inode_of(&file, metadata)),
            observed_len,
        });

    if cursor.is_none() && fail_on_io_error {
        return Err(std::io::Error::other(
            "could not verify the consumed JSONL bytes",
        ));
    }
    Ok(JsonlRead {
        resumed: was_resumed,
        state,
        cursor,
        hashed_bytes,
        observed_len,
        previous_observed_len,
    })
}

// The fixed 64 KiB resume window fits usize on supported 32-bit targets.
#[allow(clippy::cast_possible_truncation)]
#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn checked_read_exposes_io_errors_without_changing_legacy_callers() {
        let directory = tempfile::tempdir().unwrap();
        let read = read_appended_lines(
            directory.path(),
            None,
            PrefixCheck::Window,
            Vec::<Vec<u8>>::new,
            |lines, payload| lines.push(payload.to_vec()),
        );
        assert!(
            read.is_some(),
            "legacy readers keep their existing partial-read result on a read error"
        );
        assert!(read.unwrap().state.is_empty());
        let checked = try_read_appended_lines(
            directory.path(),
            None,
            PrefixCheck::Window,
            Vec::<Vec<u8>>::new,
            |lines, payload| lines.push(payload.to_vec()),
        );
        assert!(
            checked.is_err(),
            "a directory cannot be read as a transcript"
        );
    }
    use std::io::Write;

    fn lines(
        path: &Path,
        previous: Option<JsonlResumePoint<Vec<String>>>,
        check: PrefixCheck,
    ) -> JsonlRead<Vec<String>> {
        read_appended_lines(path, previous, check, Vec::new, |seen, line| {
            seen.push(String::from_utf8_lossy(line).into_owned())
        })
        .unwrap()
    }

    fn resume(read: JsonlRead<Vec<String>>) -> Option<JsonlResumePoint<Vec<String>>> {
        let state = read.state;
        read.cursor.map(|cursor| cursor.with_state(state))
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
