//! Bounded, all-or-nothing decoding of immutable Codex rollouts.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;

use super::{parse_line, CodexParseState, CodexResumableParse};

const MAX_DECODED_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_WINDOW_BYTES: u64 = 64 * 1024 * 1024;

pub(super) fn parse_archive(path: &Path) -> CodexResumableParse {
    match read_archive(path, MAX_DECODED_BYTES, MAX_RECORD_BYTES) {
        Ok(parse) => parse,
        Err(error) => CodexResumableParse {
            io_error: Some(format!("Codex archive: {error}")),
            ..CodexResumableParse::default()
        },
    }
}

fn invalid(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

fn read_archive(
    path: &Path,
    max_decoded: u64,
    max_record: usize,
) -> io::Result<CodexResumableParse> {
    let mut source = BufReader::new(File::open(path)?);
    if source.fill_buf()?.is_empty() {
        return Err(invalid("empty compressed stream"));
    }
    let mut result = CodexResumableParse::default();
    let mut state = CodexParseState::default();
    let mut json_buffer = Vec::with_capacity(4096);
    let mut record = Vec::new();
    let mut oversized = false;
    let mut chunk = [0_u8; 8192];
    // A decoder reads exactly one frame. Recreate it until compressed EOF so
    // concatenated frames and corrupt trailing data cannot hide behind success.
    while !source.fill_buf()?.is_empty() {
        let mut decoder = ruzstd::decoding::StreamingDecoder::new_with_max_window_size(
            &mut source,
            MAX_WINDOW_BYTES,
        )
        .map_err(|error| invalid(&error.to_string()))?;
        loop {
            let read = decoder.read(&mut chunk)?;
            if read == 0 {
                break;
            }
            result.decompressed_bytes += u64::try_from(read).expect("chunk length fits u64");
            if result.decompressed_bytes > max_decoded {
                return Err(invalid("decoded size exceeds limit"));
            }
            for part in chunk[..read].split_inclusive(|byte| *byte == b'\n') {
                if record.len().saturating_add(part.len()) > max_record {
                    oversized = true;
                    record.clear();
                }
                if !oversized {
                    record.extend_from_slice(part);
                }
                if part.last() != Some(&b'\n') {
                    continue;
                }
                state.lines += 1;
                result.counts.lines += 1;
                let skipped = result.counts.skipped_lines;
                if oversized {
                    result.counts.skipped_lines += 1;
                } else {
                    parse_line(
                        &record,
                        &mut json_buffer,
                        &mut state,
                        &mut result.counts,
                        &mut result.calls,
                    );
                }
                if result.counts.skipped_lines > skipped {
                    result.set_aside.push((
                        state.lines,
                        if oversized {
                            "Codex archive record exceeds 16 MiB".into()
                        } else {
                            "Codex record could not be decoded".into()
                        },
                    ));
                }
                record.clear();
                oversized = false;
            }
        }
        let frame = &decoder.decoder;
        if !frame.is_finished()
            || frame
                .get_checksum_from_data()
                .is_some_and(|checksum| frame.get_calculated_checksum() != Some(checksum))
        {
            return Err(invalid("incomplete frame or checksum mismatch"));
        }
    }
    if !record.is_empty() || oversized {
        return Err(invalid("unterminated archive record"));
    }
    if state.activity
        && result.counts.usage_records == 0
        && result.counts.supplementary_requests == 0
        && state.pending.is_none()
    {
        result.set_aside.push((
            0,
            "Codex archive has activity but no supported usage; token quantities are unknown"
                .into(),
        ));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(bytes: &[u8]) -> Vec<u8> {
        let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0, 0];
        let chunks: Vec<_> = bytes.chunks(1024).collect();
        for (index, chunk) in chunks.iter().enumerate() {
            let header =
                (u32::try_from(chunk.len()).unwrap() << 3) | u32::from(index + 1 == chunks.len());
            frame.extend_from_slice(&header.to_le_bytes()[..3]);
            frame.extend_from_slice(chunk);
        }
        frame
    }

    #[test]
    fn decoded_limit_covers_aggregate_not_just_individual_records() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("a.jsonl.zst");
        let data = "{\"type\":\"ignored\"}\n".repeat(100);
        let mut frames = frame(&data.as_bytes()[..900]);
        frames.extend(frame(&data.as_bytes()[900..]));
        std::fs::write(&path, frames).unwrap();
        assert!(read_archive(&path, 1000, 128)
            .unwrap_err()
            .to_string()
            .contains("decoded size"));
        let parsed = read_archive(&path, 4096, 128).unwrap();
        assert_eq!(parsed.counts.lines, 100);
        assert_eq!(
            parsed.decompressed_bytes,
            u64::try_from(data.len()).unwrap()
        );
    }

    #[test]
    fn oversized_record_is_bounded_and_later_usage_survives() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("a.jsonl.zst");
        let data = format!("{{\"type\":\"ignored\",\"payload\":{{\"text\":\"{}\"}}}}\n", "x".repeat(5000))
            + "{\"type\":\"session_meta\",\"payload\":{\"id\":\"thread\"}}\n{\"type\":\"event_msg\",\"ordinal\":2,\"payload\":{\"type\":\"token_count\",\"info\":{\"last_token_usage\":{\"input_tokens\":3},\"total_token_usage\":{\"input_tokens\":3}}}}\n";
        std::fs::write(&path, frame(data.as_bytes())).unwrap();
        let parsed = read_archive(&path, 10000, 512).unwrap();
        assert_eq!(parsed.set_aside.len(), 1);
        assert_eq!(parsed.set_aside[0].0, 1);
        assert_eq!(parsed.calls.len(), 1);
        assert_eq!(parsed.calls[0].producer_ref, "thread:ordinal:2");
        assert_eq!(parsed.calls[0].usage.cache_read, None);
    }

    #[test]
    fn active_archive_without_usage_has_visible_set_aside_evidence() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("a.jsonl.zst");
        std::fs::write(
            &path,
            frame(b"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\"}}\n"),
        )
        .unwrap();
        let parsed = parse_archive(&path);
        assert!(parsed.io_error.is_none());
        assert!(parsed.calls.is_empty());
        assert_eq!(parsed.set_aside.len(), 1);
        assert!(parsed.set_aside[0]
            .1
            .contains("activity but no supported usage"));
    }
}
