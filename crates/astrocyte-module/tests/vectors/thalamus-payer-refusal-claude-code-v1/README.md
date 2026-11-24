# Claude Code refusal and retry payer sample

Captured with real Claude Code 2.1.331 on Linux, Agent Runner build `9fdbeb675d43245fef29929adbdae1e48cf97135`,
and a loopback Anthropic stand-in (no signed-in account or real usage).
The `machineID` and `userID` in `.claude.json` came from the throwaway test rig and identify no person or account.

- Refused request: `payer_record.seq = 1`, `req_sample_refused` (`stop_reason: refusal`).
- Answered retry: `payer_record.seq = 2`, `req_sample_retry` (`stop_reason: end_turn`).

Both requests appear in `claude-session/config/projects/-tmp-cc-payer-refusal-e78zXM-rig-project-cc-payer-refusal-sample/2429aff9-395a-49bb-b765-65abf134e042.jsonl`. The refusal row joins
to assistant message `msg_sample_refused` with `requestId: req_sample_refused`; the retry
row joins to assistant message `msg_sample_retry` with `requestId: req_sample_retry`.
Both are `main` payer rows: the gateway forwarded the request and handed its
response back to Claude Code. The refusal row's `outcome: completed` means the
response was delivered in full, not that the model answered the prompt. By
contract, these rows have NULL `usage_json`: Astrocyte gets their usage from
the matching assistant entry, joining by request id.

`claude-session/config/` is the unedited Claude Code config tree from the capture.
`claude-session/roots.json` preserves the capture-time absolute `path`; to replay
elsewhere, point a working copy of that roots entry at the copied config tree.
`payer-rows.json` contains the session declaration and its two payer records,
with every database column (including `usage_json` as stored text). Verify the
capture with `sha256sum -c SHA256SUMS` from this directory.
