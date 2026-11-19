# Usage export: one fact per model attempt (broca, 19 Nov 2026)

Written by broca's own serializers (`write_attempt_fact_samples_when_requested` in
`crates/broca-core/src/billing/attempt/tests.rs`). Each file is one run's facts, one per line, in
the exact bytes `run-index.db` stores in `export_facts.segment_json`, in export order.

Rules:
- A fact with `"kind":"model_attempt"` is one provider request.
- A segment fact with `"usage_in_attempts":true` has its usage in the run's attempt facts too.
  Record it, don't price it; price the attempts. A segment without the member prices as today.
- Each `usage_by_model` entry names the segment that counted it (`segment_fact_id`).
- `usage_by_model` absent = the provider reported no usage for that request (unknown, not zero).
- Warm facts (`"warm":true`) are never marked and never have attempts.
- `(provider, model)` is not unique within one `usage_by_model`: a provider can serve one request
  in several iterations of the same model, and each is its own share. Key shares by position.

Files:
- `retried-refused-and-unanswered.jsonl`: step 1 failed once and was tried again; step 2 was
  refused by `claude` (billed to it) and answered by `claude-fallback`; step 3's request got no
  answer (`content_received:false`, no usage). Two marked segments, five attempts.
- `handed-on-inside-one-request.jsonl`: one request the provider handed from `claude-opus-5-5`
  to `claude-sonnet-5`. One attempt with two shares, each naming its own segment.
- `pooled-key-fast-tier-cache-split.jsonl`: a run on a pooled API key (`auth_selection`
  `apikey:anthropic`, resolved to `apikey:anthropic-pool-key-2`, account `acct2`) asking for the
  fast tier. Step 1 was served fast (`priority`, speed `fast`), step 2 at the standard tier. Each
  share has a cache write split into 5-minute and 1-hour tokens.
- `no-attempt-before-the-usage-so-unmarked.jsonl`: the run's attempts don't line up with its
  usage, so it gets no attempt facts and its segment is unmarked.
