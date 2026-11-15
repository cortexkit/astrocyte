# Colliculus two-record voice-call reader fixture

Copied byte for byte from the Colliculus `colliculus-5b` handoff bundle,
received at the workspace-local path
`CortexKit-scratch/shared-astrocyte-readers/colliculus-5b` (not a repository
path). No upstream source revision accompanied the bundle; the upstream
`README.md` describes its generator. That README, request, response and
`SHA256SUMS` are preserved here unchanged.
The manifest's SHA-256 is
`b60aeceee0bdfd6d15815c0efa6580b101bcbe57482c738494857f128d71a9ab`.

The served `usage.read` response contains two records for one voice call:
`openai/gpt-realtime-2.1` reports all six billed token roles, and
`openai/gpt-live-transcribe` reports exactly `3.125` billed input-audio
seconds. Both records share a `call_id`, but have distinct `record_id`s.
Astrocyte preserves them as separate calls, with the shared `call_id` stored
as `run_ref`, and keeps the quantities exact.

Both records include `auth_method: api_key`. The reader therefore records both
with charge basis `metered_api`. A test catalog serves distinct rates for all
six realtime token roles and a per-second rate for the transcription model;
each record prices at its own model's rates, with no unpriced charge.

`media_voice_call_v1_pinned_reply_prices_each_record_at_its_model_rates` verifies
the upstream manifest and every file it names before draining the unchanged
response through the real Colliculus reader. It checks both calls, all stored
measurements and exact charges, including `25,000,000` nano-USD for `3.125`
transcription seconds. The realtime charges in nano-USD are `0` for `input`,
`6` for `input_audio`, `6` for `cache_read`, `4` for `cached_input_audio`,
`0` for `output` and `30` for `output_audio`.
