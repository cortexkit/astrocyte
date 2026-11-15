# Colliculus two-record voice-call reader fixture

Copied byte for byte from the Colliculus `colliculus-5b` handoff bundle,
received at the workspace-local path
`CortexKit-scratch/shared-astrocyte-readers/colliculus-5b` (not a repository
path). No upstream source revision accompanied the bundle; the upstream
`README.md` describes its generator. That README, request, response and
`SHA256SUMS` are preserved here unchanged.
The manifest's SHA-256 is
`417f1c5329ade4bf9105585479842432dea9105358066fb03258b1a5ca8c01e2`.

The served `usage.read` response contains two records for one voice call:
`openai/gpt-realtime-2.1` reports all six billed token roles, and
`openai/gpt-live-transcribe` reports exactly `3.125` billed input-audio
seconds. Both records share a `call_id`, but have distinct `record_id`s.
Astrocyte preserves them as separate calls, with the shared `call_id` stored
as `run_ref`, and keeps the quantities exact.

Neither record includes `auth_method`. The reader therefore records both
with charge basis `unknown`. Even with a catalog serving rates for all seven
measurements, every charge stays unpriced with reason `charge_basis_unknown`;
no rate or amount is applied. These bytes do not demonstrate successful
model-specific pricing.

`media_voice_call_v1_pinned_reply_preserves_records_and_unknown_basis` verifies
the upstream manifest and every file it names before draining the unchanged
response through the real Colliculus reader. It checks both calls, all stored
measurements and the unpriced charge rows without changing the sample or
inferring an authentication method.
