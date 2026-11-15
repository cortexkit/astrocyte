# Voice-call usage page for Astrocyte

`usage_read.request.json` and `usage_read.response.json` are the exact request
and response body bytes from the production request handler, without rewriting
ids, timestamps or the reply envelope. The usage-reader principal is
`reserved:astrocyte`. All account and credential identifiers are invented.

The test assembly uses a fake vendor and runner. It mints and attaches one
voice-front call, reports all six realtime token roles and 3.125 transcription
seconds, ends the call, then reads the feed. Both records share the call and
routing decision. The routed model is `gpt-realtime-2.1`; the seconds model is
`gpt-live-transcribe`. No network or real credential is used.

Regenerate with:

```sh
cargo test --locked -p colliculus-module --all-features regenerate_astrocyte_voice_call_sample -- --ignored
```

`astrocyte_voice_call_sample_matches_served_bytes` replays the same handling and
checks both byte files, the complete file inventory and every SHA256SUMS entry.
