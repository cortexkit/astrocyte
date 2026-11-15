# Recorded Thalamus payer cache-write rows, version 1

These synthetic whole `payer_record` rows were recorded by Thalamus's real billing
intake and payer outbox in commit `9fd72ed81abb281b832324f9f5f46f48974172e0`.
They were copied byte for byte from
`CortexKit-scratch/shared-astrocyte-readers/payer-cache-split-v1/`.
No live store, credential or account was used to produce them.

| File | Recorded cache-write report | Stored `usage_json` |
| --- | --- | --- |
| `split.json` | 8,192 five-minute tokens and 16,384 one-hour tokens | Both lifetime parts |
| `total-only.json` | Only the 24,576-token total | SQL NULL |
| `one-part-only.json` | The 24,576-token total and only the five-minute part | SQL NULL; no inferred one-hour part |

Each JSON object contains all 16 stored columns, including `seq` and
`recorded_at_ms`. The JSON files have no trailing newline. `SHA256SUMS` pins their
exact bytes; its own SHA-256 is
`0da88caaed8de7c821f3b8ca7ac13d05b43cadcd6afc0688afcd393cd9b153e5`.
The reader tests verify both the manifest and the row hashes before importing
the rows into a temporary payer store and matching transcript calls.

Published samples are immutable. A changed recording belongs in a new versioned
directory, not in this one.
