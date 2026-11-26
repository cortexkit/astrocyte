# Export-fact caller-owner samples

Six serialized usage-export facts for an external decoder. All identifiers, paths, timestamps and
usage values are synthetic. Each JSON file contains exactly one serialized fact followed by a
newline.

| Daemon scope stamp | Segment | Model attempt | System One |
|---|---|---|---|
| Agent run (`agent_id`, `run_id`) | `agent-segment.json` | `agent-attempt.json` | `agent-system-one.json` |
| Flow (`flow_id`) | `flow-segment.json` | `flow-attempt.json` | `flow-system-one.json` |

The agent segment and attempt facts also carry `agent_run_id`; their System One counterpart carries
the batch scope's `run_id`. The flow facts have no agent or agent-run member. In particular, a
flow-scoped System One fact never inherits an agent from the flow's owner.

Both session runs use the synthetic frozen auth selection that admission records: credential id
`apikey:anthropic`, resolved credential id `apikey:anthropic`, and account ref `acct1`. Every
segment and model-attempt sample therefore carries `auth_selection`, `resolved_credential_id` and
`account_ref`; no credentials or real account identifiers are used.

The files were generated on Linux by `write_fact_owner_samples_when_requested` in
`crates/broca-store/tests/fact_owners.rs`. That test uses `compute_attempt_facts` for session facts
and `SystemOneStore::record_paid` followed by `pending_facts` for System One facts, rather than
constructing the output JSON directly. Reproduce them from the repository root with:

```sh
BROCA_FACT_OWNER_SAMPLES="$PWD/.cortexkit/internal-docs/samples/fact-owners-1126" \
  cargo nextest run --locked -p broca-store -E 'test(write_fact_owner_samples_when_requested)'
```

Verify the captured bytes with `sha256sum -c SHA256SUMS` from this directory.
