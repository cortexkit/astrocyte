# Attribution facts v1

Privacy-bounded samples for Astrocyte. The generator uses real flow approval,
template instantiation, flow model preparation, codemode model preparation,
`Module::handle("attribution.read")`, and the production outbox publisher on a
real JetStream server. Synthetic run identities and program admission metadata
make the bytes deterministic. No prompt, script, parameter text or path enters
the facts. The bus vector retains only the subject, deduplication header and
body Astrocyte reads.

- `flow.json`: first approved ordinary flow.
- `instance.json`: template instance (package instances use the same fact shape).
- `flow-session.json`: exact flow model-call session, with its parent flow.
- `program-session.json`: exact program model-call session. The admitted bind
  contains harness/session but no runner, so its parent is null.
- `read-page.json`: the first two facts, with numeric `next` and `through`.
- `bus-message.json`: an actual published message received from JetStream.

Both session subjects explicitly carry `runner: "broca"` and `harness: "broca"`.
`SHA256SUMS` covers every generated JSON file, not this README.

Regenerate on Linux (requires the repository's pinned `nats-server` on PATH):

```sh
BASAL_WRITE_VECTORS=1 cargo test --locked -p basal-module --test module_events bus_notices::attribution_outbox_publishes_once_and_vectors_match_real_paths -- --exact
```

Use the same command without `BASAL_WRITE_VECTORS=1` for drift verification.
Remote regeneration must return all six JSON files and `SHA256SUMS` to the
worktree. The test also checks exact directory membership, so adding a file
without exercising it cannot silently expand the contract.
