# Attribution publisher samples

These byte-for-byte copies of the publishers' samples pin the inputs for
Astrocyte's attribution reader. Each publisher directory retains its upstream
README and SHA256SUMS. The storage migration does not ingest these samples;
reader tests can use the same bytes when ingestion is enabled.

- `basal/`: flow, template instance, flow session, program session, read page
  and bus message from basal's `test-vectors/attribution-facts-v1`.
- `magic-context/`: read pages, refused read and event from the shared
  `mc-attribution-samples-v1` export.
- `entorhinal/`: placement read replies and events from entorhinal's
  `test-vectors/placement-facts-v1`.
- `prefrontal/`: reserved for real publisher samples; no synthetic wire shape
  is supplied in their absence.
