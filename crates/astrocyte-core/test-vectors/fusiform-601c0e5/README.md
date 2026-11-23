# Fusiform 601c0e5: dated speed-twin history

`history-moonshotai-kimi-k3.1-pro-rate.input.mode.fast.json` is the `response`
of the golden case "catalog.history — served Moonshot Highspeed rate" in
Fusiform's `crates/fusiform-module/fixtures/served-payloads.json` at `601c0e5`
(golden sha256 `ea713760caa83c9d28549011232e7e4f82bb8ae99aedd7431782e485eff179c0`),
re-serialized with sorted keys. Its request was
`{"fact_key": "rate.input.mode.fast", "model_id": "kimi-k3.1-pro", "provider_id": "moonshotai", "served": true}`.

The fast input rate is absent until 2026-10-23 19:05:57Z (`1792782357000`) and
$4 per million tokens from then on, projected from the Highspeed twin's own era
(`source: "speed_twin"`, new in `fusiform-protocol` 0.81.0). `SHA256SUMS` pins
the file.
