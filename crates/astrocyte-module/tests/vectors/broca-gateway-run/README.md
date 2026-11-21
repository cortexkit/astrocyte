# Company AI gateway keys (broca design 32, gateway half)

A company gateway key is filed in the vault under the gateway's own provider id (here
`anthropic-gw-acme`), with `auth_method: "gateway"`, `serves: ["anthropic"]`, and category
`llm-gateway`. The vault serves it with the gateway's root address (`base_url`), the header and
scheme its key goes in (`auth`), and fixed headers (`headers`), as CKCRED's
`gateway-keys-vectors-v1` define. The model catalog has no rows for the gateway's provider id,
so a run on `anthropic-gw-acme/<model>` borrows Anthropic's rows, as the paid-key pool does
(`pool-route-1113`).

Produced by broca's real serializers in the test
`a_gateway_key_run_goes_to_its_gateway_with_the_gateways_headers` (one synthetic gateway key,
an HTTP stand-in for the gateway on the loopback, model `claude-haiku-4-5`). The project root
in the segment is the test's scratch folder; the key `gateway-key` is synthetic.

Files:
- `gateway-run-export-fact-segment.json`: the usage export fact's segment
  (`export_facts.segment_json`). `provider` is `anthropic`, the provider whose rates apply,
  never the gateway's id. `auth_selection` is broca's routing key
  `gateway:anthropic-gw-acme`; `resolved_credential_id` is the gateway key that paid.
  `charge_basis` is `unknown`: the shipped table has no `(anthropic, gateway)` row yet (see
  the question to ASTRO below).
- `gateway-run-frozen-run-config.json`: the run's frozen config. `model.provider_module_id`
  stays `anthropic-gw-acme`; `served_provider: "anthropic"` records the borrowed provider, as
  on a pool run. `frozen_render_config.base_url` is the gateway's address, so a resume sends
  to the address the run started with even if the vault record changes later.
- `gateway-run-request-body.json`: the body the gateway receives. The test asserts it is
  byte-identical to the same send on `anthropic/claude-haiku-4-5` with a paid key.
- `gateway-run-request-headers.txt`: the request line and headers. The key goes in the header
  the vault named (`authorization: Bearer …` here), followed by the gateway's fixed headers
  (`x-team`). No `x-api-key`, no Claude Code persona. The headers are never frozen or stored.

New on the wire: `role.describe` lists `gateway` in `routed_accounts`, and broca's routing
runner slot adds the pair `(anthropic.messages, gateway)` with `requires_wire_api: false`.

Open question for ASTRO: should the shipped charge-basis table gain `(anthropic, gateway) =
metered_api` (mapping version 6), so gateway spend is priced at Anthropic's rates, or stay
`unknown` until a gateway's own pricing is known?
