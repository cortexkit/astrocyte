# Reasoning billed as output: reader samples

These JSON files are reply bodies returned by the real `serve_tool_call` routes
and serialized with Serde against a temporary SQLite store seeded from the
embedded models.dev snapshot. The generator also recorded the shipped
fact-override eras. No cloud store was read or written.

| Reply file(s) | Request |
| --- | --- |
| `openai-current.json`, `openai-dated.json` | `catalog.get` for `openai/gpt-6.1-sol` |
| `anthropic-current.json`, `anthropic-dated.json` | `catalog.get` for `anthropic/claude-opus-5-5` |
| `openai-history.json` | `catalog.history` for `openai/gpt-6.1-sol`, `fact_key: "rate.reasoning"`, `served: true` |
| `kimi-code-plan-global.json` | `catalog.get` for `kimi-code-plan-global/k3.1-pro`, `auth_method: "apikey"` |
| `opencode-go.json` | `catalog.get` for `opencode-go/kimi-k3.1-pro`, `auth_method: "apikey"` |

The API `catalog.get` requests filter to `rate.output` and `rate.reasoning`;
the plan requests also ask for `model.canonical_id`. The dated requests set
`at_ms: 1795629090352`, just after the seeded record begins. The reason the
history sample does not contain a pre-rule segment is that this seed's record
begins after the rule was established. `eras` is empty because the upstream
never published the reasoning key; `served` is the catalog's override history.

For `rate.reasoning: {"state":"billed_as","key":"rate.output"}`, use the
output key selected for the request, including context and mode variants. A
more specific published reasoning price is not a request to follow the fallback.
The catalog does not duplicate Moonshot's fallback into `rate.reasoning.mode.fast`;
the base rule follows `rate.output.mode.fast` for fast requests.

- **Kimi Code:** `model.canonical_id` is `moonshotai/kimi-k3.1-pro`. This
  API-key billing plane is a quota proxy that uses that exact Moonshot API card.
  The output price names its origin under `inherited_from`; the plan also serves
  Moonshot's reasoning rule, with a `fact_overrides` disclosure naming the plan
  model and the Moonshot source. These prices are quota weights, not per-token
  subscription charges.
- **OpenCode Go:** the canonical ID is also `moonshotai/kimi-k3.1-pro`, but the
  Go quota proxy uses prices on its own provider card, not the canonical donor's
  API prices. No OpenCode source established reasoning billing for that card, so
  the reply has no `rate.reasoning`. Do not infer the rule from the canonical ID.

Use the served fact instead of a provider list: Kimi Code has the rule; OpenCode
Go does not assert one.
