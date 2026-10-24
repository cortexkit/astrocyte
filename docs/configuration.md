# Configuration

This guide covers the desired configuration file that `ck-astrocyte` reads at
startup: where it lives, every member, how to check it before a restart, and
how to undo a change. The decoder is `crates/astrocyte-module/src/config.rs`;
`docs/prod-config/astrocyte-v2.jsonc` is a complete, commented example.

## Where the file lives

`<config home>/cortexkit/astrocyte.jsonc`, where the config home is
`XDG_CONFIG_HOME`, else `~/.config`. If neither variable resolves to an
absolute path, astrocyte does not guess: it treats the file as unreadable
(see the table below).

## Check a file before you use it

```sh
ck-astrocyte --check-config /path/to/astrocyte.jsonc
```

This decodes and validates the file without opening the store, connecting to
subc or touching the network. It exits 0 when the file is valid, and when the
file does not exist (it then prints that a missing file is treated as the
intentional empty configuration). It exits non-zero with the reason when the
file cannot be read or decoded. Tests:
`check_config_accepts_the_closed_live_document`,
`check_config_names_each_harness_ingest_refusal`.

## When a change takes effect

The file is read once, at startup (`hydrate_startup`). A change applies at
the next start of the module; nothing watches the file. At startup:

| File state | Result |
|---|---|
| Valid | Committed as a new configuration generation when its content differs from the committed one. |
| Missing | The intentional empty configuration is committed: poll every 60 seconds, no QTA or fusiform connection, no quota providers, every source ingested. |
| Unreadable or invalid, a generation was committed before | The committed generation keeps running and health reports the configuration as degraded. |
| Unreadable or invalid, no generation was ever committed | The module stops at startup. |

A missing file is not an error. Removing the file and restarting commits the
intentional empty configuration, which drops both peer connections. Test:
`malformed_file_retains_the_committed_generation_through_the_production_path`.

Configuration and binary move together. The format is closed, so a file
written for a newer binary (an added member) is refused by an older one, and
a file without a member a newer binary requires is refused by it. Deploy the
binary and its matching file together, and check the file with the binary
you are about to run.

## Members

The root is a closed JSONC object (comments and trailing commas allowed).
All six members are required. An unknown, missing or repeated member is
refused, at every level. `null` means "not configured"; a present string must
not be empty.

### `poll_interval_secs`

Integer, at least 1. How often the refresher runs: drains, capacity recovery,
pricing and the daily multiplier stage. A source with no successful drain
within four intervals makes health degraded, and capacity freshness uses four
times the capacity cadence.

### `qta_connection_file`

String or `null`. The subc connection file used to reach insula, the quota
module, for `usage.get` snapshots and `events_get` quota bodies. `null` disables
capacity ingestion. When configured, a separate reader binds the bus durable
without creating it. Bus failures retain one snapshot per tick; a healthy reader
backs up event delivery with snapshots at the earlier of ten minutes or the
freshness horizon minus one cadence. At the default 30-second cadence, the horizon
stays 120 seconds and quiet readings are refreshed after 90 seconds. Events update
accounts between ticks and reset freshness; empty pulls do not. Owed recovery
snapshots run on the next tick. Capacity is advisory and never blocks spend recording.

### `fusiform_connection_file`

String or `null`. The subc connection file used to reach the fusiform price
catalog. With `null`, every event is stored unpriced with reason
`catalog_unconfigured`, and `spend.multiplier.recompute` refuses with
`catalog_unavailable`.

### `harness_ingest`

```jsonc
"harness_ingest": { "enabled": true, "exclude": [] }
```

Which sources the refresher drains. `enabled: false` turns every source off.
Otherwise each source key in `exclude` is skipped. Valid keys are the
`INGEST_SOURCES` list: `claude-code`, `claude-code-workers`, `codex`,
`opencode`, `opencode-thalamus`, `pi`, `gemini`, `goose`, `omp`, `kimi`, `broca`. A skipped source
is not read and its `source_runtime` row is removed, so it never reads as
stale. Events it already recorded stay. Refused: an unknown key, a key listed
twice, and `enabled: false` with a non-empty `exclude`. Tests:
`an_unknown_excluded_producer_is_refused_by_name`,
`ingestion_off_with_a_non_empty_exclude_is_refused_as_contradictory`,
`excluded_workers_are_not_read_and_runtime_is_removed`.

### `reasoning_billed_as_output`

Array of non-empty provider ids, each listed once. A listed provider's reasoning
tokens always use its output rate, even when a reasoning rate exists. This
billing fact applies to the spend ledger and the subscription multiplier,
including mode selection; ledger tier refusals and subscription coverage still
win. Unlisted providers require their own reasoning rate: there is no default.
The production example declares `openai` and `anthropic`. When restoring a saved
configuration from the store, the module accepts the old
`subscription_multiplier.reasoning_billed_as_output` location, or uses an empty
list if none was saved. New configuration files must put
`reasoning_billed_as_output` at the root, not inside `subscription_multiplier`.

### `subscription_multiplier`

The declarations behind the daily subscription multiplier: what each
subscription's quota is worth at API list prices. It never changes the spend
ledger. All three members are required; use `{}` to declare nothing.

| Member | Shape | Meaning |
|---|---|---|
| `api_twins` | map of provider to provider | A plan-side provider priced at another provider's list rates, same model id (for example `openai-codex` to `openai`). The two must differ. |
| `plan_price_overrides` | map of plan key to price | A monthly price used instead of fusiform's for that plan key. |
| `quota_providers` | map of quota provider to declaration | One entry per quota provider, keyed by the provider name the quota module reports. |

A quota provider declaration has three required members:

- `window`: `{"pool_key": <non-empty>, "window_minutes": 1..43200}`, the one
  quota window the used percentage is read from.
- `scope`: a non-empty array of `{"producer", "provider", "charge_basis"}`
  rules naming the traffic that counts against this quota. `charge_basis` is
  required and is `null` (every basis) or one of `metered_api`,
  `subscription_included`, `unknown`. A rule may not repeat.
- `accounts`: a non-empty map of account reference to `{"plan": "<provider>/<tier>",
  "price_override": <price or null>}`.

A price is `{"minor_units": >0, "exponent": 0..9, "currency": "USD",
"period": "month"}`; `20000` at exponent `2` is 200.00. An account's
`price_override` wins over `plan_price_overrides`, which wins over fusiform.

A quota provider whose scope names a source that `harness_ingest` skips
records no multiplier day; the daily stage logs
`scoped_producer_not_ingested` until the source is ingested again. Tests:
`the_multiplier_member_is_closed_at_every_level`,
`multiplier_values_are_validated`, `the_production_config_decodes`.

## Reverse a change

Restore the previous file, check it with `--check-config`, and restart the
module. A start whose file differs from the committed one commits it as a new
generation, so reverting is a forward change, not a rollback of the store.
Ledger rows written while
the other configuration ran stay: an excluded source's events, unpriced rows
written while no catalog was configured (reprice them with
`spend.event.reprice`; see [operations](operations.md)), and multiplier days
recorded under other declarations (recompute them with
`spend.multiplier.recompute`).
