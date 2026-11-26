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
`check_config_accepts_only_the_remaining_members`,
`check_config_names_each_harness_ingest_refusal`.

## When a change takes effect

The file is read once, at startup (`hydrate_startup`). A change applies at
the next start of the module; nothing watches the file. At startup:

| File state | Result |
|---|---|
| Valid | Committed as a new configuration generation when its content differs from the committed one. |
| Missing | The intentional empty configuration is committed: poll every 60 seconds and ingest every source. |
| Unreadable or invalid, a generation was committed before | The committed generation keeps running and health reports the configuration as degraded. |
| Unreadable or invalid, no generation was ever committed | The module stops at startup. |

A missing file is not an error. Removing the file and restarting commits the
intentional empty configuration. Test:
`malformed_file_retains_the_committed_generation_through_the_production_path`.

Configuration and binary move together. The format is closed, so a file
written for a newer binary (an added member) is refused by an older one, and
a file without a member a newer binary requires is refused by it. Deploy the
binary and its matching file together, and check the file with the binary
you are about to run. Remove the old Fusiform and insula connection-path members
before deploying: both peers now use the daemon location supplied by the supervisor.

## Peer connections

Astrocyte calls Fusiform and insula after the daemon accepts its startup
handshake. Pricing waits while Fusiform's catalog is unreachable. Insula serves
`usage.get` snapshots and `events_get` quota bodies. A separate reader binds the
existing durable quota consumer without creating it. Bus failures retain one
snapshot per tick; a healthy reader backs up event delivery with snapshots at the earlier of ten minutes or the
freshness horizon minus one cadence. At the default 30-second cadence, the horizon
stays 120 seconds and quiet readings are refreshed after 90 seconds. Events update
accounts between ticks and reset freshness; empty pulls do not. Owed recovery
snapshots run on the next tick. Capacity is advisory and never blocks spend recording.

## Members

The root is a closed JSONC object (comments and trailing commas allowed).
Both members, `poll_interval_secs` and `harness_ingest`, are required. An
unknown, missing or repeated member is refused at every level.

### `poll_interval_secs`

Integer, at least 1. How often the refresher runs: drains, capacity recovery,
pricing and the daily multiplier stage. A source with no successful drain
within four intervals makes health degraded, and capacity freshness uses four
times the capacity cadence.

### `harness_ingest`

```jsonc
"harness_ingest": { "enabled": true, "exclude": [] }
```

Which sources the refresher drains. `enabled: false` turns every source off.
Otherwise each source key in `exclude` is skipped. Valid keys are the
`INGEST_SOURCES` list: `claude-code`, `claude-code-thalamus`, `codex`,
`opencode`, `opencode-thalamus`, `pi`, `pi-thalamus`, `thalamus-payer`,
`gemini`, `goose`, `omp`, `kimi`, `broca`, `colliculus`. A skipped source
is not read and its `source_runtime` row is removed, so it never reads as
stale. Events it already recorded stay. Refused: an unknown key, a key listed
twice, and `enabled: false` with a non-empty `exclude`. Tests:
`an_unknown_excluded_producer_is_refused_by_name`,
`ingestion_off_with_a_non_empty_exclude_is_refused_as_contradictory`.

The `pi-thalamus` source reads THALAMUS's declared on-demand Pi sessions under
the [Pi sessions contract](thalamus-pi-sessions-contract.md). It is enabled
unless excluded. The new exclusion value adds no config member: existing
documents and stored generations remain compatible without naming it.

## Served quota coverage and pricing

There are no settings for quota providers, pools, model lists, plan prices or
reasoning billing. Insula supplies the quota windows and their model scope.
Fusiform supplies plan prices and model lists for subset pools at the UTC
midnight ending the observation day. A subset list is keyed by the catalog
provider agreed on by the pool's readings and the pool ID, not by the quota
provider's name. If the readings do not agree on a catalog provider or there is
no nonempty list for that pair, the pool is unmeasured. Vault plan history
attributes subscription calls and quota readings by account or paying credential.
See [Subscription multiplier](subscription-multiplier.md) for coverage,
matching rules and eligibility.

Valuation starts from each call's own catalog provider, with any alias or quota
proxy the catalog serves. Pricing and valuation use the model's served
`rate.reasoning`: an explicit `billed_as rate.output` rule uses the output
rate; otherwise reasoning needs its own rate and is unpriced when none applies.
See the [data model](data-model.md#catalog-rate-units-and-quantities) for the
rate grammar and quantity rules.

Remove `subscription_multiplier` and `reasoning_billed_as_output` from the live
file. `--check-config` refuses either member by name. Older configuration
generations already stored in the database still load; the removed values
are ignored, not used as a fallback.

## Reverse a change

Restore the previous file, check it with `--check-config`, and restart the
module. A start whose file differs from the committed one commits it as a new
generation, so reverting is a forward change, not a rollback of the store.
Ledger rows written while
the other configuration ran stay: an excluded source's events, historical
unpriced rows (reprice them with
`spend.event.reprice`; see [operations](operations.md)), and existing
multiplier observations (recompute a day with `spend.multiplier.recompute`).
