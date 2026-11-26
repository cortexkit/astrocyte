# astrocyte

> [!WARNING]
> **Alpha.** Astrocyte is in early development and is not yet released for general use.
> Breaking changes will happen between versions, including changes to its settings,
> stored data and interfaces. Pin an exact version if you depend on it.

astrocyte meters the AI usage on one machine. It reads the session stores of
local coding harnesses and broca's run index store directly, accepts usage pushed by peer
modules, and records each request as an immutable usage event. It prices
events in signed nanodollars against the fusiform price catalog, applies
declared subscriptions, and serves spend reports, statement reconciliation and
a subscription multiplier. It runs as a supervised module of the subconscious
daemon (subc) and never sits on a request's hot path. `budget.verdict` always
returns `allow`: cap enforcement is not built.

## Repository map

| Crate | Role |
|---|---|
| `astrocyte-core` | Event identity, ingest, pricing, subscriptions, statements, reconciliation, reports and the SQLite schema. No subc dependency. |
| `astrocyte-module` | The `ck-astrocyte` binary: served operations, configuration, the refresher loop, peer clients and producer mappings; the `spend_drive` and `corpus_drive` tools. |
| `astrocyte-readers` | Parsers for coding-harness session stores and provider-name normalization. |

See the [architecture overview](ARCHITECTURE.md) and the [code map](STRUCTURE.md).

## Install

Once astrocyte is published in the CortexKit release index, use the CortexKit CLI:

```sh
ck setup astrocyte                  # installs and registers the module with subc
ck module status astrocyte --json   # checks the running module
ck upgrade                         # upgrades managed CortexKit components
```

No settings file or OS permissions are required. A missing
`~/.config/cortexkit/astrocyte.jsonc` is a valid empty configuration. Fusiform
is the only peer needed for pricing. Insula, the vault, Colliculus and the
attribution publishers are optional. To import subscription plan tiers, give
astrocyte's vault identity (`reserved:astrocyte`) a `list` grant on the
`llm-provider` category. It does not need permission to read secrets.

For a source build, use a current stable Rust toolchain:

```sh
cargo build --locked --release -p astrocyte-module --bin ck-astrocyte
```

For a development install, set `modules.astrocyte.program` in subc's config
to the absolute path to `target/release/ck-astrocyte` (`ck-astrocyte.exe` on
Windows). Subc supplies the connection file and starts the module.

## Build and test

Use a current stable Rust toolchain. Every dependency resolves from crates.io
through the committed `Cargo.lock`, so building and testing need only the
toolchain; no sibling checkout is needed:

```sh
cargo build --locked -p astrocyte-module
cargo test --workspace --locked
```

The build produces `target/debug/ck-astrocyte`, `target/debug/spend_drive` and
`target/debug/corpus_drive`. `ck-astrocyte` is supervised by subc, not a
standalone daemon; `ck-astrocyte --version` and `ck-astrocyte --check-config
<path>` run without it. The [module surface](docs/module-surface.md) lists the
operations it serves.

The full gate that CI runs is in [CONTRIBUTING.md](CONTRIBUTING.md). The
Windows clippy pass needs the `x86_64-pc-windows-gnu` target
(`rustup target add x86_64-pc-windows-gnu`).

## Documentation

- [Data model](docs/data-model.md): ledger tables, event identity, the priced-charge chain and money.
- [Harness readers](docs/harness-readers.md): where each harness store is found and what one event is.
- [Module surface](docs/module-surface.md): the served operations and the peer calls.
- [Money ingest](docs/money-ingest.md): caller corrections and statement attribution.
- [Configuration](docs/configuration.md): the desired configuration file.
- [Operations](docs/operations.md): subscriptions, repricing, basis corrections and multiplier recomputes.
- [CONTRIBUTING.md](CONTRIBUTING.md): contribution workflow and verification.
- [SECURITY.md](SECURITY.md): private reporting and the local threat model.
- [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md): adapted code.

## License

astrocyte is licensed under the [MIT License](LICENSE). Code adapted from other
projects keeps its own license; see [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).
