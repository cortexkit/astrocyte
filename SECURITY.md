# Security

## Reporting a vulnerability

Report vulnerabilities privately through GitHub's private vulnerability
reporting. Open the repository's **Security** tab, choose **Report a
vulnerability**, and submit the report:

https://github.com/cortexkit/astrocyte/security/advisories/new

Include the affected version or commit, reproduction steps, expected impact and
relevant platform details. Remove connection files, account references and
private session content from logs and examples. Do not open an issue for an
unpatched vulnerability.

## Threat model

astrocyte runs as the operator's own user account. It reads the session
stores of local coding harnesses: transcripts, rollouts and SQLite databases
that also contain prompts and tool output. It keeps usage metadata from them,
not message content: project root, session, model, provider, account
reference where the harness records one, and token quantities. Unreadable
records are set aside as a source key, a locator and a reason. The ledger is a
SQLite database at `<data home>/cortexkit/astrocyte/store.db`. It reveals which
projects were worked on, when, with which models, and what that cost. Treat it
and its backups as sensitive.

Harness stores are opened read-only (SQLite `mode=ro`, or plain reads of
append-only files). astrocyte never writes a harness's store. The one file it
writes outside its own store is the Claude worker read acknowledgement,
`<data home>/cortexkit/astrocyte/acks/claude-workers.json`, created with mode
`0600` in a directory set to `0700` on Unix. A declared Claude worker root must
resolve, after symlinks, inside the folder that holds the harness module's
`roots.json`; any other root is refused, so a declaration cannot point
astrocyte at arbitrary folders. See the
[Claude workers contract](docs/claude-workers-contract.md).

astrocyte trusts the local subc daemon to authenticate clients and to decide
who may open a route to astrocyte's management surface. Served operations
carry no operator identity: the caller is whoever subc admitted to that
route. That caller can write to the ledger (ingest events and
corrections, declare subscriptions, reprice) and can make astrocyte read any
regular file of at most 64 MiB that its user can read, by naming it as a
`spend.statement.import` path. The file is parsed as a statement and its
content is not returned, but parse errors name line numbers and fields.

The subc connection files named in the configuration
(`qta_connection_file`, `fusiform_connection_file`) and the one passed to
`spend_drive` let their holder authenticate to the daemon. Keep them
owner-only and out of repositories and shared directories.

These boundaries are not a sandbox against a compromised local user account.
A process running as the same user can read the ledger and the harness stores
directly, edit the configuration file, replace the binary, or write the
harness stores astrocyte reads, which changes what it records. Protect the
daemon, module binaries, data home and user account as one trust boundary.

The read-only harness access is in
[`sqlite_read.rs`](crates/astrocyte-readers/src/sessions/sqlite_read.rs) and
[`sqlite_uri.rs`](crates/astrocyte-readers/src/sqlite_uri.rs); the worker-root
containment and acknowledgement write are in
[`claude_workers_source.rs`](crates/astrocyte-module/src/claude_workers_source.rs);
the statement file checks are `read_statement_file` in
[`ops.rs`](crates/astrocyte-module/src/ops.rs). See the
[architecture](ARCHITECTURE.md) for the data flow.
