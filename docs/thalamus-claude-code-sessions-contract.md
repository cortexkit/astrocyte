# THALAMUS Claude Code sessions: roots and read acknowledgement (v1)

This is the binding contract between THALAMUS (`thalamus-harness`), which runs
Claude Code sessions, and astrocyte, which records their usage. Change a meaning
only by agreement of both modules and bump `version`. The implementation is
`crates/astrocyte-module/src/claude_thalamus_source.rs`.

## 1. Private sessions

Every primary agent (head) or delegated worker session runs with a private `CLAUDE_CONFIG_DIR` at
`<cortexkit data home>/thalamus-harness/claude-code/sessions/<session>/config`.
The cortexkit data home is `$XDG_DATA_HOME/cortexkit`, falling back to
`~/.local/share/cortexkit`, as resolved for astrocyte's store. These transcripts
are outside the user's usual `~/.claude/projects`. THALAMUS declares them so
astrocyte can read them without depending on a peer call.

## 2. Roots file, written by THALAMUS

THALAMUS atomically replaces
`<cortexkit data home>/thalamus-harness/claude-code/roots.json`:

```json
{
  "version": 1,
  "roots": [
    {
      "session": "01234567-89ab-4cde-8123-456789abcdef",
      "session_ref": "runner/head",
      "path": "/home/me/.local/share/cortexkit/thalamus-harness/claude-code/sessions/01234567-89ab-4cde-8123-456789abcdef/config",
      "owner_principal": "prefrontal-core",
      "state": "active",
      "done_at_ms": null
    }
  ]
}
```

- `session` is Claude Code's own session id, which THALAMUS supplies with
  `--session-id`. It must be nonempty, cannot be `.` or `..`, cannot contain a
  path separator, and cannot occur in two entries. Astrocyte does not impose
  an additional UUID syntax check.
- `session_ref` is the runner's reference. It is required as a string but does
  not change event identity or attribution.
- `path` is the private config directory, not its `projects` subdirectory. It
  must be absolute. After symlink resolution it must be exactly
  `<cortexkit data home>/thalamus-harness/claude-code/sessions/<session>/config`,
  relative to the resolved folder containing the roots file. The path's session
  component must equal the entry's `session`. The harness folder itself, session
  folders without `config`, descendants of `config`, other harness subfolders,
  mismatched sessions and escaping symlinks are all refused. Entries resolving
  to the same folder, including aliases, are refused rather than choosing an owner.
- `owner_principal` is stored verbatim as `work_class` on newly recorded calls.
- `state` is exactly `active` or `done`. Once `done`, the session will never write
  again. `done_at_ms` is null while active and the Unix millisecond completion
  time when done; like the worker reader, astrocyte uses `state`, not this time,
  to decide whether the session can be acknowledged.

The decoder follows the [worker roots decoder](claude-workers-contract.md):
known required fields and their types must decode, and unknown fields at the
top level or in an entry are ignored. `done_at_ms` is not interpreted by the
reader. Missing or malformed versions degrade the source; an unknown unsigned
integer version skips the roots and degrades rather than guessing. An unreadable
or malformed file keeps the last good roots and degrades, without issuing new
acknowledgements. An absent roots file means no source row and no health change.
A refused entry is not read or acknowledged and degrades the source with a
reason; otherwise-valid entries continue to read. A missing session folder is
skipped without degradation, as it may not yet exist or may already be removed.

## 3. Recording and already-recorded calls

Astrocyte reads `<path>/projects/**/*.jsonl`, including nested subagent files,
with the existing Claude Code reader. Producer stays `claude-code` and identity
stays the normal Claude Code identity based on the request id, without adding a
session lineage. The independent health and progress source key is
`claude-code-thalamus`. It is enabled by default and can be excluded through
`harness_ingest.exclude`; exclusion stops reads and new acknowledgements and
removes the source's runtime row. Active heads can stay active forever and are
read on every full refresher tick.

If a call's identity already exists in the ledger, it counts as recorded. The
first event, including its original work class and measurements, stands.
THALAMUS can move or copy a session's earlier transcripts from another config
directory into a private folder; the new owner label must not turn that history
into quarantined contradictions. This identity check happens in the recording
transaction, as it does for declared OpenCode sessions. The usual overlap check
for requests already captured by the THALAMUS gateway still applies.

## 4. Read acknowledgement, written by astrocyte

Astrocyte atomically writes
`<cortexkit data home>/astrocyte/acks/claude-code.json`, using the worker
acknowledgement shape but keyed by `session`:

```json
{"version": 1, "read_through": [{"session": "01234567-89ab-4cde-8123-456789abcdef", "at_ms": 1791850000000}]}
```

A session appears only when its declaration is `done` and all its calls are
recorded or already recorded, with no pending, refused, set-aside or quarantined
calls. Completed calls are ready immediately. Calls without a completion marker
must pass the normal Claude Code 10-minute quiet window, even in a done session.
An empty config can be acknowledged after its read commits. Active sessions
never receive a new acknowledgement.

The acknowledgement is written only after the ledger transaction commits. A
failed transaction neither advances the reader's in-memory offsets nor issues
an acknowledgement. `at_ms` is when astrocyte determined the session was fully
read; an existing timestamp is preserved across ticks and restarts. Entries no
longer declared are removed so the file stays small. The file uses the same
atomic replacement and private Unix permissions as worker acknowledgements.

## 5. Retention belongs to THALAMUS

Astrocyte never deletes a session folder or writes inside a harness config.
THALAMUS marks a session done before considering deletion and never deletes a
session astrocyte has not acknowledged. It can remove an acknowledged done
session and then remove its roots entry. If acknowledgement is still overdue
24 hours after `done_at_ms`, THALAMUS marks its own health degraded instead of
deleting the unacknowledged data. THALAMUS never imposes a time limit after which
it deletes an unacknowledged session, even when astrocyte is absent, excluded or
unable to catch up.

## 6. Tests

Tests live beside the source in `claude_thalamus_source.rs`. They cover valid
reads, unchanged event identity and verbatim ownership, all refused folder
shapes, duplicate and aliased entries, active heads, quiet-window completion,
post-commit acknowledgements, moved transcripts, configuration exclusion,
absent roots, unknown versions, malformed registries and blocked resumed calls.
