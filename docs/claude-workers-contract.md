# Headless Claude Code workers: roots file and read acknowledgement (v1)

This is the binding contract between the thalamus-harness module, which
launches headless Claude Code worker processes, and astrocyte, which records
their usage. Change it only by agreement of both modules, and bump `version`
when a meaning changes. The astrocyte side is
`crates/astrocyte-module/src/claude_workers_source.rs`.

## 1. Why it exists

Each headless worker runs with a private `CLAUDE_CONFIG_DIR` and `HOME` under
the harness module's state, so its session transcripts are not under
`~/.claude/projects`, the one root astrocyte's Claude Code reader resolves by
default. A transcript also carries no field saying what kind of work it was.
The harness module therefore declares where its workers' transcripts are and
what to call that work. Astrocyte reads those transcripts with its existing
Claude Code reader and records every call under the declared work class
(`claude-code-worker`), so per-work-class reports and pace can tell worker
sessions from head sessions.

Nothing else records these requests as usage: they go from the harness
module's gateway straight to Anthropic, never through BROCA, and the gateway
has no usage export.

## 2. Roots file (written by the harness module)

Path: `<cortexkit data home>/thalamus-harness/claude-workers/roots.json`, where
the cortexkit data home is `$XDG_DATA_HOME/cortexkit`, falling back to
`~/.local/share/cortexkit`. The worker directories sit beside it in the same
`claude-workers/` folder. The harness module writes the file atomically (a
temporary file, then rename).

```json
{
  "version": 1,
  "roots": [
    {
      "worker_id": "w-0123456789abcdef0123456789abcdef",
      "path": "/home/me/.local/share/cortexkit/thalamus-harness/claude-workers/w-0123456789abcdef0123456789abcdef",
      "work_class": "claude-code-worker",
      "state": "active",
      "done_at_ms": null,
      "account_ref": null
    }
  ]
}
```

- `worker_id`: `w-` followed by 32 lowercase hexadecimal characters. Unique
  per worker process.
- `path`: the worker's private `CLAUDE_CONFIG_DIR`. Astrocyte reads
  `<path>/projects/**.jsonl`. It must be absolute and, after symlinks are
  resolved, inside the `claude-workers/` folder that holds `roots.json`.
  Astrocyte refuses any other path, logs it and skips that root; the other
  roots still read. This stops a declaration from pointing astrocyte at
  arbitrary folders.
- `work_class`: stamped verbatim on every event recorded from that root.
- `state`: `active` while the worker may still write, `done` once it has
  exited. `done_at_ms` (Unix milliseconds) is set when the state becomes
  `done`, and is null while active.
- `account_ref`: reserved. Astrocyte ignores it in v1; send null. Per-account
  attribution would also need a pricing rule for these events and belongs to
  a later version. If it is ever filled, a worker keeps one account for its
  whole life.

Unknown fields, at the top level or in a root, are ignored, so either side
can add fields without a coordinated release. A changed meaning bumps
`version`.

| Roots file state | Astrocyte's behaviour |
|---|---|
| Absent | No workers: no source row, health unaffected. |
| Unknown `version` | Skips the roots and reports the source degraded rather than guessing. |
| Unreadable or malformed | Keeps the last good root list and reports the source degraded. |

Tests: `relative_root_is_refused_without_blocking_other_roots`,
`outside_root_is_refused_without_blocking_other_roots`,
`escaping_symlink_is_refused_without_blocking_other_roots`,
`unknown_version_skips_roots_and_degrades_source`,
`malformed_registry_reads_last_good_roots_and_degrades`,
`unreadable_registry_reads_last_good_roots_and_degrades`,
`absent_registry_removes_runtime_and_ack_entries`.

## 3. What astrocyte records

Events from declared roots keep producer `claude-code`, since Claude Code ran
the loop; event identity is unchanged (Claude Code's own request id). They
carry the declared `work_class`. Their reading progress and health use a
separate source row, `claude-code-workers`, apart from the main Claude Code
root. Astrocyte re-reads `roots.json` on every refresher tick
(`poll_interval_secs`), so a completed call is recorded within about one
tick. A call with no completion marker is recorded after the reader's
10-minute quiet window, as for any Claude Code transcript. Tests:
`worker_events_stamp_declared_class_without_changing_identity`,
`main_claude_root_still_has_no_work_class`.

## 4. Read acknowledgement (written by astrocyte)

Neither module writes into the other's folders. Astrocyte writes
`<cortexkit data home>/astrocyte/acks/claude-workers.json` atomically:

```json
{"version": 1, "read_through": [{"worker_id": "w-0123456789abcdef0123456789abcdef", "at_ms": 1791850000000}]}
```

A worker appears in `read_through` once its root is `done` and astrocyte has
recorded every complete call in it, including any unfinished call whose quiet
window has passed. `at_ms` is when astrocyte made that determination. The
acknowledgement is written only after the events it covers are committed.
Astrocyte drops an entry once the worker no longer appears in `roots.json`,
so the file stays small. Tests:
`ack_requires_done_and_waits_for_quiet_pending_call`,
`ack_is_never_written_before_event_transaction_commits`,
`ack_drops_removed_workers_and_preserves_timestamp_on_restart`.

## 5. Deleting a finished worker (harness module)

Delete a `done` worker directory once it appears in `read_through` and at
least one hour has passed since `done_at_ms`. If no acknowledgement ever comes
(astrocyte not installed, or never caught up), delete it 7 days after
`done_at_ms`. A worker deleted under that cap without an acknowledgement may
lose usage, which is accepted so that removing astrocyte can never make the
harness module keep data forever. Mark a worker `done` in `roots.json` before
deleting its directory, and remove its entry when the directory is gone.
Astrocyte skips a deleted worker without degrading the source (test:
`deleted_worker_is_skipped_without_degradation`).

## 6. Not guaranteed

- Usage of a worker deleted before its acknowledgement may be lost (§5).
- `account_ref` is not read in v1, so worker events carry no account.
- Excluding `claude-code-workers` in `harness_ingest` stops reading the roots
  (test: `excluded_workers_are_not_read_and_runtime_is_removed`), so no worker
  is newly acknowledged. The harness module then deletes such workers on the
  7-day cap.
