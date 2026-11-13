# THALAMUS Pi sessions: roots and read acknowledgement (v1)

This is the binding contract between THALAMUS (`thalamus-harness`), which runs
Pi sessions, and astrocyte, which records their usage. Change a meaning only by
agreement of both modules and bump `version`. The implementation is
`crates/astrocyte-module/src/pi_thalamus_source.rs`.

## 1. Private sessions

THALAMUS starts on-demand Pi sessions in private folders at
`<data home>/cortexkit/thalamus-harness/pi-sessions/<one component>`, outside
Pi's own session root. The data home is `$XDG_DATA_HOME`, falling back to
`~/.local/share`, as resolved for astrocyte's store. Astrocyte only reads these
folders and never writes inside them. Pi mints its own session id; THALAMUS
learns it shortly after launch rather than choosing it in advance.

## 2. Roots file, written by THALAMUS

THALAMUS atomically replaces
`<data home>/cortexkit/thalamus-harness/pi/roots.json`:

```json
{
  "version": 1,
  "roots": [
    {
      "path": "/home/me/.local/share/cortexkit/thalamus-harness/pi-sessions/worker-one",
      "session": null,
      "owner_session": "ses_core_bound_conversation",
      "owner_principal": "prefrontal-core",
      "state": "active",
      "done_at_ms": null
    }
  ]
}
```

- `path` must be absolute. After symlink resolution it must be exactly
  `<data home>/cortexkit/thalamus-harness/pi-sessions/<one component>`, relative
  to the resolved harness folder containing `pi/roots.json`. The harness folder,
  `pi-sessions` itself, deeper descendants, arbitrary folders and escaping
  symlinks are refused. A folder inside Pi's normal session root, including an
  overridden root, is also refused. Entries resolving to the same canonical
  folder are all refused, including aliases with different owners.
- `session` is required as a string or null. It is null until Pi attaches, then
  holds Pi's native session id. It is never a substitute for a file's header.
- `owner_session` must be a nonempty string, unique across entries. It names
  the owning conversation rather than Pi's newly minted session. It becomes
  the session on recorded calls and keys the read acknowledgement. Duplicate
  owners are all refused rather than choosing the first entry.
- `owner_principal` is stored verbatim as `work_class` on newly recorded calls.
- `state` is exactly `active` or `done`. Once done, the folder will never write
  again. A done entry must include the Unix millisecond completion time in
  `done_at_ms`. Astrocyte cannot acknowledge it before that time.

Known fields and their types must decode; unknown top-level and entry fields
are ignored. An absent roots file means no source row and no health change.
An unreadable or malformed roots file keeps the last good declaration list and
degrades the source without issuing new acknowledgements. Missing or malformed
versions degrade; unknown unsigned integer versions are refused without reading
their roots. Refused entries degrade the source with a reason, while valid
entries continue. A missing session folder is skipped without degradation and
does not receive a new acknowledgement.

## 3. Recording and already-recorded calls

Astrocyte reads every `*.jsonl` recursively under each declared folder with the
existing Pi parser. Each file's session header supplies its native session id.
With a null declaration session, the header id is used directly. With a non-null
declaration session, every file's header must match; a disagreement refuses the
entire folder by name, records none of its calls and issues no acknowledgement.
Unreadable or rejected records also prevent the folder from being acknowledged.
As in Pi's normal reader, readable calls can still commit while refused lines
withhold acknowledgement; a header disagreement instead refuses all calls in
the folder.

Producer stays `pi`, with the existing `pi_step_ref` identity and no extra
lineage. A copied call is not a second billable event, including a call already
read through Pi's normal root. The independent health and progress source key
is `pi-thalamus`. It is enabled by default and can be excluded through
`harness_ingest.exclude`. Exclusion stops reads and new acknowledgements and
removes the source's runtime row. Adding this exclusion value does not change
the closed config's members or require any edit to an existing live config.

New calls record `owner_session` as their session and `owner_principal` as their
work class. Payer matching still uses each file's native Pi session id and the
ordinary Pi payer wait queue under the
[payer records contract](thalamus-payer-records-contract.md). Calls wait for payer
facts up to the existing ten-minute bound; expiry records unknown payer facts
before the folder becomes eligible for acknowledgement.

If a call identity already exists in the ledger, it counts as recorded. The
first event's session, work class, measurements and money stand. Copied history
is neither rewritten nor quarantined because its new owner differs. The identity
check runs inside the recording transaction, including across folders in one
drain.

## 4. Read acknowledgement, written by astrocyte

Astrocyte atomically writes
`<data home>/cortexkit/astrocyte/acks/pi.json`, keyed by `owner_session` in the
existing acknowledgement format's `session` field:

```json
{"version":1,"read_through":[{"session":"ses_core_bound_conversation","at_ms":1791850000000}]}
```

An entry appears only for a done declaration, at or after `done_at_ms`, once
every call in its folder is recorded or already recorded. Calls still waiting
for payer facts, including waits approaching their bound, prevent the entry.
A done empty folder or a file containing no calls can be acknowledged once the
done marker is seen. Active declarations never receive a new acknowledgement.

The acknowledgement is written only after the recording transaction commits.
A failed transaction neither adopts proposed file offsets or a payer queue nor
acknowledges the folder; the next read retries its calls. The writer and format are
shared with Claude Code's acknowledgement, including atomic replacement and
private Unix permissions. Existing timestamps survive ticks and restarts and
cannot precede `done_at_ms`. Entries no longer eligible or declared are pruned.

Full refresher ticks discover declarations and reuse Pi's per-file resume state,
bounded tail checks and staggered daily full-prefix verification. An unchanged
file parses no old lines; an appended file parses only its new lines. The native
header id stays with the cached resume point, so even deferred or empty reads
still check declarations against it. Refused records remain blocked across
resumes until a verified rewrite removes the problem. A changed owner invalidates
the affected file's offset so its unrecorded calls cannot be skipped.
Fast cycles recheck only
queued payer waits, opening neither roots files nor transcripts; an idle fast
cycle does nothing. A wait released on the fast lane is acknowledged on the
next eligible full tick.

## 5. Retention belongs to THALAMUS

THALAMUS marks a folder done before considering deletion. It deletes a done
folder only after seeing its owner-session acknowledgement, then removes its
declaration. It never deletes unacknowledged transcripts merely because
astrocyte is absent, excluded or unable to catch up.

## 6. Tests

Tests in `pi_thalamus_source_tests.rs` cover absent roots, containment and
canonical aliases, duplicate owners, native-header payer joins, mismatched
headers, ownership, transaction failure and retry, empty done folders, copied
history, payer timeout, idle fast cycles, registration and config exclusion.
Counter tests read 1,000 calls, then unchanged and one-line-appended files,
asserting bounded hashed bytes and zero or append-only parsed bytes for active
and done declarations. They also exercise the daily full check, cached-header
refusal, blocked resumed records and owner changes during payer waits.
The synthetic session and roots fixture are pinned under
`crates/astrocyte-module/tests/vectors/pi-thalamus/`. No personal data is used;
a real recorded sample can be added separately when THALAMUS supplies one.
