# THALAMUS OpenCode sessions: roots file and read watermark (v1)

This is the binding contract between the thalamus-harness module, which runs
OpenCode sessions in private or shared OpenCode servers, and astrocyte, which
records the usage of those sessions. Change it only by agreement of both
modules, and bump `version` when an existing meaning changes.

## 1. Why it exists

The harness module stores its OpenCode sessions under its own state, either
in a private data folder per session or in one shared OpenCode 2 store.
Astrocyte's OpenCode readers resolve only the user's own OpenCode store, so
without a declaration these sessions would go unbilled. The harness module
also erases a session's folder or shared-store rows soon after the session
ends, so astrocyte must read them first, and the harness module must be able
to tell when it has.

Live step events that a runner publishes are fine for live views, but they are
never the billing record: a missed event would be a missed call. Billing comes
from reading the declared store's `opencode.db`.

## 2. Roots file (written by the harness module)

Path: `<cortexkit data home>/thalamus-harness/opencode-sessions/roots.json`,
where the cortexkit data home is `$XDG_DATA_HOME/cortexkit`, falling back to
`~/.local/share/cortexkit` on Linux and macOS and to the platform data folder
on Windows. The session folders sit inside the same `opencode-sessions/`
folder or at the runner-head paths defined below. The shared store sits at
the shared-server path defined below. The harness module writes the file
atomically (a temporary file, then rename).

```json
{
  "version": 1,
  "roots": [
    {
      "path": "/home/me/.local/share/cortexkit/thalamus-harness/opencode-sessions/s-0123",
      "session_ref": "s-0123",
      "owner_principal": "prefrontal-core",
      "state": "active",
      "done_at_ms": null
    },
    {
      "path": "/home/me/.local/share/cortexkit/thalamus-harness/opencode2-server/data/opencode",
      "kind": "shared",
      "sessions": [
        {
          "session": "ses_0123",
          "session_ref": "s-0456",
          "owner_principal": "prefrontal-core",
          "state": "active",
          "done_at_ms": null
        }
      ]
    }
  ]
}
```

An entry without `kind` declares a folder for one session, with these fields:

- `path`: the folder that directly holds the session's `opencode.db`, that
  is the private OpenCode server's data folder, not the session's whole
  private tree. Astrocyte reads `<path>/opencode.db`. Entries are keyed by
  `path`, and a folder belongs to one session for that session's whole life:
  when the session's server restarts after a module restart or an idle stop,
  it reuses the same folder and the same entry. A new session always gets a
  new folder. The path must be absolute and, after symlinks are resolved,
  either strictly inside the canonical `opencode-sessions/` folder that
  holds `roots.json`, or exactly `<H>/launch-head-<name>/data/opencode`.
  Here `<H>` is the canonical parent of that `opencode-sessions/` folder,
  and `<name>` is one non-empty path component: the `launch-head-` component
  must have at least one character after the prefix. The second shape
  supports long-lived runner heads whose folders OpenCode and other modules
  already reference, so those folders cannot move. Astrocyte refuses any
  other resolved path, logs it once and skips that entry; the other entries
  still read. A symlink does not exempt its target from these rules.
- `session_ref`: the harness module's reference for the session. Astrocyte
  uses it only in log lines in v1.
- `owner_principal`: the module that started the session, as the bare
  module id the daemon attests (for example `prefrontal-core` or
  `magic-context`, never `reserved:prefrontal-core`). Astrocyte records it
  verbatim as the `work_class` of every call from that folder.
- `state`: `active` while the session may still write, including while its
  server is stopped for idleness. It becomes `done` only when the session
  ends for good: it expired, was released or was deleted. `done_at_ms` (Unix
  milliseconds) is null while active. It is the time of the roots-file write
  that first marks the entry `done`, made after the session's last OpenCode
  server has exited. It must not be an earlier recorded time, such as the
  server's exit time: astrocyte only applies the done-folder rules to a folder
  once a read sees it marked `done`, so a `done_at_ms` from before that write
  could fall behind a watermark that never covered the folder as done.

An entry with `kind: "shared"` declares the shared OpenCode 2 store. Its
`path`, after symlinks are resolved, must be exactly
`<H>/opencode2-server/data/opencode`, where `<H>` is the canonical parent of
the `opencode-sessions/` folder that holds `roots.json`. No ancestor,
descendant or similarly named path is accepted. This path is not accepted
without `kind: "shared"`, and a shared entry cannot use a session-folder or
runner-head path. A symlink does not exempt its target from these rules.
Astrocyte logs a refused entry once and skips it; the other entries still read.

A shared entry has a `sessions` array instead of its own `state` and
`done_at_ms`. The store itself is always active. Each session item names
OpenCode's session id in `session` and carries `session_ref`,
`owner_principal`, `state` and `done_at_ms` with the same meanings as a
folder entry. For a shared session, `done_at_ms` is the time of the roots-file
write that first marks the item `done`, after that session can no longer
write; the shared server need not exit. A listed session absent from the
store is allowed because its rows may already have been erased.

Astrocyte reads every session in the shared store, whether listed or not.
A listed session's calls use its `owner_principal` verbatim as `work_class`.
An unlisted session's calls have no work class and use the active-session
quiet window. The harness module writes a session's item before its first
send so that its first calls have the right owner. Missing ownership never
holds back recording a call, and a later item cannot change an already
recorded call.

The roots file remains `version: 1`; runner-head paths and shared entries
extend the accepted declarations without changing existing folder entries.

Unknown fields, at the top level, in an entry or in a session item, are
ignored, so either side can add fields without a coordinated release. In
particular, an extra `work_class` field does not override `owner_principal`.
A changed existing meaning bumps `version`.

### Amendment: copied history in a shared session

A shared-store session item may also carry `imported_through_seq`, a
non-negative integer. It is the copy's highest `session_message.seq` at the
moment the copy was made on the store. This covers any copied session,
including imported or forked copies; it does not depend on how the copy was
made. Sequence values are ordered within a session but may have gaps. For
example, a session item may be:

```json
{
  "session": "ses_copy",
  "session_ref": "s-copy",
  "owner_principal": "prefrontal-core",
  "state": "active",
  "done_at_ms": null,
  "imported_through_seq": 1200
}
```

Astrocyte records only assistant rows in that session whose `seq` is greater
than `imported_through_seq`. Rows at or below the boundary are counted as
skipped copied history, not recorded or set aside. They do not hold the read
cursor, the open-row floor or the read watermark. The watermark's recording
guarantee excludes this deliberately skipped history. Without the field,
every row remains eligible for recording under the existing rules. The field
applies only to shared-store session items, not to per-session folder entries
or the user's plain `opencode-v2` source.

A present value of the wrong type, including null, a negative number or a
non-integer number, makes the session item unreadable. As with other typed
session fields, this makes the roots file malformed: astrocyte keeps the last
good entry list and degrades the source.

An astrocyte version implementing this amendment must be deployed and live
before THALAMUS writes the field; older readers ignore unknown fields and
would bill the copied history. THALAMUS writes the session item, including
the boundary, before the copy accepts any message. A row read before its
session item or boundary is declared remains eligible for billing. A later
declaration does not undo an already recorded call or a call already waiting
for its payer. This preserves the existing behaviour for undeclared sessions.
This amendment keeps roots-file `version: 1`.

| Roots file state | Astrocyte's behaviour |
|---|---|
| Absent | No sessions: no source row, health unaffected. |
| Unknown `version` | Skips the entries and reports the source degraded rather than guessing. |
| Unreadable or malformed | Keeps the last good entry list and reports the source degraded. |

## 3. What astrocyte records

Calls keep producer `opencode`, since OpenCode ran the loop, and their
identity is OpenCode's own message id, exactly as for the user's OpenCode
store. If a message was already recorded, including before it was copied
into a shared store from a head folder, its first record stands: it is not
recorded again or quarantined. Astrocyte reads both OpenCode layouts
(OpenCode 1's `message` table and OpenCode 2's `session_message`) with its
existing readers. It opens each `opencode.db` read-only and closes it after
every read on every operating system, so it never stops the harness module
from erasing a folder.

Reading progress and health use their own source row, `opencode-thalamus`,
apart from the user's OpenCode store. The same key excludes these sessions in
`harness_ingest`.

In an `active` folder or shared session, a call that has tokens but no
completion is recorded after OpenCode 2's one-hour quiet window, as for the
user's store. In a `done` folder or shared session no further write can come,
so such a call is recorded on the next read.

Who paid for each call comes from the harness module's payer store, under the
[payer records contract](thalamus-payer-records-contract.md). The read
watermark below also covers that store's rows.

## 4. Read watermark (served by astrocyte)

Astrocyte provides `log-read-watermark/v1` from the llm-runner contract
(`cortexkit-role-llm-runner` 0.7.1, §9.15), declared in its manifest's
`capabilities.provides`. `watermark.get {}` returns `{watermark_ms: T}`;
a request with any member is refused `invalid_params`:

- T is the start of astrocyte's most recent complete read pass over every
  declared folder and shared store, minus 60 seconds, reported only after
  that pass's recording transaction has committed. There is one watermark
  for the `opencode-thalamus` source, not one per store or session. Every call
  in a folder or shared session whose `done_at_ms` is at or before T has been
  recorded. Every complete pass includes the shared store's unlisted sessions.
- T is stored and never goes backwards, including across restarts.
- A record astrocyte cannot parse counts as read once it has been set aside
  as evidence, so one bad record cannot hold T back.
- A declared folder or shared store that cannot be read (its `opencode.db`
  cannot be opened or queried for any reason other than being gone) is not
  part of a complete pass: it holds T back and reports the source degraded
  until it reads, so a session whose calls were never recorded is never
  released. A store whose `opencode.db` is gone is skipped without degrading
  the source.
- A complete pass runs on every refresher tick (`poll_interval_secs`, 30
  seconds in the shipped configuration). A `done` folder or shared session
  can therefore normally be erased about two to three minutes after its
  `done_at_ms`.
- `{watermark_ms: null}` means no complete pass yet: erase nothing.
- Astrocyte answers only the `thalamus-harness` principal the daemon attested
  when the route was bound. Any other caller, and every caller while
  `opencode-thalamus` is excluded in `harness_ingest` (or `enabled` is
  `false`), is refused with `not_a_read_source`.

## 5. Erasing a finished session (harness module)

Erase a `done` folder only once `watermark.get` returns a T at or past its
`done_at_ms` plus 60 seconds. A null watermark or a refusal means keep
waiting. There is no deletion ceiling: the harness module reports its own
health degraded once any due erase has waited 24 hours past due, so a long
astrocyte outage is visible and never loses billing. Mark the entry `done`
before erasing its folder, and remove the entry once the folder is gone.

For a shared store, mark the session's item `done` once the session can no
longer write. Erase only that session's rows, and only once T is at or past
its `done_at_ms` plus 60 seconds. Remove its session item once its rows are
gone; keep the shared store and its entry for other sessions. The same null
watermark, refusal and overdue-health rules apply. Deleting a session's rows
below astrocyte's read cursor does not mean the store has gone backwards and
does not prevent later calls from being read.

## 6. Not guaranteed

- The OpenCode agent name is not recorded for these sessions in v1;
  `work_class` carries `owner_principal` instead.
- `session_ref` is not stored in v1.
- While `opencode-thalamus` is excluded, nothing is read and every
  `watermark.get` is refused, so the harness module keeps those folders and
  rows until the exclusion is removed or an operator decides otherwise.
