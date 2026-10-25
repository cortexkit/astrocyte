# THALAMUS OpenCode sessions: roots file and read watermark (v1)

This is the binding contract between the thalamus-harness module, which runs
each OpenCode session it launches in its own private OpenCode server, and
astrocyte, which records the usage of those sessions. Change it only by
agreement of both modules, and bump `version` when a meaning changes.

## 1. Why it exists

Each session the harness module launches gets its own OpenCode server with a
private data folder under the harness module's state. Astrocyte's OpenCode
readers resolve only the user's own OpenCode store, so without a declaration
these sessions would go unbilled. The harness module also erases a session's
folder soon after the session ends, so astrocyte must read it first, and the
harness module must be able to tell when it has.

Live step events that a runner publishes are fine for live views, but they are
never the billing record: a missed event would be a missed call. Billing comes
from reading the folder's `opencode.db`.

## 2. Roots file (written by the harness module)

Path: `<cortexkit data home>/thalamus-harness/opencode-sessions/roots.json`,
where the cortexkit data home is `$XDG_DATA_HOME/cortexkit`, falling back to
`~/.local/share/cortexkit` on Linux and macOS and to the platform data folder
on Windows. The session folders sit inside the same `opencode-sessions/`
folder. The harness module writes the file atomically (a temporary file, then
rename).

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
    }
  ]
}
```

- `path`: the folder that directly holds the session's `opencode.db`, that
  is the private OpenCode server's data folder, not the session's whole
  private tree. Astrocyte reads `<path>/opencode.db`. Entries are keyed by
  `path`, and a folder belongs to one session for that session's whole life:
  when the session's server restarts after a module restart or an idle stop,
  it reuses the same folder and the same entry. A new session always gets a
  new folder. The path must be absolute and, after symlinks are resolved,
  inside the `opencode-sessions/` folder that holds `roots.json`. Astrocyte
  refuses any other path, logs it and skips that entry; the other entries
  still read.
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

Unknown fields, at the top level or in an entry, are ignored, so either side
can add fields without a coordinated release. A changed meaning bumps
`version`.

| Roots file state | Astrocyte's behaviour |
|---|---|
| Absent | No sessions: no source row, health unaffected. |
| Unknown `version` | Skips the entries and reports the source degraded rather than guessing. |
| Unreadable or malformed | Keeps the last good entry list and reports the source degraded. |

## 3. What astrocyte records

Calls keep producer `opencode`, since OpenCode ran the loop, and their
identity is OpenCode's own message id, exactly as for the user's OpenCode
store. Message ids are unique across OpenCode stores, so a session folder can
never collide with the user's store. Astrocyte reads both OpenCode layouts
(OpenCode 1's `message` table and OpenCode 2's `session_message`) with its
existing readers. It opens each `opencode.db` read-only and closes it after
every read on every operating system, so it never stops the harness module
from erasing a folder.

Reading progress and health use their own source row, `opencode-thalamus`,
apart from the user's OpenCode store. The same key excludes these sessions in
`harness_ingest`.

In an `active` folder, a call that has tokens but no completion is recorded
after OpenCode 2's one-hour quiet window, as for the user's store. In a `done`
folder no further write can come, so such a call is recorded on the next read.

## 4. Read watermark (served by astrocyte)

Astrocyte provides `log-read-watermark/v1` from the llm-runner contract
(version 0.7.1, point 5). `watermark.get {}` returns `{watermark_ms: T}`:

- T is the start of astrocyte's most recent complete read pass over every
  declared folder, minus 60 seconds, reported only after that pass's
  recording transaction has committed. Every call in a folder whose
  `done_at_ms` is at or before T has been recorded.
- T is stored and never goes backwards, including across restarts.
- A record astrocyte cannot parse counts as read once it has been set aside
  as evidence, so one bad record cannot hold T back.
- A declared folder that cannot be read (its `opencode.db` cannot be opened
  or queried for any reason other than being gone) is not part of a complete
  pass: it holds T back and reports the source degraded until it reads, so a
  folder whose calls were never recorded is never released. A folder whose
  `opencode.db` is gone is skipped without degrading the source.
- A complete pass runs on every refresher tick (`poll_interval_secs`, 30
  seconds in the shipped configuration). A `done` folder can therefore
  normally be erased about two to three minutes after its `done_at_ms`.
- `{watermark_ms: null}` means no complete pass yet: erase nothing.
- Astrocyte answers only the daemon-attested `thalamus-harness` principal.
  Any other caller, and every caller while `opencode-thalamus` is excluded in
  `harness_ingest`, is refused with `not_a_read_source`.

## 5. Erasing a finished session (harness module)

Erase a `done` folder only once `watermark.get` returns a T at or past its
`done_at_ms` plus 60 seconds. A null watermark or a refusal means keep
waiting. There is no deletion ceiling: the harness module reports its own
health degraded once any due erase has waited 24 hours past due, so a long
astrocyte outage is visible and never loses billing. Mark the entry `done`
before erasing its folder, and remove the entry once the folder is gone.

## 6. Not guaranteed

- The OpenCode agent name is not recorded for these sessions in v1;
  `work_class` carries `owner_principal` instead.
- `session_ref` is not stored in v1.
- While `opencode-thalamus` is excluded, nothing is read and every
  `watermark.get` is refused, so the harness module keeps those folders until
  the exclusion is removed or an operator decides otherwise.
