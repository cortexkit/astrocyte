# Pi session folder, version 1 (on-demand Pi worker session)

Recorded from the real Pi 1.6.0 test `real_pi_session_roots_and_payer_rows_survive_reattach`
(Thalamus local main, `a1b50418c` plus a test-only sample writer). Two turns ran against a
stand-in model ("probe"): one, a park and relaunch of Pi, then a second turn on the same
session file. No real conversation, account or credential is included.

- `roots.json`: the committed normalized sample (fixed example path and ids).
- `unedited/roots.json`: the roots file exactly as written, whose `session` equals the
  journal header's `id` and whose `path` is the folder below, as it sat on the build server.
- `unedited/session/session.jsonl`: Pi's journal as Pi left it (two assistant replies with usage).
- `unedited/session/.prompt-handed`: an empty marker Agent Runner keeps in the folder.

`owner_principal` reads `prefrontal-core` because the test's stand-in owner still uses
that name; the live owner is `prefrontal`.
