# step-cause-claude-code-v1

A Claude Code `step.finished` body once the gateway records each forwarded request's
cause facts: `view_version`, `tools_digest`, `system_digest` and `signed_floor`, the
same members the OpenCode 2 and Pi bodies in `step-cause-v1` carry. Built by the same
builder as live hints from synthetic values
(`runner::step_events::tests::claude_code_cause_reader_vector_matches_its_exact_body`).
`step-cause-v1/claude-code.json` stays as recorded: the body before these facts.

The two digests describe the plan surface the gateway installs into the request (the
plan's system text and tool definitions), as on OpenCode 2 and Pi. System blocks Claude
Code adds itself, such as its billing line, are outside them. The view is the compaction
view the request was rendered from, and `0` when none applies yet.
