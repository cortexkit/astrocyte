# Step rewrite-cause reader samples, version 1

These are exact `step.finished.v1` bodies emitted by
`cause_reader_vectors_match_each_hosts_exact_body`, followed by a single newline.
Every identity is synthetic. No sample was sent to a bus or another service.
`SHA256SUMS` checks the complete files, including that newline.

The OpenCode 2 sample is the first step after a tools-only plan change: the
`read` definition was removed, leaving an empty tool list. The system text stayed
`Synthetic system`, the last asked request rendered view 7, and the newest signed
floor was `previous-reply`. The tools digest changes across that refresh; the
system digest does not. Pi illustrates the same request facts with its own step
key and usage. Claude Code supplies only the account reference for now: its gateway
is the only boundary that sees every request as sent. View, signed floor and both
digests are omitted until that boundary records them. The gateway adds system
blocks and rewrites tool schemas, so frozen plan digests would not describe its
provider input.

`account_ref` is the contract helper's hash of `synthetic-account` and
`synthetic-credential`, not either raw id. Each sample keeps the existing step
identity, usage and timing fields alongside the newly supplied facts.
