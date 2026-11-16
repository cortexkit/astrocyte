# Agent Runner unasked-step cause facts

`step.finished.v1` bodies from Agent Runner's publisher for the first unasked
OpenCode 2 and Pi steps of a run, sent 16 Nov 2026. An unasked step went out
without asking the module, so it carries the facts in force for its session:
view version 7, the signed floor and both digests. The bodies are synthetic
(ids, times and the frozen tool list are stand-ins) and carry no account, since
no routing admission was supplied. `SHA256SUMS` hashes each file as received.
