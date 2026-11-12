# BROCA fork spend samples (2026-11-12)

A fork is a side question asked of a running head, such as a voice-call
question. BROCA bills each fork attempt that reaches a model call under the
fork's own run (`fork:<digest>`) and session (`<head session>:fork:<fork id>`),
on the head's credential. These files come from BROCA's real serializers,
with synthetic sessions and accounts:

- `segment.fork.json`: a fork attempt's usage-export fact.
- `segment.ordinary.json`: the same call made as an ordinary run of the head.
  Only `run_id`, `session` and `fact_id` differ from the fork's fact.
- `step-finished.fork.before.json`: the fork attempt's `step.finished.v1`
  event as BROCA publishes it. It carries no fork member.

`SHA256SUMS` hashes each file. The bytes are unchanged from BROCA's sample
set `fork-spend-1112`, whose own checksum list gives the same hashes.
