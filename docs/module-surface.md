# Module surface

This is the contract for the operations `ck-astrocyte` serves and the
operations it calls on peer modules. The source of truth is
`crates/astrocyte-module/src/ops.rs`. See the [data model](data-model.md) for
the tables these operations read and write.

## 1. Transport

astrocyte runs under subc, the subconscious daemon, which supervises local
modules and routes requests between them. It serves one management surface
under module id `astrocyte`. A
client opens a route with target `{"kind": "management_surface", "module_id":
"astrocyte"}` and sends a request body `{"method": <operation>, "params":
<object>}` on that route. `spend_drive` is a client that does exactly this;
see [operations](operations.md).

- A success is a Response frame whose body is `{"result": <reply>}`.
- A refusal is an Error frame whose body is `{"code": <code>, "message":
  <text>}`. `code` is a closed string from this document; `message` is for
  people and is not a stable interface.
- A body that does not decode as `{method, params}` is refused
  `invalid_request`.
- A reply larger than the daemon delivers (4 MiB, counting the 21-byte
  frame header) is replaced by a refusal `frame_too_large` whose `detail` is
  `{"size": <reply bytes>, "limit": <limit>}`, on the request's own route and
  correlation id. Test:
  `oversized_route_reply_is_refused_frame_too_large_on_its_own_correlation`.
- A request whose handler panics is refused `internal`, and the module then
  stops with a non-zero exit, because a panic may have left shared state
  broken. Test: `handler_panic_replies_before_serving_ends_with_nonzero_exit`.
- A route opened by a subc flow (work that runs for its owner without the
  owner present) may call the read operations. Every write
  operation on such a route is refused `flow_scopes_unsupported` before
  dispatch. Test: `a_flow_stamped_route_may_read_but_every_write_is_refused`.

The manifest advertises the operation list and a versioned params schema per
operation (`operation_schemas`, `OPERATION_SCHEMA_VERSION` = 2). Every params
object is closed (`additionalProperties: false`). A field is required exactly
when the request type has no default for it; an optional field also accepts
`null`. Integer bounds are the Rust type's range. Tests:
`operation_surface_is_set_equal_to_the_versioned_artifact`,
`every_operation_schema_agrees_with_its_decoder`.

## 2. Operations

`PUBLIC_OPERATION_NAMES` is the closed list. An unknown method is refused
`unknown_method`. Writes are serialized through one writer; reads run on a
read-only connection in one snapshot and neither wait for nor block ingest.
Test: `every_operation_name_reaches_its_own_handler`.

| Operation | Kind | Request fields (required unless marked `?`) |
|---|---|---|
| `budget.verdict` | read | `project_root?`, `provider?`, `model?`, `est_cost_nanos?` |
| `spend.report` | read | `grouping` (string array), `grain`, `timezone` |
| `spend.subscription.report` | read | none |
| `spend.workclass.report` | read | none |
| `spend.ledger.coverage` | read | none |
| `spend.charge.explain` | read | `event_id` |
| `spend.reconcile` | read | `statement_id` |
| `spend.multiplier.report` | read | `from_ms`, `to_ms` |
| `spend.multiplier.pace` | read | `quota_provider`, `from_ms`, `to_ms` |
| `usage.cache.sessions` | read | `since_ms?`, `harness?`, `limit?` |
| `usage.cache.steps` | read | `harness`, `session`, `limit?`, `after_cursor?` |
| `usage.steps.subscribe` | read, watch | none |
| `usage.steps.read` | read | `after?`, `limit?` |
| `spend.event.ingest` | write | see §3.1 |
| `spend.correction.ingest` | write | see §3.2 |
| `spend.chargeability.settle` | write | `event_id`, `to_state`, `reason` |
| `spend.statement.import` | write | `path`, `provider`, `account_ref`, `period_start`, `period_end`, `amount_exponent`, `currency` |
| `spend.subscription.set` | write | `account_ref`, `provider`, `plan_name`, `recurring_cost_minor`, `currency`, `amount_exponent`, `period`, `effective_from` |
| `spend.subscription.close` | write | `account_ref`, `provider`, `effective_to` |
| `spend.event.reprice` | write | `event_id` |
| `spend.basis.correct` | write | `event_id`, `corrected_basis`, `reason` |
| `spend.multiplier.recompute` | write | `observation_day`, `quota_provider?` |

Times are Unix milliseconds. Money is signed integer nanounits of the named
currency (`*_nanos`); `*_minor` amounts are minor units scaled by
`amount_exponent` (0 to 9).

## 3. Write operations

Every request that fails to decode (a missing or unknown field, a wrong type,
a value outside an enum) is refused `malformed`. Store failures are
`internal`. A refusal writes nothing. Test:
`malformed_operation_is_a_refusal_without_a_write`.

### 3.1 `spend.event.ingest`

Records one usage event pushed by a peer. Required: `producer`,
`producer_ref`, `charge_basis` (`metered_api`, `subscription_included` or
`unknown`). Optional strings: `producer_lineage`, `prior_event_ref`,
`harness`, `project_root`, `session`, `run_ref`, `work_class`,
`terminal_status`, `vendor_request_id`, `provider`, `model`, `account_ref`,
`auth_selection`. Optional integers: `occurred_at_ms`, `emitted_at_ms`,
`attempt_ordinal`. `measurements` defaults to empty; each item has `role`,
`unit`, `kind`, `source` and optional `quantity_units`, `quantity_exponent`.

Reply: `event_id`, `disposition`, `transitions_written`,
`quarantine_evidence_written`, `waiting_for_target`. `disposition` is one of:

| Disposition | Meaning |
|---|---|
| `recorded` | A new event; its measurements are queued for pricing. |
| `exact_redelivery` | The identity exists with the same payload. Nothing changes except one `redelivery_log` row. |
| `quarantined_redelivery` | The identity exists with a different payload. The stored event is kept; the incoming payload goes to `rejected_delivery`. |

Refusal `malformed`: empty `producer` or `producer_ref`, an empty
`prior_event_ref`, a measurement with an empty field, a quantity with only one
of its two parts, negative units, an exponent outside -9..0, or two
measurements with the same `(role, unit)`. Tests:
`exact_redelivery_is_an_order_independent_content_noop_and_logs_occurrence`,
`same_identity_different_payload_is_quarantined_without_overwrite`.

### 3.2 `spend.correction.ingest`

Appends a signed money adjustment to an event. Required: `correction_id` (1 to
128 UTF-8 bytes), `event_id`, `currency`, `amount_nanos`, `reason`
(`write_off`, `vendor_restatement`, `our_repricing`, `refund`, `goodwill`),
`occurred_at_ms` (at least 0). Optional: `note`. Reply: `adjustment_id`,
`origin` (`correction_ingest`). The retry rules are in
[money ingest](money-ingest.md).

| Code | Condition |
|---|---|
| `malformed` | An empty or oversized `correction_id`, an empty `event_id` or `currency`, or a negative time. |
| `correction_id_reused` | The key exists with different content. |
| `not_found` | No event has `event_id`. |
| `undated_target` | The event has no `occurred_at_ms`. |
| `unpriced_target` | The event has no currency yet: it has never been priced. |
| `currency_mismatch` | `currency` differs from the event's. |
| `write_off_requires_pricing` | `reason` is `write_off`; write-offs are not accepted yet. |

Tests: `correction_ingest_requires_key_and_replays_without_moving_money`,
`correction_key_conflicting_content_is_refused_without_moving_money`.

### 3.3 `spend.chargeability.settle`

Settles whether an event is chargeable, once. `to_state` is `billed`,
`written_off` or `unbillable`. Reply: `event_id`, `to_state`.

| Code | Condition |
|---|---|
| `malformed` | An empty `event_id` or `reason`. |
| `operator_cannot_bill` | `to_state` is `billed`; only a later record or a statement can bill. |
| `not_found` | No event has `event_id`. |
| `already_settled` | The event already has a transition. |
| `unpriced_target` | The event has no currency. |

Tests: `operator_cannot_mark_candidate_billed_without_effect`,
`settlement_is_terminal_and_current_state_comes_from_history`.

### 3.4 `spend.statement.import`

Imports a vendor statement CSV from `path` and matches its lines against the
ledger. The file is read before the write transaction. Reply: `statement_id`,
`import_id`, `content_outcome` (`created` or `no_op`), `lines_created`,
`lines_skipped`, `matching`.

| Code | Condition |
|---|---|
| `invalid_request` | `path` is not a regular file, or is over 64 MiB. |
| `statement_source` | The file cannot be read. |
| `malformed` | A line or field does not parse. |
| `invalid_period` | `period_start` is not before `period_end`. |
| `line_outside_period` | A line falls outside the declared period. |
| `statement_currency_mismatch` | A line's currency differs from `currency`. |
| `conflicting_reimport` | A line already imported has different content. |
| `amount_out_of_range` | An amount cannot be represented in nanounits. |
| `settlement_act_refused` | A settlement the import makes is refused. |

Tests: `statement_import_refuses_a_directory_without_importing`,
`statement_import_refuses_files_over_64_mib_without_importing`,
`statement_import_refuses_fifo_without_waiting_for_a_writer`.

### 3.5 `spend.subscription.set` and `spend.subscription.close`

`spend.subscription.set` declares a plan for `(provider, account_ref)` from `effective_from`,
bounding any open plan for that key at the same instant. `period` is `month`
or `year`; `currency` is three uppercase letters. Its reply is the plan's
fields plus `backdated` (`effective_from` is before the call) and
`coverage_notice` (the one-way notice text when backdated, else `null`).

`spend.subscription.close` bounds the open plan at `effective_to`. Its reply is `subscription`,
`coverage_is_one_way` (always `true`) and `coverage_notice`.

| Code | Operation | Condition |
|---|---|---|
| `malformed` | both | An empty field, a negative cost or time, `amount_exponent` above 9, or a non-canonical currency. |
| `recurring_cost_out_of_range` | set | The cost cannot be represented in nanounits. |
| `overlapping_effective_range` | set | An open plan starts at or after `effective_from`, or a closed plan ends after it. |
| `no_open_subscription` | close | No open plan exists for the key. |
| `close_before_effective_from` | close | `effective_to` is at or before the open plan's start. |

Coverage is one-way: closing or superseding a plan never uncovers an event the
plan already priced as covered. See [operations](operations.md#2-subscriptions). Tests:
`subscription_set_reply_carries_the_one_way_notice_only_when_backdated`,
`storage_update_and_delete_guards_preserve_subscription_history`.

### 3.6 `spend.event.reprice` and `spend.basis.correct`

`spend.event.reprice` re-resolves an event's measurements whose current head is unpriced
or missing, and keeps priced and covered heads. `spend.basis.correct` changes the
event's charge basis to `corrected_basis` and supersedes every head. Both
resolve against the catalog (or the subscription in force) before the write
transaction, then re-check that nothing moved. Reply: `event_id`,
`pricing_generation`, `measurements_repriced`, `measurements_preserved`.

| Code | Condition |
|---|---|
| `malformed` | An empty `event_id`, or (correction) a blank `reason`. |
| `not_found` | No event has `event_id`. |
| `basis_unchanged` | (correction) `corrected_basis` equals the current basis. |
| `no_measurements_to_price` | The event has no measurements. |
| `already_priced` | (reprice) Every measurement already has a priced or covered head. |
| `pricing_superseded` | Another pricing act committed between resolve and commit. |
| `subscription_coverage_superseded` | The subscription in force changed between resolve and commit. |
| `catalog_currency_mismatch` | A resolved price's currency differs from the event's currency or from another measurement's. |

Tests: `op_event_reprice_routes_subscription_included_through_coverage`,
`basis_correction_op_refusals_write_no_charges`,
`basis_correction_op_reaches_all_three_arms`,
`missing_reprice_event_preserves_not_found_and_writes_nothing`.

### 3.7 `spend.multiplier.recompute`

Recomputes one UTC day (`observation_day`, `YYYY-MM-DD`) for one configured
quota provider, or for all of them when `quota_provider` is absent, over the
seven days ending at that day's 00:00Z. Each row supersedes the day's head, or
is the day's original when none exists. All rows append in one transaction or
none does. Reply: `{"recomputed": [{quota_provider, observation_day,
observation_id, supersedes, multiplier, fee_multiplier}]}`.

| Code | Condition |
|---|---|
| `malformed` | `observation_day` is not a date. |
| `multiplier_not_recorded` | The committed configuration has no `subscription_multiplier` member. |
| `unknown_quota_provider` | The named provider is not configured, or none is. |
| `scoped_producer_not_ingested` | A requested provider's scope names producers not ingested by the configuration. The message names them; all requested providers are checked before any call or write. |
| `catalog_unavailable` | No fusiform catalog is connected, or a catalog call failed. |
| `plan_prices_unavailable` | Plan prices were needed and could not be read. |
| `rate_fact_unrecognized` | The catalog served a rate this build cannot read. |
| `source_reads_pending` | A missing original's sources have not all been read after the day's boundary, and two hours have not passed. |
| `arithmetic_out_of_range` | An exact computation overflowed. |

Tests: `a_recompute_supersedes_the_day_and_the_report_serves_it`,
`recompute_creates_a_missing_day_without_supersedes_and_refuses_unknown_provider`,
`served_recompute_refuses_excluded_scope_without_calls_or_rows`.

## 4. Read operations

Reads never write. A refusal on a read is one of the codes below.

- `budget.verdict`: `decision` (always `allow`), `consumed_nanos` (current
  heads plus adjustments, by currency, filtered by the optional fields),
  `est_cost_nanos` (echoed), `limit_nanos` and `remaining_nanos` (always
  `null`), `uncertainty` (`unpriced_events`, `events_total`),
  `uncertainty_scope` (`machine`). Test:
  `only_the_charge_nothing_supersedes_counts_toward_verdict_and_total`.
- `spend.report`: `totals`, `rows`, `undated`, `ingest_freshness`,
  `rate_staleness`, and `hierarchy_resolution` (`unavailable`) when grouped by
  `project`. `grouping` names are `provider`, `model`, `account`,
  `auth_selection`, `charge_basis`, `work_class`, `project`; `grain` is
  `day`, `week` or `month`; `timezone` is an IANA name. Refusals:
  `invalid_grouping` (unknown or repeated), `invalid_grain`,
  `invalid_timezone`. Test: `spend_report_refusal_keeps_its_code`.
- `spend.subscription.report`: `rows`, `coverage`, `ingest_freshness`.
- `spend.workclass.report`: `rows`, `producers`.
- `spend.ledger.coverage`: `set_aside_by_source`, `events_total`,
  `events_unpriced`, `events_undated`, `zero_measurement_events`,
  `unpriced_events_by_reason`, `unpriced_measurements_by_reason`,
  `ingest_freshness`, `rate_staleness`, `capacity`. Test:
  `open_coverage_report_does_not_stall_an_ingest_commit`.
- `spend.charge.explain`: `event`, `measurements`, `adjustments`,
  `transition`. Each measurement supplies `role` and `unit`; its charges are
  identified by `pricing_generation` within that measurement. There is no
  `charge_id` field. `supersedes` is the predecessor's pricing generation (null
  for an original), not an integer storage key. Event IDs are unchanged and no
  local keys are served. Refusals: `malformed` (empty `event_id`), `not_found`.
- `spend.reconcile`: `statement`, `lines`, `summary`. Refusals: `malformed`,
  `statement_not_found`; a store failure is `store_error`.
- `spend.multiplier.report`: `latest` (each provider's newest head) and
  `series` (heads by day in range). Refusal: `invalid_range` (`from_ms`
  negative or not before `to_ms`). Test:
  `multiplier_report_refuses_a_malformed_range`.
- `spend.multiplier.pace`: hourly usage by producer and work class, valued at
  the latest head's recorded rates without a catalog call. Refusals:
  `unknown_quota_provider`, `invalid_range`, `range_too_large` (over 192
  aligned hours), `no_multiplier_observation`. Test:
  `pace_refuses_unknown_provider_invalid_or_excessive_range_and_missing_head`.

Absent values are `null` or omitted, never zero. A total that cannot be
priced is reported with its unpriced population, not as a smaller number.

### 4.1 Cache diagnostics

These two operations serve harness steps and BROCA's diagnostic model steps,
per session, with cache verdicts. A harness is a producer whose events are one
model step each: `claude-code`, `codex`, `gemini`, `kimi`, `omp`, `opencode` or
`pi`, plus `broca` for its separate diagnostic facts. BROCA uses the binding's
session key, as its session spend does; the host harness stays in the stored
binding. A request that fails to decode is refused `malformed`, as everywhere
else. A request that decodes is refused with one of these codes:

| Code | Condition |
|---|---|
| `invalid_request` | `limit` is 0, `session` is empty, or `since_ms` is negative. |
| `unknown_harness` | `harness` is not one of the harnesses above. |
| `range_too_large` | `limit` is above its maximum, or `since_ms` is more than 7 days before now. |

`usage.cache.steps` returns up to `limit` steps of one session (default 200,
maximum 1,000): the newest ones, oldest first. With `after_cursor`, only the
steps of that window whose cursor is greater are returned. The reply has
`harness`, `session`, `steps`, `earlier_steps` (whether the session has older
steps than the window) and `undated_steps` (steps without a time, which are
not placed on the timeline). Each step has these fields:

- `cursor`, the durable shared cache-step cursor. It increases across harness
  events and BROCA steps, survives restarts and is never reused by retention.
  A late step can have a greater cursor than a step that happened after it.
- `step_ref`, the harness's own id for the step, and `occurred_at_ms`
  (BROCA's required `finished_at_ms`). BROCA steps read from BROCA's export
  facts use `run_id#step_id`; that diagnostics table is being retired, and the
  reader of BROCA's durable step log will use the per-attempt form
  `run_id#step_id#attempt` described in section 4.2.
- `input`, `cache_read`, `cache_write`, `output` and `reasoning`, each omitted
  when the reader did not store it. `cache_write` sums every cache lifetime
  Claude Code reports.
- `output` includes reasoning, the fleet convention: `reasoning` is a
  breakdown inside `output`. The ledger stores the two disjoint, and a served
  row adds them: `output` is the stored output plus the stored reasoning when
  both are present, the stored output when reasoning is absent, and omitted
  when no output was stored. A count is never made up from reasoning alone.
  Test: `served_output_includes_reasoning_on_both_reads`.
- `provider`, `model`, `finish` and `context_limit`. `finish` comes from
  OpenCode's `finish`, Pi's `stopReason`, or the stored stop reason of Claude
  Code and Oh My Pi, or BROCA's `finish_reason`. `context_limit` comes from
  Codex or BROCA.
- `request_body_len` is present when BROCA reported it. On a prompt drop,
  `request_size` is present when both adjacent steps report body lengths:
  `smaller` when the current body is shorter, otherwise `not_smaller`.
- `prompt`, which is input plus cache read plus cache write.
- `retention`, `verdict` and `prompt_dropped`. `retention` is served
  unrounded and unclamped: a non-negative number that may exceed 1 when a step
  reads more cache than expected.

The verdict is one of `cold_start`, `stable`, `warning`, `bust`, `full_bust`
and `unknown`. It is computed over the whole session in time order, so a
step's verdict does not depend on the page size. The rules are the Magic
Context dashboard's:

- A step without a stored cache read is `unknown`. It is also never the
  baseline the next step is compared with.
- In a session where no step ever read a positive number of cached tokens,
  every step is `unknown`. Codex is the exception.
- The session's first step is `cold_start`.
- Otherwise, the step is compared with the last earlier step that reported its
  cache. When there is none, or that step read zero, the verdict is `stable`
  with no `retention`. Otherwise, `expected` is that step's cache read plus its
  growth, which is its cache write when positive and otherwise its uncached
  input. `retention` is this step's cache read divided by `expected`.
- Codex reports no cache writes, so its `retention` is the cache read divided
  by the cache read plus the uncached input. When that sum is zero, the
  verdict is `unknown`.
- A cache read of zero is `full_bust`. Otherwise, a `retention` of at least
  0.95 is `stable`, at least 0.80 is `warning`, and anything lower is `bust`.
- `prompt_dropped` is true when `prompt` is at least 15,000 tokens smaller than
  that earlier step's prompt.

The serving side keeps the newest 1,000 steps of up to 200 sessions active in
the last 24 hours in memory. It loads them at startup and adds each commit's
new steps after the commit. A request it can answer exactly is answered from
memory, and any other request is answered from the store. Both build the reply
the same way. Tests: `drained_pi_steps_are_served_alike_from_memory_and_store`,
`store_steps_are_the_same_whatever_the_window`,
`prefix_cache_verdicts_cover_every_band_and_edge`.

The memory window is distinct from storage retention. BROCA diagnostics are
kept for 30 days after the step finished, with at most 500 expired rows deleted
per maintenance transaction. Harness ledger steps are kept indefinitely.

`usage.cache.sessions` returns `since_ms` (default: 24 hours ago) and
`sessions`, the harness sessions with a step at or after `since_ms`, most
recently active first. There are at most `limit` sessions (default 50, maximum
200), optionally for one `harness`. Each session has these fields:

- `harness` and `session`.
- `project_root`, from the newest step in the window that has one.
- `steps`, the number of steps in the window.
- `hit_ratio`, which is the cache reads divided by the sum of input, cache
  read and cache write. Only steps that report their cache count, and the
  field is omitted when there are none.
- `busts`, the number of steps whose verdict is `bust` or `full_bust`.
- `last_activity_ms`.
- `context_limit`, the newest positive context limit in the window.

It is always read from the store. Test:
`sessions_report_hit_ratio_busts_limit_and_activity`.

### 4.2 Live step feed (contract v1)

Apps follow cache diagnostics live through two operations: a subscription
that only says new steps exist, and a paged read that returns them. Apps talk
only to astrocyte. Astrocyte combines the steps it reads from harness files
with the `step.finished.v1` events agent runners publish on the bus. A step a
runner announces is in the feed within seconds; billing still comes only from
the durable reads.

**Who may call.** The daemon-attested principal at route bind decides:
`Principal::Direct` (local apps on the same computer) and `reserved:callosum`
with a phone-class device label (`fed-phone:<fingerprint>`). Every other
principal is refused `not_permitted`, simulators (`fed-sim:`) and other
computers (`fed-computer:`) included.

**Cursors** are opaque strings. They are ordered by astrocyte's commits, are
stable across restarts and are never reused. Apps pass them back and never
parse or compare them.

**Order rule for apps.** Subscribe first and hold the notices, read history
with `usage.steps.read`, then read again from the read's `through`. After a
reconnect, read once more a few seconds later: a step that has just finished
can take a few seconds to be committed.

#### `usage.steps.subscribe`

Request: `{}`. Any member is refused `malformed`.

Each notice is one StreamData frame:

```json
{"through": "c1824004"}
```

`through` is the newest committed cursor. Notices are coalesced: at most four
a second, and a notice may cover many steps or none that the caller may see.

| How it ends | Meaning |
|---|---|
| StreamEnd | Astrocyte is stopping or the route closed. Subscribe again. |
| Error `lagged`, detail `{"through": "<last notified cursor>"}` | The subscriber fell behind its queue bound. Subscribe again and read from the last cursor it holds. |
| Caller's Cancel | Ended by the caller; no further frame. |

Opening refusals: `not_permitted`, `subscription_capacity` (more than 16 open
subscriptions), `draining` (astrocyte is stopping), `malformed`.

#### `usage.steps.read`

Request: `{"after": "<cursor>", "limit": 200}`. Both members are optional.
Without `after`, the read starts at the oldest row the feed still holds. The
feed holds the rows committed in the last 24 hours. `limit` defaults to 200,
with a maximum of 1,000.

Reply:

```json
{
  "rows": [
    {
      "cursor": "c1824003",
      "harness": "opencode",
      "source": "file",
      "step_ref": "msg_0a1b2c3d4e5f",
      "session": "ses_0a1b2c3d",
      "occurred_at_ms": 1792880000000,
      "provider": "anthropic",
      "model": "claude-sonnet-5",
      "input": 1200,
      "cache_read": 182000,
      "cache_write": 900,
      "output": 640,
      "reasoning": 120,
      "finish": "tool-calls",
      "prompt": 184100,
      "retention": 0.9912,
      "verdict": "stable",
      "prompt_dropped": false
    }
  ],
  "next": null,
  "through": "c1824004"
}
```

- `rows` are oldest first. `next` is the cursor to pass as `after` for the
  following page, or null when the reply reached `through`. `through` is the
  newest committed cursor.
- A row's identity is `harness` plus `step_ref`. When any field of a step
  changes (a runner-announced step recorded from its log, a finish reason that
  arrives late, or a verdict recomputed because an earlier step of its session
  arrived late), the whole row is sent again at a new cursor. The last row for
  an identity wins: apps replace, never append.

A row from a runner's `step.finished.v1` event takes its identity from the
event. When the event carries `harness_step_ref` (runner contract 14.3,
present for runners whose history is the host harness's own record),
`harness` is the event's `harness` (the hosting program, such as
`claude-code` or `opencode`) and `step_ref` is `harness_step_ref`, so
astrocyte's later read of the harness store replaces the row. Otherwise
`harness` is the event's `runner` and `step_ref` is
`<run_id>#<step_id>#<attempt>`, one row per provider request actually sent,
retries included, matching astrocyte's read of that runner's durable step
log. A step with no durable record stays a live-only row with
`source: "runner"`.

- `source` is `file` for a step astrocyte read from a harness store or a
  runner's durable log, and `runner` for a step known so far only from a
  runner's bus event.
- `harness` is the producer: `claude-code`, `codex`, `gemini`, `kimi`, `omp`,
  `opencode`, `pi`, or the runner's module id (`broca`, `thalamus-harness`,
  `thalamus`).
- Required: `cursor`, `harness`, `source`, `step_ref`, `session`,
  `occurred_at_ms`, `prompt`, `verdict`, `prompt_dropped`. Steps without a time
  are not in the feed.
- Optional, omitted when unknown and never zero for unknown: `run_id` (runner
  steps), `provider`, `model`, `input`, `cache_read`, `cache_write`,
  `cache_write_5m`, `cache_write_1h`, `output`, `reasoning`, `finish`,
  `context_limit`, `request_body_len`, `retention`, `request_size`.
- `output` includes `reasoning`, which is a breakdown inside it, as in
  section 4.1, and `cache_write` is the sum of every cache lifetime. `cache_write_5m` and
  `cache_write_1h` are present only when the source split them.
- `prompt`, `retention`, `verdict` (`cold_start`, `stable`, `warning`, `bust`,
  `full_bust`, `unknown`), `prompt_dropped` and `request_size` (`smaller`,
  `not_smaller`) follow the rules in section 4.1 exactly. They are computed
  over the step's whole session in time order. The thresholds 0.95 and 0.80
  and the 15,000-token prompt drop are part of this contract's version.
- Rows never carry prompt text, tool names or error text.

Refusals: `malformed` (undecodable, or an unknown member), `invalid_request`
(`limit` is 0, or `after` is not a cursor astrocyte issued), `range_too_large`
(`limit` above 1,000), `cursor_expired` (`after` is older than the feed holds;
detail `{"through": "<newest cursor>"}`: reload per-session history with
`usage.cache.sessions` and `usage.cache.steps`, then read from `through`), and
`not_permitted`.

#### How astrocyte serves the feed

- The feed is the `step_feed` table (see the [data model](data-model.md)).
  After each commit that adds steps or changes a step's stored detail,
  astrocyte recomputes the served rows of each affected session, from the
  earliest changed step onward, and appends every row that differs from the
  newest row stored for its `harness` and `step_ref`. That append is its own
  transaction, run after the commit that recorded the steps; subscribers are
  told only once it has committed. A cursor is `c` followed by the row's
  feed position.
- A step that happened more than 24 hours before it is committed (a first
  read of old history) is not added to the feed; `usage.cache.steps` serves
  it. Rows become eligible for deletion 24 hours after they were committed;
  each feed append deletes up to 1,000 of them, so a cursor stays readable
  until the deletion reaches it.
- In this version every row has `source: "file"`. Runner rows join once
  astrocyte consumes the `step.finished.v1` events runners publish on the
  bus. `cache_write_5m`, `cache_write_1h`
  and `run_id` are not sent yet.
- The phone's device label is the route's bind identity `harness`
  (`fed-phone:<fingerprint>`), as Callosum stamps it; the principal must be
  `reserved:callosum` as well.
- A subscriber's queue holds 64 cursors. A subscriber whose notices are held
  up for longer than that ends `lagged`.
- The serve helper always answers a cancelled request; a cancelled
  subscription's last frame is the error `cancelled`, which the daemon may
  drop because the caller asked to stop.

Tests: `a_notice_is_sent_only_after_its_steps_commit`,
`reads_page_through_the_feed_in_commit_order`,
`a_verdict_changed_by_a_late_earlier_step_is_sent_again`,
`a_read_from_beyond_the_window_is_refused_cursor_expired`,
`the_feed_is_refused_to_simulators_computers_and_other_modules`,
`a_subscriber_that_falls_behind_its_queue_ends_lagged`,
`a_drain_ends_every_step_subscription_before_goodbye`,
`two_subscribers_both_receive_a_notice`.

## 5. Calls astrocyte makes

| Peer | Route | Calls | On failure |
|---|---|---|---|
| fusiform (price catalog) | `tool_provider`, module `fusiform` | `catalog.status`, `catalog.get`, `plan.prices` as `{"name", "arguments"}` tool calls | A failed pricing call writes no head; the event stays queued and a later pass retries it. Recompute refuses. Route death and `module_reloading` are retried once within the call's deadline. |
| insula (the quota module, QTA) | `management_surface`, module `insula` | `usage.get` with empty params; `events_get` with `event_key` and `event_name: "quota_sampled"` | Invalid or missing event bodies owe a snapshot. A failed snapshot records the attempt and keeps the last windows. Capacity is advisory and never blocks spend. |
| ckbus | Attested `management_surface`, module `ckbus` | `ckbus.credential`, `ckbus.credential_renew`, `ckbus.nonce_sign` | Retain per-tick insula polling and retry with exponential backoff (250 ms to 30 s). Never fail the module or change broker topology. |

Fusiform and insula use the connection files named in the
[configuration](configuration.md). The ckbus route uses the module's daemon
connection file and the daemon-validated `consumer_identity` shared with HELLO,
not the un-attested peer clients. With no fusiform connection, events are
stored unpriced with reason `catalog_unconfigured`; with no QTA connection,
capacity is `disabled`.

Insula notices use `ck.{bus_account}.event.insula.quota_sampled.v1`, with no body.
Astrocyte binds its pre-existing pull durable from `cortexkit-bus-naming`'s consumer
table, fetches in batches of 32 (one-second expiry), and hashes the exact compact
`result.body` bytes against both reply and notice digests. It checks the body's
provider, optional account, epoch and sequence against the notice. In-sequence
readings update only that account (or unlabeled provider-current readings); they
have no complete-provider retirement authority. First notices, new epochs, gaps,
missing bodies or failed validation record a snapshot obligation before ack.
Duplicates do not write again. Progress acks protect pending writes; final acks
follow commit or recorded recovery. A restart begins with a snapshot owed.

Healthy attachment, a current credential and a recent successful pull suppress
per-tick snapshots. The backstop is the earlier of ten minutes and the existing
capacity freshness horizon minus one cadence: normally 90 seconds. Event commits
refresh capacity sooner; empty pulls never do. Any bus failure restores per-tick
polling. Transitions are logged; health reply shapes, freshness rules, the
multiplier and `budget.verdict` are unchanged.

broca, the coding harnesses and thalamus-harness are not called. astrocyte
reads their stores and files directly; see [harness readers](harness-readers.md)
and the [Claude workers contract](claude-workers-contract.md).

## 6. Not guaranteed

- `budget.verdict` enforces no cap. Callers must not read `allow` as approval
  of a budget.
- A timed-out write has an unknown outcome. Clients must not retry a write
  blindly; `spend.correction.ingest` is the one write with a retry key.
- Served writes carry no operator identity. The caller is whoever subc
  admitted to the route.
