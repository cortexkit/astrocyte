# Module surface

This is the contract for the operations `ck-astrocyte` serves and the
operations it calls on peer modules. The source of truth is
`crates/astrocyte-module/src/ops.rs`. See the [data model](data-model.md) for
the tables these operations read and write.

## 1. Transport

astrocyte runs under subc, the subconscious daemon, which supervises local
modules and routes requests between them. It displays as "Spend Ledger" and
serves one management surface under module id `astrocyte`. A
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
- A request/reply read under `spend.*` or `usage.*`, or a call to
  `budget.verdict` or `watermark.get`, refuses `reply_too_large` if its reply
  would exceed the daemon's frame limit. The Error's `detail` carries
  `reply_bytes` and `limit_bytes` for the encoded body (including the `result`
  wrapper on success, excluding the frame header), plus `rows` when the result
  has a `rows` array. The body limit is 4,194,283 bytes. The caller must narrow
  the request; the watch subscriptions are not subject to this read check.
  Test: `read_reply_uses_exact_body_size_and_keeps_refusal_code`.
- A reply larger than the daemon delivers (4 MiB, counting the 21-byte
  frame header) that reaches the transport is replaced by `frame_too_large` whose `detail` is
  `{"size": <reply bytes>, "limit": <limit>}`, on the request's own route and
  correlation id. Test:
  `oversized_route_reply_is_refused_frame_too_large_on_its_own_correlation`.
- A request whose handler panics is refused `internal`, and the module then
  stops with a non-zero exit, because a panic may have left shared state
  broken. Test: `handler_panic_replies_before_serving_ends_with_nonzero_exit`.
- A route opened by an approved subc flow (work that runs for its owner without
  the owner present) may call exactly these reads: `budget.verdict`,
  `spend.report`, `spend.subscription.report`, `spend.workclass.report`,
  `spend.ledger.coverage`, `spend.charge.explain`, `spend.reconcile`,
  `spend.multiplier.report`, `spend.multiplier.pace`, `usage.cache.sessions`,
  `usage.cache.steps`, `usage.runs.list` and `usage.runs.detail`. The manifest
  declares `flow-scopes/v1` and names these in `flow_operations`; the same list
  controls dispatch. Run reads retain their principal checks on flow routes.
- Every other operation on a flow route is refused `flow_scopes_unsupported`
  before dispatch, including every write and the principal-checked reads
  `usage.steps.subscribe`, `usage.steps.read`, `usage.runs.subscribe` and `watermark.get`. Without a
  flow, the step feed admits only local apps and Callosum phones and desktops, and
  `watermark.get` admits only `thalamus-harness`. Tests:
  `manifest_provides_flow_scopes_and_exact_read_operations`,
  `a_flow_stamped_subscription_report_is_advertised_and_served`,
  `a_flow_stamped_route_may_call_only_declared_operations`,
  `a_flow_stamped_feed_read_is_refused_before_dispatch`,
  `a_flow_stamped_watermark_read_is_refused_before_dispatch` and
  `a_flow_stamped_runs_read_keeps_principal_checks`.

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
| `spend.report` | read | `grouping` (string array), `grain`, `timezone`; optional paired `from_ms`/`to_ms`, `where` |
| `spend.subscription.report` | read | none |
| `spend.workclass.report` | read | none |
| `spend.ledger.coverage` | read | none |
| `spend.charge.explain` | read | `event_id` |
| `spend.reconcile` | read | `statement_id` |
| `spend.multiplier.report` | read | `from_ms`, `to_ms` |
| `spend.multiplier.pace` | read | `quota_provider`, `from_ms`, `to_ms`, `plan?`, `pool?` |
| `usage.cache.sessions` | read | `since_ms?`, `harness?`, `limit?` |
| `usage.cache.steps` | read | `harness`, `session`, `limit?`, `after_cursor?` |
| `usage.runs.list` | read | `from_ms`, `to_ms`, `harness?`, `limit?`, `after?` |
| `usage.runs.detail` | read | `key`, `limit?`, `after?` |
| `usage.runs.subscribe` | read, watch | none |
| `spend.quota.history` | read | `from_ms`, `to_ms`, `grain`, `quota_providers?` |
| `usage.steps.subscribe` | read, watch | `harness?`, `session?` |
| `usage.steps.read` | read | `after?`, `limit?`, `harness?`, `session?` |
| `watermark.get` | read | none |
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

All nine writes above share a daemon operator-confirmation gate, declared in the
manifest's `confirm_operations`. The gate is **off until release**, so that a
daemon unable to prompt never blocks development:
`ledger_write_confirmation_required()` returns `false`. Release enables
it by changing only that function to return `true`, with no config, environment
or feature switch. While off, direct callers write without a prompt as before.

When enabled, only a `Principal::Direct` caller (a local program) must obtain
daemon confirmation for each in-flight write. The deployed daemon chooses its
OS presence provider. Module-route calls and the refresher never prompt; flow
routes keep their existing `flow_scopes_unsupported` refusal. The prompt names
the operation and target, and waits outside the lock that serializes ledger
writes and outside the store transaction. Callers
must allow at least 300 seconds for the person's response. A confirmation is
never cached or retried: any refusal or failure returns
`operator_confirmation_refused` without dispatching or changing the store. The
message includes the daemon's reason, not request secrets. Any confirmation
error refuses the write, on every platform. This includes direct Linux writes
when the deployed daemon cannot confirm the person, for example before its
polkit provider is available; there is no platform bypass.

`spend_drive` allows 300 seconds for these writes, even while the gate is off;
its reads and authentication retain their 15-second deadlines.

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
ledger. The import folder is `statements/` beside astrocyte's `store.db`; create
it before importing. A relative `path` resolves inside that folder. The folder
and the requested file are each fully resolved, including all symlinks, and the
resolved file must remain inside the resolved folder. The file is read before
the write transaction. Reply: `statement_id`, `import_id`, `content_outcome`
(`created` or `no_op`), `lines_created`, `lines_skipped`, `matching`.

| Code | Condition |
|---|---|
| `invalid_request` | `path` is not a regular file, or is over 64 MiB. |
| `statement_outside_import_folder` | The resolved file is outside the resolved import folder. |
| `statement_source` | The import folder or file is missing, or the file cannot be read. |
| `malformed` | A line or field does not parse. |
| `invalid_period` | `period_start` is not before `period_end`. |
| `line_outside_period` | A line falls outside the declared period. |
| `statement_currency_mismatch` | A line's currency differs from `currency`. |
| `conflicting_reimport` | A line already imported has different content. |
| `amount_out_of_range` | An amount cannot be represented in nanounits. |
| `settlement_act_refused` | A settlement the import makes is refused. |

Tests: `statement_import_refuses_a_directory_without_importing`,
`statement_import_refuses_files_over_64_mib_without_importing`,
`statement_import_refuses_fifo_without_waiting_for_a_writer`,
`safeguard_statement_import_reads_relative_name_inside_folder`,
`safeguard_statement_import_refuses_parent_escape`,
`safeguard_statement_import_refuses_absolute_path_outside`,
`safeguard_statement_import_refuses_file_symlink_outside`,
`safeguard_statement_import_refuses_folder_symlink_outside`.

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

Recomputes the seven days ending at `observation_day` (`YYYY-MM-DD`) at 00:00Z.
Name a `quota_provider` or omit it to recompute providers with readings in
that interval or a record for that day. Each new row supersedes its own
provider/plan/pool head for the day, or starts a chain if none exists. For a
pool that drops out, recompute appends a `pool_retired` row: the old head no
longer appears in the report. Pre-migration chains are not superseded.
All rows and provider-day records commit in one transaction or none does.
Reply: `{"recomputed": [{quota_provider, plan, observation_day,
observation_id, supersedes, multiplier, fee_multiplier}]}`.

| Code | Condition |
|---|---|
| `malformed` | `observation_day` is not a date. |
| `unknown_quota_provider` | The named provider has no readings, observations or provider-day record, or no providers qualify when the name is omitted. |
| `catalog_unavailable` | No fusiform catalog is connected, or a catalog call failed. |
| `plan_prices_unavailable` | Plan prices were needed and could not be read. |
| `rate_fact_unrecognized` | The catalog served a rate this build cannot read. |
| `source_reads_pending` | A pool may be measurable, but a tracked source is stale or has backlog and two hours have not passed since the interval ended. Recompute checks this even for an existing day. After the bound, measured rows with `source_backlog` are routing-ineligible. |
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
  `auth_selection`, `charge_basis`, `work_class`, `project`, `producer`, `runner`,
  `harness`, `plan`, `plan_account`, `alfonso`, `parent_alfonso`, `workload`,
  `project_id` and `workspace`; `grain` is
  `hour`, `day`, `week`, `month` or `range`; `timezone` is an IANA name. Refusals:
  `invalid_grouping` (unknown or repeated), `invalid_grain`,
  `invalid_timezone`. Test: `spend_report_refusal_keeps_its_code`.
  Event exclusion reasons in spend, work-class and ledger coverage include
  `observed_only` when a producer reported readings but no billed quantity.
  The work-class report's populations and producer labels are unchanged.

  Optional `from_ms` and `to_ms` must be supplied together, with `from_ms < to_ms`,
  or the read refuses `invalid_range`. They select a half-open interval widened
  outward to whole buckets. Without them, day/week/month/range cover all history as
  before. Day buckets begin at the first existing instant of the local date;
  weeks begin Monday, and months begin on the first local date of the month.
  Day ranges are limited to 400 days after widening. Week/month requests are
  limited to 400 days before widening, with widening permitted to add less than
  one week/month bucket on each side. A supplied span over 400 days is always
  `range_too_large`. These limits use elapsed 24-hour days, not local-date counts.
  Hour buckets are UTC hours regardless of timezone, including across daylight
  saving transitions. Hour requires an explicit range and allows at most 744
  UTC buckets after widening; otherwise it refuses `range_too_large`. A 31-day
  interval with non-hour-aligned edges therefore needs shortening or alignment.
  Hour refuses `hour_timezone_unsupported` if the timezone has any offset that
  is not an integral number of UTC hours anywhere in the widened range, including
  historical second offsets and fractional-hour daylight saving offsets.

  `range` selects the same local days as `day`, with the same 400-day limit and
  grouping and filter rules. It returns one row per group and currency instead
  of one per day. With explicit bounds, all rows share the widened request
  edges. Without bounds they span the first through last local day with
  selected usage, including future-dated days if the day report shows them.
  The range has no rows if the equivalent day report has none. Counts and
  money sum the day rows, retaining their daily rounding and fee allocation.
  `deduced.by_rule_share` is never present because fees are not assigned by rule.
  `totals` and `attribution_coverage` match the equivalent day report.
  Test: `spend_report_range_matches_daily_values_across_dst`.

  `runner` groups calls by the recorded runner (`broca`, `thalamus` or
  `colliculus`); null identifies unmanaged calls. The label is recorded once,
  not derived from current session declarations.
  `harness` names the host loop: the event's harness for `thalamus-harness`
  payer rows, and its producer otherwise. A BROCA call therefore has harness
  `broca`, regardless of its event's harness field. Both dimensions support
  `where`; `{"runner":[null]}` selects unmanaged calls.

  `where` is an optional object mapping any supported grouping name to a
  nonempty list of strings or JSON null. Values are ORed within each dimension,
  dimensions are ANDed, and null matches an absent dimension, not an empty
  string. Filters need not be grouping dimensions. An unknown key refuses
  `invalid_grouping`, just like an unknown grouping; an empty list refuses
  `invalid_filter`. The recognized per-run `task` dimension still refuses
  `dimension_unavailable` in grouping and where. Unknown request members remain
  `malformed`.
  `undated` and `rate_staleness` describe the selected population, so a ranged
  report's zero undated count does not claim the ledger has no undated events;
  `ingest_freshness` remains global because it describes the sources.

  Attribution dimensions are `alfonso`, `parent_alfonso`, `workload`,
  `project_id` and `workspace`. Up to two distinct attribution dimensions may
  appear across grouping and where, with at most one of `model` or `provider`,
  whether grouped or filtered. Any other dimension combined with attribution,
  or using both model and provider, refuses `combination_unavailable`, naming
  the incompatible pair. Attribution supports the same ranges, calendar grains,
  exact quantities, recorded spend, list value and deduced cost as ordinary
  reports. Fees still use the whole unfiltered UTC day for each subscription
  pool; attribution filters select only row weights, never fee denominators.

  `project` is the folder the call ran in, as recorded in `project_root`; its
  existing `hierarchy_resolution: "unavailable"` is unchanged. `project_id` is
  the project the publisher named for the session, run or flow. It has no
  `hierarchy_resolution` member. Combining legacy `project` with an attribution
  dimension is unavailable, not a request to translate folders into projects.

  A call joins its run fact first, then its session fact, then its flow fact.
  Session names meet through the runner's canonical reference and recorded
  aliases; runner labels are normalised for matching, not for reference
  computation. The newest unsuperseded fact supplies its owning `alfonso` and
  workload. `parent_alfonso` comes from the launching subject's own winning
  fact. Workspace is the placement of that fact's project at the call's
  occurrence time. If none of those publisher facts matches, a BROCA call can
  use the daemon scope stamp on its export. A stamped flow joins the flow's
  publisher fact. A stamped agent supplies `alfonso`, with `parent_alfonso` and
  `project_id` from the stamped run's publisher fact when known. The workload
  is `broca_session` for session segments and model attempts, and `system_one`
  for System One calls, whose export does not name an operation. No task is
  inferred. Without matching publisher or stamp evidence, all five values are
  null (Not attributed). A known fact may itself name a null Alfonso, so a null
  Alfonso group alone does not prove that every call lacks attribution.

  A request grouping or filtering by attribution additionally carries
  `attribution_coverage: {attributed_calls, unattributed_calls}`. These count
  dated calls in the selected range with its non-attribution filters, ignoring
  attribution filters. Their sum equals the ordinary report's dated call total
  for that population. When nonzero, `stamp_disagreement_calls` counts calls
  in the same population for which the winning publisher fact and the stamp
  name different agents. The publisher still wins. Even without a range,
  undated calls remain solely in the separate `undated` disclosure. Ordinary
  requests omit attribution coverage.
  Tests: `attribution_report_real_split_rows_keep_unattributed_and_all_metric_totals`,
  `attribution_report_randomized_groups_sum_to_ordinary_totals_with_identical_model_filters`,
  `attribution_report_filter_coverage_ignores_attribution_but_keeps_model_filter`.

  Each row and each currency total additionally carries `tokens` and `units`.
  Tokens have optional `input`, `output`, `reasoning`, `cache_read`,
  `cache_write`, `cache_write_5m` and `cache_write_1h` integer counts. Output
  includes reasoning using the Runs served-token rule: stored output plus stored
  reasoning, but output stays omitted when only reasoning was stated in that
  row or total. Reasoning is also a breakdown inside output. Cache write is all
  lifetimes together; the 5-minute and 1-hour fields are parts of that total.
  A role with no stated quantity is omitted, never invented as zero; a stated
  zero remains present. Exact fractional token sums are served only when they
  represent whole tokens, as on Runs reads. Counts include measurements
  regardless of pricing state or charge basis, including observed readings.
  Non-token units are a list of `{role, unit, quantity: {units, exponent}}`,
  with exact base-10 quantities and no floating-point conversion. Token-role
  measurements missing quantity are counted in optional
  `unknown_quantities: {role: count}` on the row/total. For non-token units the
  count is an optional `unknown_quantities` member on the corresponding
  `{role, unit}` entry, whose `quantity` is omitted if none was stated. A count
  with unknown measurements is a lower bound. Totals aggregate stored roles
  before applying inclusive output, so they can include reasoning whose row
  omitted output. All new figures use hourly summary quantities for clean
  hours; dirty hours and UTC hours crossed by a local bucket boundary or UTC
  offset transition use the existing ledger fallback.
  Tests: `analytics_unknown_quantities_are_not_measured_zero_on_either_path`,
  `analytics_tokens_and_exact_media_units_are_additive`,
  `analytics_widened_range_caps_and_calendar_edges`,
  `analytics_refusals_are_specific_and_read_only`. Handler samples for range
  grain and `reply_too_large` are in `crates/astrocyte-module/test-vectors/analytics-reads-v7`,
  pinned by `analytics_vectors_match_real_dispatch_and_sha256sums`.
  The published `analytics-reads-v3` through `analytics-reads-v6` sets are unchanged.

  Every row and currency total also carries `list_value`, with
  `{currency: "USD", nanos, unvalued: {measurements, tokens, by_reason},
  not_yet_valued_hours}`. It values positive measurements at the published USD
  API rates in force at each call, independently of recorded charge basis.
  Exact token line values are summed before one half-even rounding to nanodollars.
  Unit-billed measurements are valued per measurement, as they would be charged.
  Unusable positive measurements each count once under `no_rate`, `refused`,
  `non_usd`, `missing_model` or `valuation_error`; `tokens` counts their token
  quantities. Zero quantities do not count as unvalued. Other valued measurements
  in the same call still contribute, except that a valuation error discards the
  entire call's partial valuation. `not_yet_valued_hours` counts distinct UTC
  hours whose valuation is not current, not missing rates already fetched and
  known to be absent. List value is therefore an at-least figure. Bundled usage
  above a context threshold has no per-call prompt size: its tokens are
  **valued at the base rate; the real cost may be higher**. The optional
  `base_rate_tier_untested_tokens` counter on `list_value` counts those tokens
  and is omitted when zero, on rows and totals. `tiered_bundle` is not an
  unvalued reason.

  `plan` names the active subscription fee record's provider, including for
  a hand-recorded plan. `plan_account` is that record's account or credential.
  Calls with charge basis `metered_api` are `api`. Other calls look up fee
  records by account first, or by paying credential when there is no account,
  preferring the resolved credential to the selected credential. No active
  record gives `none`;
  more than one active record gives `pool_conflict`, still valued but never a
  fee weight. Those labels have null `plan_account`. `account_unknown` is no
  longer used. A plan with multiple quota pools still has one membership
  and its fee is counted once. Unlike the multiplier, hourly grouping uses
  fee records rather than vault plan history. Both dimensions work
  in `grouping` and `where`, including null filters. When grouping or filtering
  by either dimension, unbuilt or stale valuation hours retain recorded spend
  under `plan: "pending"`, `plan_account: null`. Otherwise their recorded rows
  are unchanged and their hours count in `not_yet_valued_hours`. A summary-dirty
  hour uses the same per-call valuer and membership resolver as the builder;
  an unfetched rate timeline leaves that hour pending. Calendar seams use the
  same bounded per-call path. These figures never change recorded spend,
  `total_nanos` or `metered_nanos`.

  Rows with USD or no recorded currency additionally carry `deduced`:
  `{currency: "USD", nanos, pending_days}`. It is recorded spend plus the row's
  exact share of USD subscription fees, rounded once half-even. Fees come only
  from account/credential plan records, prorated over UTC calendar days of the
  plan's month or year and over partial-day membership. Each record spreads
  its fee over its own calls' list value. Without positive weight the fee
  stays unspread; it is not allocated to calls lacking a matching record.
  Day denominators always cover the whole UTC day, unfiltered; row numerators
  cover only matching calls. A day is pending if it has not ended or an hour
  holding the record's calls is not current;
  pending fees are neither spread nor unspread. `pending_days` counts distinct
  pending UTC days touching the row. A dirty summary is never a built valuation
  hour even when its live row can be valued. While an hour awaits valuation,
  non-metered NULL-account summary cells without a retained resolved payer
  conservatively defer a fee spread if a credential plan could
  match. This can delay a spread until the builder reaches the hour, but cannot
  assign a fee to the wrong calls.

  Summed row `deduced` nanos match the USD total within one nanodollar per
  independently rounded row (the day rows before folding for `range`). The
  exception is a USD fee share assigned to a call whose recorded currency is
  neither USD nor null: only the USD total counts that share.

  Only the USD currency total carries `deduced` and the fee diagnostics:
  `deduced_unspread_nanos`, `deduced_non_usd_plans`, `deduced_conflict_plans`.
  Unspread fees are prorated to the report window's overlap with the plan's
  active part of each UTC day and rounded once. A filter on `plan` or
  `plan_account` restricts them to matching records; a filter on any other dimension omits
  `deduced_unspread_nanos`. Non-USD plans contribute no fee and count in
  `deduced_non_usd_plans`; records mapping to multiple pools count in
  `deduced_conflict_plans` and their USD fees go directly to unspread. Totals
  aggregate exact values and distinct pending hours/days across rows before
  rounding. Non-USD and null-currency totals carry `list_value` only. When USD
  fees exist but no USD calls match, an additional USD total has zero recorded
  spend and exposes those fees; no such entry is added without USD fees.
  With explicit dates, the fee window is the widened report window. Without
  dates, it starts at the first calendar bucket holding any dated summary hour
  and ends at the end of the bucket holding `now_ms`; with no dated hours it
  covers just that current bucket. Plans before the ledger's first bucket do
  not contribute, but fee days after its last call still do. If the ledger's
  first bucket is after the current bucket, the fee window is empty; recorded
  spend and list value are still returned. Tests:
  `report_surface_plan_grouping_filters_and_unfiltered_fee_weights`,
  `report_surface_pending_hours_preserve_recorded_spend_and_distinct_counts`,
  `report_surface_dirty_fallback_matches_built_across_five_timezones`,
  `report_surface_existing_members_are_byte_identical`,
  `report_surface_range_free_fees_cover_empty_ledger_and_days_after_last_call`,
  `report_surface_currency_totals_expose_fees_once_and_do_not_add_empty_usd`,
  `report_surface_bundle_counter_is_served_in_rows_and_totals`.
- `spend.subscription.report` keeps `rows`, `coverage` and `ingest_freshness`.
  `rows` lists account plans only. The additive `credential_rows` lists covered
  usage of credential-keyed plans as `{provider, credential_ref,
  period_start_ms, period_end_ms, plan_cost_nanos, covered_events,
  covered_measurements, quantities}`, and the additive
  `coverage.events_covered_by_credential` counts the account-less events those
  plans cover. Such events are not counted in `coverage.events_unpriced`;
  `coverage.events_without_account` still counts every dated event with no
  account. Hours holding a fully priced account-less subscription call on a
  provider with a credential plan in force are read from the ledger rather
  than the hourly summary. Tests:
  `vault_key_only_plan_covers_past_calls_by_paying_credential` and
  `credential_plan_seams_only_hours_with_covered_account_less_calls`.
  It adds optional diagnostic members; the request remains `{}`, so the params
  schema stays at version 2. `unpriced_plans` lists `{plan_key, provider,
  plan_tier, accounts}` for exact vault keys without a usable sourced price.
  `provider` and `plan_tier` retain the vault's raw spelling, and `accounts`
  contains account IDs, or vault record IDs for credential-keyed plans. The
  owner of the price catalog (the fusiform module) reads this list to find
  tiers that still need a sourced price. `unknown_plans` lists
  records without exactly one provider and a tier, with a null `plan_key`.
  `hand_vault_differences` lists `{provider, account_id, ledger_plan, vault_plan}`;
  the hand-recorded ledger interval is never changed by the vault.
  `vault_record_differences` lists disagreements between records sharing an
  account, with `provider`, `account_id`, `records`, `winning_plan` and
  `ambiguous`. The latest effective observation wins; equal-time disagreements
  have no winner and cannot change the ledger. Records include `credential_id`,
  `provider`, `account_id`, `plan_tier`, `plan_key`, `source`,
  `effective_from_ms` and `observed_at_ms`. `chatgpt_account_mismatches` lists
  `{credential_id, account_id, account_refs}` where subscription calls attributed
  to that OpenAI credential carry a different account reference.
  `invalid_plan_tier_since`, omitted when empty, lists `{credential_id, reason}`
  for ignored `plan_tier_since_ms` values in the latest successful listing.
  Reasons are `not_non_negative_integer` or `after_observed_at`; affected rows
  still use the observed timestamp, or the first observation when it is absent.
  `vault_read_status` is `{status: "not_read" | "available" | "unavailable"}`,
  with a safe `reason` on failures; it is process-local and resets on restart.
  `plan_price_status` is `available` or `unavailable`, independently of the vault
  read. A failed vault read changes no recorded plans or history. Tests:
  `vault_hand_plan_is_untouched_and_difference_is_reported`,
  `vault_unpriced_raw_tier_is_reported_and_later_price_records_plan` and
  `vault_shared_account_uses_latest_record_without_tick_order_churn`.
- `spend.workclass.report`: `rows`, `producers`.
- `spend.ledger.coverage`: `set_aside_by_source`, `skipped_local_by_source`, `events_total`,
  `events_unpriced`, `events_undated`, `zero_measurement_events`,
  `unpriced_events_by_reason`, `unpriced_measurements_by_reason`,
  `broca_segment_usage_mismatches`, `ingest_freshness`, `rate_staleness`, `capacity`. Test:
  `open_coverage_report_does_not_stall_an_ingest_commit`.
  `skipped_local_by_source` is always a map of producer keys to counts of local
  work acknowledged without spend events; it is empty when no work was skipped.
  Test: `media_skipped_local_coverage_is_live_on_base_and_hourly_paths`.
  `broca_segment_usage_mismatches` counts marked BROCA segments for which any
  reported usage quantity differs from the sum of attempt shares naming the
  segment. The refresher checks a bounded batch per tick and waits until a
  segment has been in the ledger for more than an hour, giving its attempts
  time to arrive. It records each differing role and unit once; an unreported
  quantity is distinct from zero. This is a consistency check, not a pricing
  input.
  Optional `thalamus_payer` reports lifetime counts of distinct identities:
  `transcript_calls_without_payer` (timeout or queue overflow), `late_payer_rows`,
  `unmatched_main_rows`, `set_aside_payer_rows`, `owner_disagreements` (the owner
  Thalamus's roots file declares for a session differing from the owner on its
  payer session row) and `cache_split_mismatches`
  (well-formed main cache-write splits that disagree with one another or do not
  match a transcript cache-write total). These counters include explicit
  zeroes when payer coverage is present. Replays do not increase them.
  Its additive `waiting_calls_by_reader` map reports current process-local queue
  lengths by reader source key, including explicit zero after a queue drains.
  A restart clears these gauges until readers replay their transcripts; it does
  not clear the lifetime counts. Tests:
  `payer_coverage_counts_exceptions_once_and_reports_live_waits`,
  `thalamus_payer_coverage_is_live_on_base_and_hourly_paths`.
- `spend.charge.explain`: `event`, `measurements`, `adjustments`,
  `transition`. Each measurement supplies `role` and `unit`; its charges are
  identified by `pricing_generation` within that measurement. There is no
  `charge_id` field. `supersedes` is the predecessor's pricing generation (null
  for an original), not an integer storage key. Event IDs are unchanged and no
  local keys are served. Refusals: `malformed` (empty `event_id`), `not_found`.
- `spend.reconcile`: `statement`, `lines`, `summary`. Refusals: `malformed`,
  `statement_not_found`; a store failure is `store_error`.
- `spend.multiplier.report`: migration 68 introduced per-pool observations.
  For each quota provider, `latest` shows the
  unsuperseded, unretired pool rows of its newest recorded day, one per
  `(quota_provider, plan, pool_id)`. An empty day yields no rows for that
  provider. An unmeasured row still appears; `latest` does not fall back to
  an older day. `last_eligible`, when available, gives the newest eligible,
  unsuperseded head for the same provider, plan and pool as `{day,
  interval_end_ms, multiplier, fee_multiplier}`. This value can come from
  before migration 68, but those rows never appear in `latest` themselves.
  `series` shows unsuperseded, unretired rows with `interval_end_ms` in
  `[from_ms, to_ms)`. Historical rows appear there only for keys without pool
  observations since migration 68. Refusal: `invalid_range` (`from_ms`
  negative or not before `to_ms`).

  `pool_id` is insula's slot name (`primary`, `secondary`, `tertiary`) or
  the raw ID of an extra window, not its stored `window_pool_key`. A historical
  row can have a null `pool_id` if its stored key has no stable identity.
  `pool_scope` is `all`, `subset`, or `unknown` when there is no single stated
  scope; it is null on historical rows. `catalog_provider` is the catalog
  provider the readings agree on, or null; it is distinct from the insula
  `quota_provider`. `pool_models` is the served subset list as `provider/model`
  IDs, or null when there is no list. `pool_models_at_ms` is the day-ending
  `plan.prices` lookup instant for a subset, even if its list is missing;
  otherwise it is null. `pool_member_models` records raw IDs admitted through
  suffix removal or alias resolution; it is null if `pool_models` is null.
  `window_minutes` is the newest stated duration (the larger duration if
  readings share that instant).

  Reasons `scope_unstated`, `scope_changed`, `window_changed` and
  `pool_list_missing` mark rows whose ratios are null and whose
  `routing_eligible` is false. Their zero token counts, `api_nanos`,
  `subscription_nanos`, `fee_paid_nanos`, `delta_percent`, quality shares and
  usage and login counts, and empty usage and login diagnostics are
  placeholders, not measured zero usage. `window_tie` also has null ratios and
  is ineligible, but its other measurements are computed normally. A recompute
  uses `pool_retired` to supersede a pool that dropped out of that day. Retirement
  heads are not served in `latest`, `series`, `last_eligible`, pace or the bus.

  Rows retain `routing_eligible`, `eligibility_reasons`, `clean_hours_share`,
  `saturated_share`, `gap_share`, `unpriced_share`, `unconfigured_accounts`,
  `source_backlog`, `unattributed_event_share`,
  `zero_measurement_events`, `missing_quantity_events` and the derived
  `unknown_quantity_measurements` count. Missing-quantity and no-measurement
  usage exclude account-hours; neither their counts nor their share independently
  veto a row. Retained sample adequacy and stability govern eligibility.
  Saturation keeps its separate veto. Quality shares are fractions of the
  login-hours under consideration for the row's plan and pool; unpriced
  diagnostics also include excluded hours. Historical rows retain their old
  values, including a null plan on older rows pooled across plans.
  `api_nanos_by_model[].rates` may include an optional `conditional` map for
  complete token condition sets, including lifetime-only rows and combined
  lifetime, mode and context-tier rows. Its keys are rate keys with selectors
  sorted by name. It is omitted when empty; older rows decode with an empty map.
  A supported billing rule is stored at its role and condition as
  `{"state":"billed_as","key":"rate.output"}` for reasoning, or with `rate.input`
  (including its named context-tier key) for cache writes.
  `stability_diagnostics` adds the recorded day-block bootstrap interval/width,
  active-day count, held-out-day six-hour RMSE/R² and resampling parameters;
  older rows without calibration inputs return null there. Adequacy uses clean
  hours/points, active days and interval width, not a clean-, gap- or unknown-share floor.
- `spend.multiplier.pace`: hourly usage by producer and work class, valued at
  the selected head's recorded rates without a catalog call. Optional `plan`
  and `pool` select a plan key and a `pool_id`. Candidates are eligible,
  unsuperseded pool heads ending in `[to_ms − 2 days, to_ms]`; historical and
  retirement rows do not qualify. Without `plan`, candidates spanning more
  than one plan refuse `plan_required`. Without `pool`, candidates in the
  selected plan spanning more than one pool refuse `pool_required`. A pool
  can qualify even if it is absent from the provider's newest day. The newest
  candidate of the selected plan and pool supplies the estimate; the reply
  names `plan` and `pool_id` and sets `routing_eligible: true`.

  Pace counts only subscription-included calls attributed to the selected
  plan. An `all` pool includes every such call. A `subset` pool includes only
  raw `provider/model` IDs in the head's `pool_models` or `pool_member_models`.
  Pace cannot resolve new aliases or strip suffixes offline: a raw ID that
  only matches by such a step must already be in `pool_member_models`.
  Other refusals: `unknown_quota_provider` (no observation since migration
  68), `no_multiplier_observation` (no eligible candidate for the selection),
  `invalid_range` and `range_too_large` (over 192 aligned hours).

Absent values are `null` or omitted, never zero. A total that cannot be
priced is reported with its unpriced population, not as a smaller number.

### 4.1 Cache diagnostics

These two operations serve harness steps per session, with cache verdicts.
A harness is a producer whose events are one
model step each: `claude-code`, `codex`, `gemini`, `kimi`, `omp`, `opencode` or
`pi`. BROCA steps will arrive from its step log, not its exported spend facts.
A request that fails to decode is refused `malformed`, as everywhere
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

- `cursor`, the durable cache-step cursor. It increases across harness
  events, survives restarts and is never reused.
  A late step can have a greater cursor than a step that happened after it.
- `step_ref`, the harness's own id for the step, and `occurred_at_ms`.
  BROCA's step-log reader will use the event's `attempt_id`, a random id per
  provider attempt, as described in section 4.2.
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
  Code and Oh My Pi. `context_limit` comes from Codex.
- `request_body_len` is optional and is not populated by the current readers. On a prompt drop,
  `request_size` is present when both adjacent steps report body lengths:
  `smaller` when the current body is shorter, otherwise `not_smaller`.
- `prompt`, which is input plus cache read plus cache write.
- `retention`, `verdict` and `prompt_dropped`. `retention` is served
  unrounded and unclamped: a non-negative number that may exceed 1 when a step
  reads more cache than expected.
- `cause`, omitted when there is no stored explanation. This is the feed's
  persisted rewrite cause, looked up by `(harness, step_ref)`, not a guess
  computed from ledger rows. The cause is kept forever, including after its
  evidence and live feed row expire.
- `account_ref`, `warm_up`, `view_version`, `tools_digest`, `system_digest`,
  `signed_floor`, `started_at_ms`, `finished_at_ms`, `cache_ttl_ms` and
  `cache_implicit` are the feed's event-only rewrite evidence, with the same
  meanings as in section 4.2. They are read by `(harness, step_ref)` for 30 days
  from when the step first reached the feed, independently of the live feed's
  one-day window. Each member is omitted when unknown or expired, never
  defaulted. A retained `view_changed` cause therefore includes `view_version`
  while that evidence is kept, but can outlive it.

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

Sample replies, published for a client to pin its decoder against, live in
`crates/astrocyte-module/test-vectors/usage-cache-steps-v1/`, with the request
that produced each one. The step `cursor` is the cache-step cursor, a
different cursor space from the live feed's `cN` cursors: it numbers recorded
steps in their own order, so two rows with the same number in a cache read and
a feed read are not the same step. To join a session's history with
the live feed, an app matches rows by (`harness`, `step_ref`) and replaces its
stored copy of a matched step with the newer row, and never compares a
cache-step cursor with a feed cursor.

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
with the person's phone or desktop as the device label (`fed-phone:<fingerprint>`
or `fed-app:<fingerprint>`). Every other
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

The request is `{}` or `{"harness": "pi", "session": "session-id"}`.
`harness` and `session` are optional, but must come together; one alone is
refused `invalid_request`. Unknown members are refused `malformed`.

Each notice is one StreamData frame:

```json
{"through": "c1824004"}
```

`through` is the newest committed cursor. Notices are coalesced: at most four
a second, and a notice may cover many steps or none that the caller may see.
With a session filter, a notice is sent only when a row whose own `harness`
and `session` match exactly is appended. Its `through` is the feed-wide cursor
committed with that row; later unrelated rows do not advance it, so it may be
older than the feed's newest cursor. A caller reads `after` its own last cursor
with the same filter, so this changes nothing it needs. Without a filter,
notices are unchanged.

| How it ends | Meaning |
|---|---|
| StreamEnd | Astrocyte is stopping or the route closed. Subscribe again. |
| Error `lagged`, detail `{"through": "<last notified cursor>"}` | The subscriber fell behind its queue bound. Subscribe again and read from the last cursor it holds. |
| Caller's Cancel | Ended by the caller. The caller may still receive Error `cancelled` (the daemon may drop it) and treats it as the normal end. |

Opening refusals: `not_permitted`, `subscription_capacity` (the shared limit of 16
open step and Runs subscriptions), `draining` (astrocyte is stopping), `malformed`, and
`invalid_request` (only one filter member was supplied).

#### `usage.steps.read`

The request is `{"after": "<cursor>", "limit": 200, "harness": "pi", "session": "session-id"}`.
All members are optional. `harness` and `session` must come together; one alone
is refused `invalid_request`. Unknown members are refused `malformed`.
Without `after`, the read starts at the oldest row the feed still holds. The
feed holds the rows committed in the last 24 hours. `limit` defaults to 200,
with a maximum of 1,000.
With a session filter, only rows whose own `harness` and `session` match exactly
are returned, still excluding superseded identities. `limit` bounds the rows
returned, not the unrelated rows between them. `through` remains the newest
cursor across the whole feed. `next` is the last returned row's cursor while
more matching rows exist up to `through`, and null when no matching rows remain.
A filtered page is never empty with a non-null `next`. Cursor expiry is unchanged.
Without a filter, the read and its reply are unchanged.

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

Runner rows are served: a row from a runner's `step.finished.v1` event takes
its identity from the event. When the event carries `harness_step_ref` (runner contract 14.3,
present for runners whose history is the host harness's own record),
`harness` is mapped through `cortexkit-usage-steps`' host table (`opencode2`
is served as `opencode`), and `step_ref` is `harness_step_ref`, so
astrocyte's later read of the harness store replaces the row. An unmapped
host is counted and dropped. Without `harness_step_ref`,
`harness` is the event's `runner` and `step_ref` is the event's `attempt_id`, a
random id per provider attempt. Retries reported as separate steps have their
own rows, matching astrocyte's read of that runner's durable step log. Refused
attempts listed in a step's `fallback` do not create separate runner rows. A
step with no durable record stays a live-only row with `source: "runner"`.

- `source` is `file` for a step astrocyte read from a harness store or a
  runner's durable log, and `runner` for a step known so far only from a
  runner's bus event.
- `harness` is the producer: `claude-code`, `codex`, `gemini`, `kimi`, `omp`,
  `opencode`, `pi`, or the runner's module id (`broca`, `thalamus-harness`,
  `thalamus`).
- Required: `cursor`, `harness`, `source`, `step_ref`, `session`,
  `occurred_at_ms`, `prompt`, `verdict`, `prompt_dropped`. Steps without a time
  are not in the feed.
- Optional, omitted when unknown and never zero for unknown: `message_id`,
  `run_id` (runner steps), `provider`, `model`, `input`, `cache_read`, `cache_write`,
  `cache_write_5m`, `cache_write_1h`, `output`, `reasoning`, `finish`,
  `context_limit`, `request_body_len`, `retention`, `request_size`,
  `started_at_ms`, `finished_at_ms`, `account_ref`, `cache_ttl_ms`,
  `cache_implicit`, `warm_up`, `view_version`, `tools_digest`, `system_digest`,
  `signed_floor`, `cause`.
- `started_at_ms` and `finished_at_ms` are the event's request start and
  completion timestamps, not a reinterpretation of `occurred_at_ms`.
  `account_ref` is the runner contract's domain-separated SHA-256 reference to
  routing's admitted account and credential, never their raw ids.
  `cache_ttl_ms` is the shortest cache-breakpoint lifetime; `cache_implicit`
  is true for a cache whose lifetime cannot be proved and is never emitted
  false. `warm_up` marks a keep-warm request. Unknown is always omitted.
- `view_version` is the applied compaction view (`last_prompt_view`), where
  zero means no applied view. `tools_digest` and `system_digest` are opaque
  runner-local visible-ASCII digests of provider inputs, ignoring cache
  markers. `signed_floor` is the stable entry id of the newest boundary before
  which the runner invalidates earlier signed reasoning after a prompt-prefix
  change, such as a changed tool definition.
  Event evidence is retained for 30 days from when the step first reached the
  feed and remains on the file row when a
  durable read replaces a runner row. It is never reconstructed from ledger
  rows, and repeated digests are interned rather than copied per feed emission.
- `cause` is computed once by `cortexkit-usage-steps` 0.2.8 for `warning`,
  `bust` and `full_bust`, in precedence order: `account_or_model_changed`,
  `cache_expired` (busts only), `tools_or_system_changed`, `view_changed`,
  `signed_floor_moved`, then `unexplained`. Missing facts skip comparisons.
  The predecessor is the same harness/session's previous distinct step from
  the same runner, warm-ups included, ordered by request start (occurrence
  time when unknown), with first feed cursor breaking ties. Steps with no
  runner identity compare only with other such steps. Late steps and
  replacements also recompute the following step's cause. If late evidence
  changes a step's comparison runner or request start, the successor at its
  old position is rejudged as well. Changed explanations
  append a new feed row and notify subscribers through the existing cursor
  notices; subscriptions carry no row payload. Cause history is append-only
  and kept forever, independently of its evidence. `usage.cache.steps` serves
  the newest stored cause even after live feed retention expires; no cause
  means the member is omitted, not `unexplained` by default.

Synthetic samples for `usage.cache.steps` and `usage.steps.read` are pinned in
`crates/astrocyte-module/test-vectors/usage-cache-causes-v1/` with SHA256SUMS.
They cover every cause and a row without one. The strict llm-runner 0.9.6
step-event decoder accepts the optional `fallback` record; older pinned runner
events remain valid.
- `message_id` is a content-free message identifier: on a file row the host's
  own, on a runner row the one the runner serves for that message. It is
  present only when the host or runner has one and is never invented. Consumers join on `harness` plus
  `session` plus `message_id`, not on `message_id` alone: it is unique only
  within its session. `harness` plus `step_ref` remains the row identity;
  consumers must not parse `step_ref` to recover a message id, because its
  format is not promised. OpenCode 1 and 2 (including THALAMUS stores) use the
  message's `id`; Pi uses the assistant entry's `id`; Claude Code uses
  `message.id`; Gemini CLI uses the message's `id`; Kimi CLI uses
  `StatusUpdate.payload.message_id`. Codex has turn and response ids, Oh My Pi
  has entry and response ids, and Goose has session totals, so none supplies a
  message id here. Pi operation entries are not messages and omit it too.
  A runner row carries the step event's `message_id` when the runner sends
  one: the id the runner serves for the assistant message the step's reply
  was placed in. A runner row without one omits it, and it is never derived
  from `step_ref` or `harness_step_ref`. A file row that replaces a runner row
  serves its own `message_id`, or none. Previously recorded
  rows are not backfilled; they remain without the member and age out of the
  24-hour feed.
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
  until the deletion reaches it. Pruning stops at the oldest live row, even
  when a backward wall-clock step made later cursors expire first.
- Runner rows come from the `step.finished.v1` events of the runners the bus
  naming table grants astrocyte; today those are `broca`, `thalamus-harness`
  and `thalamus`. Astrocyte follows each runner's own subject,
  `ck.<account>.live.<runner>.step.finished.v1`, with a plain subscription and
  no durable. The subscriptions share the quota reader's single
  ckbus credential and NATS connection, and they run whether or not insula is
  configured. A row is appended as soon as the event arrives, and subscribers
  are told once it is committed. Runner rows are display data only: nothing
  from an event reaches the spend ledger.
- A runner row's `prompt`, `retention`, `verdict`, `prompt_dropped` and
  `request_size` are computed by the same rules as a file row's, over the
  session's recorded steps and its other runner rows. When an event reports a
  refusal fallback, the row keeps the answering model and subtracts refused
  attempts from each token counter before computing prompt and cache diagnostics.
  A counter is omitted if the step or any refused attempt omits it. If the sum
  of refused attempts exceeds the step's counter, the row keeps the reported
  step value for that counter. A fallback without refused attempts
  (`cause: "round"`) leaves the counters unchanged. A runner's first positive
  cache read in a session can append corrected verdicts for earlier retained
  runner rows.
  `run_id`, `cache_write_5m` and `cache_write_1h` are sent only on runner rows.
  `output` includes `reasoning` unless the event says reasoning is outside it.
- When the feed's newest row for the identity is a file row, astrocyte drops
  the runner event, because the durable read is the step's record. When it is
  the same runner row, the event is a duplicate and is also dropped. A later
  file row for the same identity replaces a runner row. An event that does not
  decode, or whose subject names a different runner than its body, is counted
  and dropped. So is a step that finished more than 24 hours ago. If the bus is
  unavailable or refuses the subscription, astrocyte logs it once and retries,
  and no new runner rows arrive; runner rows already appended stay served
  until a feed append prunes them. Quota events are not affected.
- The device label is the route's bind identity `harness`
  (`fed-phone:<fingerprint>` or `fed-app:<fingerprint>`), as Callosum stamps it; the principal must be
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
`two_subscribers_both_receive_a_notice`,
`each_runners_event_is_served_as_a_runner_row`,
`a_file_row_for_the_step_supersedes_the_runner_row`,
`a_runner_event_after_the_file_row_leaves_the_file_row_newest`,
`an_undecodable_event_is_counted_and_dropped`,
`a_runner_rows_verdict_matches_the_file_row_that_replaces_it`,
`a_runner_row_wakes_its_sessions_filtered_subscription`,
`bus_reader_without_insula_serves_runner_step_events`.

### 4.3 Read watermark (`log-read-watermark/v1`)

`watermark.get` tells a module how far astrocyte has durably read the logs
that module writes, so the module can erase logs that have been recorded.
Astrocyte declares the capability in the manifest's `capabilities.provides`.
The names and wire types are the `watermark` module of
`cortexkit-role-llm-runner` (0.7.1, contract §9.15).

- The request is `{}`; any member is refused `invalid_params`.
- Astrocyte answers for the principal the daemon attested when the route was
  bound, never for a module the request names. It forgets that principal when
  the route goes or the connection ends.
- A module principal maps to its session reader. Today the only row is
  `thalamus-harness` → `opencode-thalamus` (the
  [THALAMUS sessions contract](thalamus-opencode-sessions-contract.md) §4).
- Reply `{"watermark_ms": <u64>}`, the minimum of the stored `opencode-thalamus`
  and `thalamus-payer` pass watermarks, read in one snapshot on the
  read-only connection; `{"watermark_ms": null}` (present, not omitted)
  until both sources have committed a complete read pass. Each payer pass uses
  its start minus 60 seconds and advances only after all mirror pages commit;
  a full pass that finds no payer store publishes the same safe empty-pass
  watermark without creating a runtime row or changing health. Fast absent-store
  checks do not advance it. When `thalamus-payer` is excluded, its component is
  ignored and the session source's value is served alone.
  Health timestamps and waiting queues do not determine this value. See the
  [payer contract](thalamus-payer-records-contract.md) §4.
- Refusal `not_a_read_source`: the caller is not in that table (a local app's
  `Direct` principal, any other module), or its source is excluded in
  `harness_ingest` (named in `exclude`, or `enabled: false`).

Tests: `watermark_get_answers_thalamus_harness_as_the_crate_vectors_pin`,
`watermark_get_refuses_every_caller_astrocyte_does_not_read`,
`watermark_get_refuses_thalamus_while_its_source_is_excluded`,
`the_bind_principal_is_forgotten_when_the_route_or_connection_goes`,
`manifest_provides_the_log_read_watermark_capability`.
The minimum and rollback rules are covered by
`watermark_requires_both_committed_passes_and_serves_the_lower_value`.
Absent, excluded and newly appearing payer stores are covered by
`payer_watermark_absent_store_full_pass_serves_session_progress_without_a_lane`,
`payer_watermark_excluded_source_serves_only_session_progress` and
`payer_watermark_store_appearing_after_absence_cannot_pass_its_uncommitted_first_row`.

### 4.4 Runs cost reads (`usage-runs-v1`)

These reads serve machine-wide session, BROCA run and BROCA flow costs from the
ledger, never from client-side pricing. They share the live feed's admission
check: `Principal::Direct` and reserved `callosum` routes with a `fed-phone:`
or `fed-app:` identity harness. Every other caller is refused `not_permitted` before operation-specific parameters are decoded.
Each reply is the response frame's `result`, read from one SQLite snapshot.

**Keys** are closed tagged objects:

- `{"kind":"session","harness":h,"session":s}`: `(producer, session)`, not
  the ledger's optional hosting-harness attribution. Harnesses are `opencode`
  (both versions), `claude-code`, `codex`, `pi`, `gemini`, `goose`, `omp`, `kimi`.
- `{"kind":"run","harness":"broca","run_id":r}`: BROCA `run_ref`, verbatim.
- `{"kind":"flow","harness":"broca","flow_id":f}`: BROCA batch `flow_ref`.

Run rows also carry `session` when any fact of the run records its label, even
if that fact is outside the requested range. It is absent for unlabelled runs;
no label is fabricated and no run is dropped. Flow rows carry `work_class` when
recorded; no additional flow-launcher identity is inferred.

**`usage.runs.list` request:** `{from_ms, to_ms, harness?, limit?, after?}`.
The half-open range must satisfy `from_ms < to_ms` and span at most 31 days.
`limit` defaults to 50 and must be 1–200. `harness` filters the key's harness.
`after` is the opaque `next` from a previous page with the same range/filter.

Reply: `{rows, totals:{rows, steps, tokens, cost}, next?}`. Range totals are on
**every** page and cover the entire filtered range. Each row is
`{key, first_at_ms, last_at_ms, steps, tokens, cost, pricing_unit, models}`;
models are `{provider, model, steps, tokens, cost}` splits of exactly the same
figures. Missing provider/model identity is `null`. Every figure counts only
facts with `occurred_at_ms` in the range. Undated facts are not listed.
Rows sort by newest `last_at_ms`, then canonical key; the cursor resumes after
that pair. `next` is absent on the last page. A concurrently active key may move
to the first page: restart paging on a feed notice rather than merging snapshots.

**`usage.runs.detail` request:** `{key, limit?, after?}`. `limit` defaults to 200
and must be 1–1000. Reply: `{key, pricing_unit, items, totals:{tokens,cost}, next?}`.
Totals cover the entire key, not just the page or the list's time range. Items
sort oldest first, with undated (`occurred_at_ms: null`) items first and stable
ledger order for ties. Harness items are
`{step_ref, occurred_at_ms, provider, model, measurements}`: `step_ref` is the
verbatim producer reference also used by the feed for step-based harnesses.
Goose's reference identifies its accumulated session event, not a feed call.
BROCA attempt items carry a `step_ref` identifying the attempt share in the
ledger, not its live feed step id. Other BROCA items carry `segment_ref`, the
fact id.

Pricing units are `step` for ordinary harnesses and BROCA runs with any attempt
calls, `run_segment` for other BROCA runs and flows, and `session_total` for
Goose. A BROCA `step` run can still have `segment_ref` items for unmarked
segments such as warm calls. Goose's detail has one item for its accumulated
session event. Segment/session-total costs belong to the run/session as a
whole; clients must not divide them over feed calls.

**Tokens** have optional `input`, `output`, `reasoning`, `cache_read`, and
`cache_write` integer members. Missing is not zero: a role is absent when no
included event reports an exact whole-token quantity. A role total too large to
represent is also left out, just like an unknown total. Exact decimal encodings
are normalized (100 × 10^-1 contributes 10). Genuinely fractional quantities
are omitted from integer totals without rounding and preserved exactly in detail. `output` includes reasoning, as on the feed;
`reasoning` is its breakdown. Stored detail roles remain disjoint.

**Cost** is `{priced, covered_measurements, unpriced_measurements,
unpriced_by_reason, pending_measurements}`. `priced` maps currency to signed
integer nanos and includes only current `priced` charges; it is `{}` if none.
Covered/unpriced/pending counts are never folded into money. Reasons are the
ledger's closed set, including `no_rate`, `no_catalog_coverage`, `rate_tiered`,
`rate_inherited`, and `stated_zero_requires_policy`.
Observed measurements count as unpriced under `unpriced_by_reason.observed_only`,
never as pending or free.
A billed measurement with no charge head is pending.
All-zero counts plus empty `priced` means no measurements.

Each detail measurement is `{role, unit, quantity, state, amount_nanos?,
currency?, reason?}`. `quantity` is `{units, exponent}` (decimal ledger quantity),
or `null` when unreported. State is `priced`, `covered`, `unpriced`, or `pending`.
Only priced measurements carry amount/currency, and only unpriced measurements
carry reason. Current means no charge row supersedes it, so repricing replaces
figures rather than adding historical charges. List money excludes adjustments.

List refusals: `malformed`, `invalid_range`, `range_too_large`, `invalid_request`
(bad limit, harness or cursor/request mismatch), `not_permitted`. Detail refusals:
`malformed`, `invalid_request`, `not_found` (no fact has that key), `not_permitted`.
Pricing may lag ingestion; pending counts expose that lag. Subscribe to
`usage.runs.subscribe` to re-read when calls or their recorded prices change.

List totals use rebuildable key/model/hour cells maintained by the existing
hourly dirty triggers, revision fence, bounded reducer, atomic publication and
audit. Clean complete hours use cells; dirty/missing/partial hours use indexed
base ranges in the same snapshot. Detail pages use key/time indexes with LIMIT;
whole-key totals combine clean cells and dirty-hour base reductions.

Synthetic vectors and SHA-256 checksums are published under
`crates/astrocyte-module/test-vectors/usage-runs-v1/`. Tests regenerate frame bodies
through the real dispatch and the serving path's shared permission refusal.

#### `usage.runs.subscribe`

The request is `{}`. Each StreamData body is only `{"cursor":"<opaque>"}`.
The cursor advances within the process when a committed call, a new or superseding
charge head (the current recorded price row for a measurement), or a rebuilt
run-summary hour could change Runs reads. A replacement charge preserves the
earlier row and becomes the new head. The cursor is a change
token, not a paging or durable resume cursor; a process restart can repeat a value.
On opening or reconnecting, read `usage.runs.list`, then re-read on each notice.
Notices are coalesced to at most one per two seconds per subscriber, and an idle
ledger sends nothing. No call or price evidence is carried by a notice.

Admission is exactly the Runs reads' admission: local apps and Callosum phone
and desktop routes, not flows. The watch shares the step watch's 16 subscription slots and
held-request pool. Opening refusals are `not_permitted`, `subscription_capacity`,
`draining`, and `malformed` (unknown parameters). Cancellation ends with the same
`cancelled` error as the step watch; route closure and module shutdown end the
stream. A slow subscriber receives the newest coalesced token rather than a
queue of intermediate tokens. Synthetic request, notice and refusal vectors with
checksums are in `crates/astrocyte-module/test-vectors/usage-runs-subscribe-v1/`.

### 4.5 Quota history (`spend.quota.history`)

A history of the insula quota readings astrocyte has recorded, read on the
read-only connection. It makes no provider request and estimates neither usage
nor spend. A route opened by an approved flow (§1) may not call it.

**Request:** `{from_ms, to_ms, grain, quota_providers?}`, for example
`{"from_ms":0,"to_ms":3600000,"grain":"hour","quota_providers":["claude"]}`.
`grain` is `hour` or `day`, in UTC. The range is widened outward to whole UTC
buckets, and readings are selected in that widened half-open range. The
widened range may span at most 31 days for `hour` (744 buckets) and 400 days
for `day`; a longer one, including an exactly-31-day hourly range that doesn't
start on the hour, refuses `range_too_large`. `from_ms >= to_ms` refuses
`invalid_range`. `quota_providers` selects quota-provider names: omitted or
`null` selects all, an unknown name selects nothing, and an empty list refuses
`invalid_filter`. Decoding is closed: a missing required member, an unknown
member, a wrong type or another grain refuses `malformed`.

**Reply:**

```json
{
  "windows": [
    {
      "quota_provider": "claude",
      "account": "synthetic-account",
      "credential": null,
      "pool": "primary",
      "window_minutes": 300,
      "buckets": [{"start_ms": 0, "used_percent_max": 142.0, "samples": 3}],
      "resets": [{"at_ms": 0, "from_percent": 197.0, "to_percent": 103.0}]
    }
  ],
  "earliest_sample_ms": -1
}
```

- One window per quota provider, account or credential, and pool that has
  readings in the selected range. An account reading has `account` set and
  `credential: null`; an account-less reading has `account: null` and its
  recorded `credential`. Windows sort by quota provider, accounts before
  credentials, then identity and pool; buckets and resets are in time order.
- `window_minutes` is the window length on the last selected reading, or
  `null` if it wasn't recorded. It can differ from earlier readings in the
  range.
- Each bucket has a UTC-aligned `start_ms`, the highest percent read in it
  (the raw percent when insula gave one, otherwise the normalized one, as the
  subscription multiplier reads them) and the number of readings. Percents can
  exceed 100. Buckets with no reading are omitted.
- A reset is the recorded reset time moving forward past the latest reset time
  seen so far by more than ten minutes, or half the window length if shorter,
  stamped at the later reading with the previous and current percents. A percent
  drop alone is not a reset, and a reading with no reset time cannot start a
  period. A cached reading whose reset time is older than the current period's
  by more than that tolerance is excluded from both buckets and resets. The
  most recent period observed before the range supplies the starting reset
  time, and the last accepted reading supplies the previous percent. Earlier
  readings add to no bucket.
- `earliest_sample_ms` is the first recorded reading among the selected
  providers, from either table and regardless of the range, or `null` if none.
  A provider with no reading in the range can set it without appearing in
  `windows`.

Samples, generated through the real handler and pinned by test, are in
`crates/astrocyte-module/test-vectors/analytics-quota-history-v2/`. Tests:
`quota_history_buckets_raw_percent_predecessor_resets_and_credentials`,
`quota_history_refuses_invalid_ranges_and_empty_filters`.

## 5. Calls astrocyte makes

| Peer | Route | Calls | On failure |
|---|---|---|---|
| fusiform (price catalog) | Attested `tool_provider`, module `fusiform` | `catalog.status`, `catalog.get`, `catalog.history`, `plan.prices` as `{"name", "arguments"}` tool calls | A failed pricing call writes no charge; the event stays queued and a later pass retries it. Recompute refuses. Route death and `module_reloading` are retried once within the call's deadline. |
| insula (quota) | Attested `management_surface`, module `insula` | `usage.get` with empty params; `events_get` with `event_key` and `event_name: "quota_sampled"` | Invalid or missing event bodies owe a snapshot. A failed snapshot records the attempt and keeps the last windows. Capacity is advisory and never blocks spend. |
| colliculus (media usage) | `management_surface`, module `colliculus` | `usage.read` pages and `usage.ack` after each committed page | Keep the committed cursor and retry. An absent module does not degrade health; transient route refusals (the module warming or reloading) defer the drain. `feed_not_admitted` means colliculus is not configured to let astrocyte read its usage feed; that won't fix itself, so it degrades health immediately. |
| ckbus | Attested `management_surface`, module `ckbus` | `ckbus.credential`, `ckbus.credential_renew`, `ckbus.nonce_sign` | Retain per-tick insula polling and retry with exponential backoff (250 ms to 30 s). Never fail the module or change broker topology. |
| claustrum (credential vault) | Attested `management_surface`, module `claustrum` | `credential.list_scoped` and held `auth.watch_scoped`, with empty params | Startup never waits for the peer. Missing vault or grant changes no history or ledger plans, is logged on transition and is shown in `spend.subscription.report`. `unknown_method` on watch enables full-tick-only list polling. |
| prefrontal, basal, magic-context | Attested `management_surface` of each publisher | `attribution.read` with optional opaque `after` and page `limit` | Keep the committed cursor and health unchanged on failure; retry at the next scheduled refresh, but back off per absent publisher from one minute up to ten minutes unless a bus wake arrives first. |
| entorhinal | Attested `management_surface`, module `entorhinal` | `placement.read` with optional opaque `after` and page `limit` | The same optional-reader behavior as attribution publishers. |

Fusiform, insula and ckbus use the module's daemon connection file supplied by
the supervisor, not paths in desired configuration. Their routes use the
module's daemon-validated identity after HELLO. If Fusiform is not reachable,
events wait for pricing; if insula is not reachable, capacity remains advisory.

Claustrum uses the module's daemon connection and launch identity, as colliculus
does, under `reserved:astrocyte`'s `list` grants on the `llm-provider` category or
on exact credential IDs. Astrocyte uses only `llm-provider` records. No `read`
grant or `credential.get_scoped` call is needed. The published
`cortexkit-claustrum-client` 0.9.0 decoder and watch types validate replies;
Astrocyte's SDK consumer connections use the serving connection's claim when
launched with a registration channel, and the SDK's implicit identity on older
unbound launches. The reader
re-lists after every notice and on daily full-tick backstops. Only full ticks
record its queued observations, outside all peer calls.

Colliculus uses the module's daemon connection file while its source is ingested.
The manifest declares its consumer and `astrocyte-colliculus-usage-read` self-signal
by default, omitting both when `harness_ingest` is disabled or excludes `colliculus`.
The signal describes `usage.read` as an observe poller on the refresher tick cadence;
its note records that `usage.ack` follows each committed page and retries acknowledge
the durable watermark before reading.

The manifest always declares the four attribution publishers as service clients.
Its `self_signals` also describe each periodic read as a poller with an observe
effect: the peer call reads facts rather than mutating the publisher. They run only
on scheduled full refreshes, never the shorter pricing-only cycles. Each page
commits its facts and cursor in one transaction. An empty page is one call.
Only a returned `next` is a resume token; a terminal null keeps the request
cursor and replays that suffix next refresh. A page containing only unchanged,
known facts opens no write transaction, including no cursor update. Changed
bodies for known identifiers are set aside as `fact_body_changed`, never
replaced. Malformed facts, invalid open workload/task names, and subjects already
claimed by another publisher are also set aside and counted by source in
`spend.ledger.coverage`. Attribution-fact events do not yet trigger refreshes;
only the scheduled refresh polls each publisher.
Known correction targets must have the same publisher and exact subject
(`project_id` for placements);
`supersedes_other_subject` distinguishes a bad correction from a malformed body.
Unknown targets are kept pending and checked on arrival, without deleting
immutable evidence.

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
Queued events published at or before the start of a committed `usage.get`
snapshot cannot replace it. A complete inventory also forgets event cursors
for accounts the snapshot retires.

Healthy attachment, a current credential and a recent successful pull suppress
per-tick snapshots. The backstop is the earlier of ten minutes and the existing
capacity freshness horizon minus one cadence: normally 90 seconds. Event commits
refresh capacity sooner; empty pulls never do. Any bus failure restores per-tick
polling. Transitions are logged; health reply shapes, freshness rules, the
multiplier and `budget.verdict` are unchanged.
If only astrocyte's quota-event bus consumer cannot attach, runner steps and the
other bus paths continue on the same connection while quota falls back to polling.

broca, the coding harnesses and thalamus-harness are not called. astrocyte
reads their stores and files directly; see [harness readers](harness-readers.md).

## 6. Not guaranteed

- `budget.verdict` enforces no cap. Callers must not read `allow` as approval
  of a budget.
- A timed-out write has an unknown outcome. Clients must not retry a write
  blindly; `spend.correction.ingest` is the one write with a retry key.
- Served writes carry no operator identity. The caller is whoever subc
  admitted to the route.

## Post-commit bus publications

Astrocyte publishes three additive views for readers moving from polling to the
fleet bus. The existing reports, `usage.steps.subscribe` notices, watermark
operation, acknowledgement files and all reader polls remain in place. Subjects
and bucket names come from `cortexkit-bus-naming`; Astrocyte reuses its existing
ckbus credential and NATS connection without changing the `m_astrocyte` durable
or its quota and runner-step readers. The module declares registration-channel
support. The SDK's serve helper adopts that channel, and its consumer connections
obtain a self claim from the serving registration owner. Calls do not supply a
per-call identity; the SDK retains its implicit identity behavior for older,
unbound launches.

Ledger changes are committed before their values are staged for publication.
Acknowledgement values are staged only after the corresponding atomic file
write succeeds, or after reading an unchanged file that already exists. Network
work runs independently of the writer, outside every store transaction. A bus
failure neither rolls back a commit nor changes module health. A fixed warning
is logged once per failure streak, without peer error text, credentials or
values. The next change or credential session republishes the current desired
state. These publications are hints and mirrors, not a replacement for the
ledger or a new source of billing facts.

### Live step hint

After a commit appends rows to the live step feed, Astrocyte publishes on
`ck.{acct}.live.astrocyte.steps_committed.v1`, constructed with
`AccountNames::live_subject`. Magic-context is the registered reader. The exact
body shape is `{"cursor":"c42"}`, using the newest feed cursor, not a row or a
session name. A rollback and an unchanged feed emit nothing. There is at most
one hint in flight; commits during a send replace the pending cursor with the
newest one. The live subject is Core NATS, with no replay or offline delivery.
The existing `usage.steps.subscribe` stream is unchanged.

### Multiplier KV

The `astrocyte.multiplier` bucket is written by Astrocyte and read by Striatum.
`AccountNames::module_bucket_name("astrocyte", "multiplier")` derives its
account-specific name. Values are limited to the bucket's declared size (64 KiB
today, from `cortexkit-bus-naming`), with history 1 and no TTL.
Each key identifies one `(quota_provider, plan, pool_id)` and its value is
the whole `spend.multiplier.report` `latest` row in compact JSON. The key is
`x<provider-hex>.x<plan-hex>.x<pool-hex>`: each `x` is followed by the lowercase
hexadecimal of that component's UTF-8 bytes, even for ASCII IDs. Dots only
separate the three components. All three IDs are retained in the value; the
pool component encodes the raw `pool_id`, not `window_pool_key`. To join the
row to a served subset list, use the value's `catalog_provider` and `pool_id`.

Astrocyte reconciles the bucket to the current `latest` rows after a daily
provider-day commit, after an operator recompute and at startup. Unchanged
values are not put again. Keys absent from the new set, including old
two-part keys, are deleted even when `latest` is empty. A failed day's write
does not evict that provider's older rows; a failed report read leaves the
bucket unchanged.

Values larger than the declared size are omitted without stopping publication of other
rows. An earlier value for an omitted key is deleted rather than left stale.
The warning records the key and byte count, not the value. Astrocyte never
truncates a report row to fit. An absent key means no estimate from the bucket.

### THALAMUS read-marks KV

The `astrocyte.read-marks` bucket is written by Astrocyte and read by
thalamus-harness. Its account-specific name comes from
`AccountNames::module_bucket_name("astrocyte", "read-marks")`. Values are limited
to 256 bytes, with history 1 and no TTL.

`thalamus.watermark` holds exactly the value `watermark.get` serves:
`{"watermark_ms":123}` or `{"watermark_ms":null}`. It is staged at startup and
whenever the committed effective watermark changes, including changes in the
payer watermark that bounds session progress. `watermark.get` remains available
with its existing admission rules. When ingestion is disabled or its source is
excluded, the operation stops serving a value and the key is deleted after the
configuration commit is published, including at startup. Re-enabling the source
restores its served value after the next commit; a served null watermark remains
a key, not a deletion.

For each session in a THALAMUS acknowledgement file, the key is
`thalamus.ack.<encoded source>.<encoded session>`. Source and session ids use the
same always-prefixed UTF-8 hex rule as multiplier keys; they are never treated
as paths or as already-separated key fragments. For example, source
`pi-thalamus` and session `s/_.é` map to
`thalamus.ack.x70692d7468616c616d7573.x732f5f2ec3a9`. Its value is
`{"session":"s/_.é","source":"pi-thalamus","through_ms":123}`. The readable ids
are included so readers do not need to decode a key.

The bucket mirrors each successfully written acknowledgement file's complete
session set. Removing an entry from the file revokes its key; a failed file
write changes neither the desired puts nor the desired deletions. Unchanged
files also seed their entries when first read after startup. File writes and
all source reads continue unchanged. A value above 256 bytes is omitted and any
old key is deleted, with the same key-and-size warning policy as the multiplier
bucket. It is never truncated. A missing key is not evidence that a session is
unacknowledged until THALAMUS has switched to the bucket; during migration it
must retain its acknowledgement-file read. A missing or stale bus view must
never allow deletion that the file or `watermark.get` would not allow.
