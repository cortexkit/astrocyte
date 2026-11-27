# Subscription multiplier: matched account-hours

The multiplier is descriptive calibration, not a discovered provider tariff:

```
multiplier = clean API list value / subscription cost of clean quota points
subscription cost = sum(monthly plan USD * window_minutes / 43200 * points / 100)
list USD per point = clean API list USD / clean points
```

An observation day ends a trailing seven-day UTC interval at midnight. Daily
rows overlap; never add them to recover a week's consumption. List valuation
uses catalog rates, not recorded charges. The readings, calls, measurements
and vault plan history for a provider day are captured together before catalog
calls; late inputs affect that day only on recompute. The spend ledger is not
changed. Coverage and pricing do not come from configuration: the only
configuration members are `poll_interval_secs` and `harness_ingest`.

## Account and credential identities

An account-bearing quota reading always joins by `account_ref`, even when it
also carries a vault credential reference. A reading without an account can
instead use its exact `credential_ref`. Credential history is keyed by quota
provider, credential, pool and source instant; two keys for one provider never
share a counter. Readings with neither identity remain latest-only and cannot
produce a multiplier. Earlier latest-only readings cannot be reconstructed as
credential history.

Plans come from `vault_plan_history`. A quota-reading pair counts toward a plan
only if the login holds that plan at both readings. A call counts toward a plan
only if its current charge basis is `subscription_included` and its login held
the plan at the call's instant. The call joins by account, or when there is no
account, by paying credential (resolved credential first, then selected
credential). No usable login or no plan means the call counts toward no pool.
There are no producer, provider or model-prefix filters for plan attribution.
The insula quota provider's name need not match the vault or catalog provider.

Credential counters use the same reset, high-water, clean-hour, saturation
and stability rules as account counters. The historically named
`accounts_included` and `accounts_excluded` lists use `credential_ref` for
credential-keyed logins, and `plan_prices` shows the identity actually priced.

The vault reader uses only `credential.list_scoped` and `auth.watch_scoped`
under the reserved module's `list` grants on the `llm-provider` category or on
exact credential IDs. It uses only `llm-provider` records and never fetches
secrets. It lists at startup, after every notice and daily as a backstop.
`unknown_method` on the watch switches to polling on full ticks only. Network
work runs outside startup and recording transactions, and a notice's new plan
applies on the next full tick. The key is exactly `<provider_ids[0]>/<plan_tier>`
when exactly one provider and a tier are present. Otherwise it is unknown.
Effective time uses `plan_tier_since_ms` when it is a non-negative integer no
later than the row's `plan_tier_observed_at_ms` (when that timestamp is present).
Otherwise it uses `plan_tier_observed_at_ms`, falling back to the first observation
of a change when neither time is usable. Invalid start dates are ignored and
reported by `spend.subscription.report` in the optional `invalid_plan_tier_since`
diagnostic. Without a valid explicit start date, an unchanged plan retains its
previous effective time even if the vault re-stamps its observation. Repeated
lists with the same plan and start append nothing; an explicit date-only edit
appends a new history row without rewriting earlier evidence.

For each credential, a snapshot uses the last recorded history row whose start
is before the snapshot's exclusive end. Rows replace earlier recorded evidence
from their own start onward, even when a correction moves the start backwards
across an older row's start. A future-start row does not change earlier snapshots.
The first known plan applies from the login's first recorded call. The lookup
seeks backwards in recording order and stops at the first eligible row,
skipping future-start rows if needed. Date-only edits to an open,
vault-owned ledger plan with the same key leave its fee interval unchanged.
A valid changed catalog fee for the same key starts a successor at the catalog
boundary if it follows the open interval's start. The earlier interval keeps
its fee. Plan history determines the multiplier's tier; ledger intervals
record subscription costs.

Ledger reconciliation runs once after the first successful startup list, when
a snapshot appends history or first confirms a credential-only seed without a
start date, and on a scheduled retry. Unpriced keys retry at most hourly;
otherwise the backstop is daily. Only those reconciles ask
Fusiform for prices, and only ledger changes enter a writer transaction.
Diagnostics are cached between reconciles. ChatGPT mismatches refresh on a
changed snapshot or daily; their query is not part of an idle tick or an hourly price retry.

## Served pool coverage

Insula's `usage.get` supplies each window's `modelScope`. `all` covers every
call counted toward that login's plan. `subset` covers the models in the
Fusiform `plan.prices` reply's `quota_pools` list. The lookup uses the catalog
provider agreed on by the pool's readings and the pool ID. The ID is the
insula slot name or the raw extra-window ID. For example, an extra window
stored as `extra:5:Fable` has pool ID `Fable`. An extra window with no ID is
known only by its position in an array, so it cannot produce a row or compete
with other windows. Plan prices and subset lists are requested as of the UTC
midnight ending the observation day, including on recompute.

A subset matches the call's own catalog model ID: try an exact row, then
remove a trailing `-low` or `-high` if the row is missing, then try an alias.
An unresolved model belongs only to `all` pools. Membership ignores a
`quota_proxy` price link, through which a plan's model borrows another
model's list prices, and does not depend on whether the rates are usable. A listed
model with unusable rates is still a member, and its unpriced usage enters
that pool's diagnostics. List valuation starts from the call's own provider
and follows catalog references rather than a configured provider alias.
Reasoning is priced at the selected output rate only when the served reasoning
rule says so; otherwise it needs a reasoning rate and is unpriced without one.

For each plan and distinct coverage, only the longest window with known
coverage and one consistent duration is measured. Model lists are compared
as sets. Two equally long candidates for the same coverage both get
`window_tie` rows instead of choosing one. Different coverage can overlap:
a call can count in both an all-models pool and a narrower pool. Excluding
the call's login from one pool does not exclude it from a sibling pool.

A window with no stated duration anywhere gets no row. Missing or differing
scopes, inconsistent durations, and missing or empty subset lists produce
diagnostic rows instead of measurements. A subset list is also treated as
missing when any reading lacks the catalog provider or the readings disagree
about it. A `plan.prices` reply without `quota_pools` leaves subset lists
missing but does not prevent measurement of `all` pools.

For a pre-migration-68 reading with no scope, use the same login's first
post-migration reading for that quota provider and pool, even if it too has no
scope. If the login has no post-migration reading for the pool, use each peer
login's first stated post-migration scope for the same quota provider, plan,
and pool. Only peers that state a value count, and they must agree. For a
missing catalog provider, use the same login's first later stated provider for
the same plan and pool. If there is none, look for unanimous providers among
the peers' first stated readings that are both later and post-migration.
Stated values are never overwritten. Donor readings can be outside the
interval; they supply metadata only, not counter readings or additional logins
to measure. The [data model](data-model.md#quota-readings-and-multiplier-observations)
describes how readings are stored.
See the [module surface](module-surface.md) for the row members and reason codes.

## Counter periods and boundaries

Use `COALESCE(raw_used_percent, used_percent)` and sort by fetched timestamp, not
arrival order. A period ends only when `resets_at` advances by **more than ten
minutes**, capped at half the stated window length for short pools.
Codex recomputes its horizon on each read, so small timestamp changes do not
necessarily mean a new allowance. The tolerance prevents jitter from inventing
a rollover or making normal readings appear stale, while preserving genuine
weekly rollovers. Within a period count only rises above its high-water mark:
rounding dips, drops to zero and rebounds below that mark add nothing. A new
period's first observed value counts from zero, including a first reading greater than
the old period's last reading. A missing/unparseable reset horizon is not proof
of a reset and excludes the hour from calibration.
An older reset horizon arriving after a newer one (outside that same
jitter tolerance) is stale cache data: exclude that sample rather than letting
an old allowance raise the current period's high-water mark. Account diagnostics
count these as `stale_reset_readings`.

Adjacent reading increases are divided across UTC hour buckets in proportion
to elapsed time, clipping both interval boundaries. This is an explicit
constant-burn approximation between samples, not exact event attribution. The
new period's first value is located at its first reading instead. A changed
pair more than **15 minutes** apart is unknown: its hours and interpolated
points are excluded. Equal percentage endpoints in the same known period,
within reset tolerance, instead certify quiet time even across a longer gap.
Saturated quiet time remains unclean; missing reset metadata cannot certify
quiet time. The scan reads through 15 minutes after the end plus one later
candidate. A later changed value/reset cannot certify the tail; an unchanged
quiet pair can. Both boundaries are clipped to the requested interval.

This reflects event-driven ingestion: insula publishes on change, and the
backstop reads at most ten minutes apart (or earlier for freshness). History
deduplicates repeated **fetched instants**, not equal percentages. A backstop
returning the same cached source instant therefore creates no new sample.
Equal values at distinct fetched instants remain separate rows. Quiet periods
must not be mistaken for missing consumption just because the history is sparse.

Antigravity `vault` readings before **2026-10-20T19:08Z** are excluded because
they measured a different quota pool. This cutoff is not configurable. Later
`vault` and `oauth` readings sample the same counter, although they can be
cached for about one hour. Merge them using the same reset/high-water rules. Claude
readings have no provenance tag; source splitting cannot recover it. An unchanged-reset drop is
treated as noise, never a new allowance. This includes a reading that briefly
drops to 0 and returns to its previous percentage with an unchanged reset time.

## Matching numerator and denominator

A login-hour is clean only when fully bracketed by short or unchanged-period
quiet pairs, all bracketing readings are below 100%, reset metadata is usable,
and its covered calls have no positive unpriced quantity. One positive
unknown-price token is material: premium-model quota burn need not be
proportional to token count. Missing quantities and events with no measurements
are unknown usage, not zero tokens. Like other unpriced usage, they exclude
only the corresponding login-hour in this pool. Each such hour counts once
in `unpriced_share`; the retained clean exposure can still yield an eligible
estimate. No value is fabricated for the excluded usage.

Each row measures one **(quota provider, plan, pool)**. Its numerator and
denominator come from the same logins and clean hours. A call can contribute
only if its login's readings for that pool enter the denominator. The report
retains unpriced diagnostics from excluded hours even though `tokens` and
`api_nanos` describe retained hours. Readings without a vault plan do not
veto other logins' rows. If no readings qualify for any plan, the provider
day is recorded with no rows. A known plan with no resolved monthly price is
ineligible and has null ratios. Normalizing fees does not make different
plans' quota points interchangeable.

The report and bucket use each provider's newest recorded day, even when that
day contains only unmeasured rows or has no rows at all. A recompute reconciles
that day's pool set by appending rows and retiring heads for dropped pools;
it never rewrites older observations. See the [module surface](module-surface.md)
for how `latest`, `series` and `last_eligible` handle historical rows.

For account-bearing plans the same vault history creates ledger coverage from
the account's first recorded call, using fusiform's sourced recurring price.
Ledger plans retain any representable sourced currency and monthly or annual
period, including a zero price; the multiplier's USD/month pricing restrictions
remain unchanged. Decimal scales are normalized only when the amount is exact.
Tier changes bound vault-owned intervals and open successors at the change
time. A hand-recorded plan is never closed or changed. Missing prices create
no new ledger plan and remain visible, with raw provider/tier and account IDs,
in `spend.subscription.report`; a later price is picked up by the hourly retry.
If one plan cannot be changed, such as a backdated tier whose change time
precedes the open plan, its ledger interval stays unchanged and other plans
still reconcile. The report lists the refusal as `reconciliation_refusals`
with `provider`, `identity`, `ledger` (`account` or `credential`) and `code`.
That report also shows hand/vault differences, disagreements between vault
records, ChatGPT credential/account mismatches and vault-read status. Existing
coverage backfill covers waiting calls; the multiplier itself never changes
recorded ledger money.

A vault credential with no account id, such as an API key, gets the same kind
of ledger plan keyed by `(provider, credential id)` in `credential_subscription`,
with the same start rule, close-on-change, hand-plan protection and pricing.
It covers subscription calls that record no account and whose paying
credential, `resolved_credential` when recorded and otherwise
`auth_selection`, equals the credential id exactly; past calls are re-covered
by the coverage backfill. Calls with neither fact stay uncovered: a credential
is never guessed from the provider. Config seeds stay skipped, with one
exception. When the vault states a seeded credential's tier with no start
date, the listing appends no history row because the plan key and start are
unchanged, so the seed remains the newest row. If the vault's latest listing
states the same plan key for that credential, reconciliation treats the seed as
vault-backed and opens the credential's first ledger plan at its first recorded
call. A seed the vault does not confirm creates nothing.

For a producer whose event is one call, the multiplier sums that call's input,
cache-read and cache-write tokens to test context thresholds. Each priced role
selects its own highest `above_context` threshold strictly below that prompt
size, considering only that role's rate rows. A tier on another role cannot
hide a lower applicable tier or force a role off its base rate. Mode and
cache-write lifetime conditions still require their complete matching rate;
they do not fall back to a less-specific condition set. An unknown prompt on a
tiered model is unpriced. Bundled events keep base rates and the existing
`base_rate_tier_untested_tokens` diagnostic because their per-call prompt sizes
are unknown. The `tiered_tokens` diagnostic remains event-level: it counts all
tokens in a per-call event whenever its prompt crosses any model threshold,
including roles priced at base or left unpriced.

Money uses checked integer arithmetic. API list numerators are summed before
one half-even rounding of the row total. Independently displayed model lines
may differ from that total by rounding nanodollars; they are not the source of
the aggregate. Subscription/fee shares also round after aggregation.
`fee_paid_nanos` remains the recurring fee over the **whole observation
interval** for priced accounts of the plan, not the price of clean exposure;
`fee_multiplier` is therefore ancillary and is not the routing divisor.

## Eligibility policy

All policies are named constants in `multiplier/clean.rs` and
`multiplier/calibration.rs`; they are deliberately
not tunable configuration. Shares use all hours for the pool's priced logins
as denominator, including unobserved hours. Exclusions may overlap, so shares
need not sum to one.

| Requirement | Reason code when rejected | Rationale |
|---|---|---|
| At least 24 clean account-hours total | `insufficient_clean_hours` | Require a minimum matched sample, not a fixed coverage share. |
| At most 25% saturated account-hours | `saturation` | Capped periods select heavy-use traffic, so excluding too much can bias the estimate. Saturated exposure is censored, not free. |
| Reset metadata present throughout observed exposure | `unknown_reset` | A percentage drop alone cannot identify a period. |
| Every included plan price resolved | `unpriced_plan` | No subscription-cost denominator can be fabricated. |
| At least 20 clean points | `insufficient_points` | One point of endpoint quantization is 5% of twenty points. |
| At least 3 active UTC days | `insufficient_stability_days` | Leave-one-day-out evaluation must retain at least two training days. |
| Conditional day-block 95% interval width at most 50% of pooled USD/point | `unstable_estimate` | Limit uncertainty in the retained estimate rather than requiring a fixed coverage share. |
| Positive finite multiplier | `nonpositive_ratio` | Missing value or zero burn is not a free routing price. |
| Tracked sources fresh and drained | `source_backlog` | A successful bounded page can leave older usage pending. |

There is no clean-, gap- or unknown-share veto. Excluding hours from both
value and points reduces sample size but does not itself prove bias. The
minimum sample and measured stability now govern adequacy. `clean_hours_share`
and `gap_share` remain diagnostics, as does `unpriced_share`. Unpriced models
still exclude their hours and are never valued at zero. Saturation keeps its
25% veto: capped periods select the heaviest-use traffic, so their exclusion
can bias the retained estimate even when it is numerically stable.

### Stability and held-out-day validation

The calculation retains exact rate-times-token numerators, points and clean
hours in six-hour bins from the same captured inputs as the observation.
Each bin sums matched login-hour usage and quota points for the pool. Empty
zero/zero bins do not inflate fit metrics; zero-point bins with list value remain.

Three hundred deterministic resamples draw whole UTC-day blocks, preserving
all accounts and bins of a day together. Each draw uses a ratio of sums, not an
average of per-bin ratios. The 2.5th–97.5th percentile span divided by pooled
USD/point is the width gate. This is conditional sample uncertainty, not a
confidence guarantee covering invisible clients, selection bias or a new task
mixture. The fixed seed and resampling method are recorded with the inputs.

For validation, each UTC day is held out; the ratio of training points to list
value predicts that day's six-hour bins. RMSE, centered R² and an OLS cross-check
are reported. No held-out value trains its own coefficient. Validation uses
held-out UTC days, not the seven highly overlapping rolling-week ratios.

`stability_diagnostics` serves the recorded summary. Its immutable inputs and
summary are retained as `plan_calibration` alongside each included account's
JSON diagnostics, shared by the row's included logins. Exact numerators are
decimal strings so JSON integer limits and per-bin rounding cannot alter row money.
Legacy observations without those inputs report null stability diagnostics.

For pools that can be measured, every tracked source must have
`last_success_ms` at/after the interval end **and** no `pending_known` or
`pending_unknown` backlog. While a source is stale or still draining, the
daily stage retries until two hours after the later of the interval end and
process start. After that deadline it may append a routing-ineligible row
with `source_backlog` so a failing source does not leave a permanent gap.
Operator recompute uses the same readiness check but counts two hours from
the interval end alone. The daily stage also retries while a needed model's
rate timeline is not yet cached. Sources absent from `source_runtime` are not
independently certified; this gate does not prove that traffic outside
ingestion is visible.

Rows expose `zero_measurement_events` and `missing_quantity_events`. The
historical `unattributed_event_share` field is zero on new rows because calls
without a usable login and plan are not admitted. The report derives
`unknown_quantity_measurements` from the persisted missing-quantity diagnostics;
several missing measurements in one event count separately there. Event and
measurement counts include excluded hours and apply to the pool's attributed
events. They distinguish an absent
quantity from an event with no measurements. Nonzero counts are compatible with
an eligible row when its retained sample is adequate and stable; neither counts
nor unknown-hour share are vetoes. No schema change is needed for the derived report count.

`spend.multiplier.report` serves the diagnostics. A numeric ratio alone is not
authorization: consumers must use `routing_eligible` and match the plan, pool
and model/request mixture. Pace reads the recorded coverage and rates from an
eligible, unsuperseded pool head, without contacting the catalog. It requires
`plan` or `pool` when bounded candidates are ambiguous at that selection step.
The [pace contract](module-surface.md) describes the time bound and the limit
on offline subset membership. An estimate for one workload is not a tariff
or a quota conversion for a different workload mixture.
