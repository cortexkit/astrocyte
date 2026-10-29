# Subscription multiplier: matched account-hours

The multiplier is descriptive calibration, not a discovered provider tariff:

```
multiplier = clean API list value / subscription cost of clean quota points
subscription cost = sum(monthly plan USD * window_minutes / 43200 * points / 100)
list USD per point = clean API list USD / clean points
```

An observation day ends a trailing seven-day UTC interval at midnight. Daily
rows overlap; never add them to recover a week's consumption. Valuation uses the
attempt's current configuration and catalog snapshot, not event-time invoice
prices. Recompute can change historical values when prices or scope change.
Quota readings and scoped events are captured together before catalog calls.
The spend ledger is not changed.

## Account and credential identities

An account-bearing quota reading always joins by `account_ref`, even when it
also carries a vault credential reference. A reading without an account can
instead use its exact `credential_ref`. Credential history is keyed by quota
provider, credential, pool and source instant; two keys for one provider never
share a counter. Readings with neither identity remain latest-only and cannot
produce a multiplier. Earlier latest-only readings cannot be reconstructed as
credential history.

Declare account plans in `accounts` or credential plans in `credentials`, not
both for one quota provider. Credential counters use the same reset, high-water,
clean-hour, saturation and stability rules described below. The report's
historically named `accounts_included` and `accounts_excluded` lists contain
`credential_ref` instead of `account_ref` for credential rows; `plan_prices`
also shows the identity actually priced. Account rows keep their existing shape.
Unconfigured credential readings are listed in the legacy-named
`unconfigured_accounts` array and reject the row with `unconfigured_credentials`.
Their exclusion diagnostics use `credential_ref` and `credential_not_configured`.

With exactly one configured credential, in-scope provider tokens are
attributed to that credential, in the clean hours only, under the same
clean-hour rules as account counters. Ledger credential labels are not
used as a quota identity: some producers omit them and their names need not
match vault references. More than one configured credential makes every plan
row ineligible with `multiple_credentials_without_token_attribution`, even when
the credentials hold the same plan. Those rows have null ratios and no tokens
assigned to an individual key or plan. Unpriced diagnostics remain visible.

OpenCode Go uses quota provider `opencodego`, its `tertiary` 43,200-minute pool,
and plan `opencode-go/go` ($10/month). Kimi for Coding uses quota provider
`kimi-for-coding`, its `primary` 10,080-minute pool, and plan
`kimi-code-plan-global/allegro` ($99/month). Plan prices are resolved from the
catalog, not hard-coded. Their token scopes use ledger providers `opencode-go`
and `kimi-code-plan-global`, with both `broca` and `opencode` producers. Go's
paid extra usage cannot be distinguished from included traffic here and would
distort its ratio; the estimate is not evidence that extra usage is included.

## Counter periods and boundaries

Use `COALESCE(raw_used_percent, used_percent)` and sort by fetched timestamp, not
arrival order. A period ends only when `resets_at` advances by **more than ten
minutes**, capped at half the configured window length for short pools.
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

Antigravity `vault` readings before **2026-10-20T19:08Z** are excluded. This is a
fixed historical correction: that frozen set queried Google's wrong production
quota pool, not an adjustable source preference. Later `vault` and `oauth`
readings sample the same counter, although readings can be cached for about
one hour. Merge those readings using the same reset/high-water rules. Claude
readings have no provenance tag; source splitting cannot recover it. An unchanged-reset drop is
treated as noise, never a new allowance. This includes a reading that briefly
drops to 0 and returns to its previous percentage with an unchanged reset time.
Do not choose a source solely by history length: Antigravity's `vault` readings
before the fixed cutoff queried the wrong production quota pool.

## Matching numerator and denominator

A configured account-hour is clean only when fully bracketed by short or
unchanged-period quiet pairs, all bracketing readings are below 100%, reset metadata is usable, and it
has no positive unpriced quantity. One positive unknown-price token is material:
premium-model quota burn need not be proportional to token count. Unknown
measurement quantities and events with no measurements are unknown usage, not
zero tokens. They exclude the matching account-hour, or every account's hour
when unattributed, just like unpriced-model usage. Each excluded account-hour
counts once in the diagnostic `unpriced_share`, not a share-based veto. The retained clean exposure can still
produce an eligible estimate. No value is fabricated for the excluded usage.

Attributed events count only in their own configured account's clean hours.
Unattributed events count only when **every configured account**, across plans,
is clean. If multiple plans exist, unattributed events are omitted instead of
inventing a plan allocation. Their omission can lower the measured value; the
eligibility gate bounds that event fraction. Quota points are summed only over
the row's plan and its clean account-hours. Unpriced diagnostics retain usage
from excluded hours so exclusions are visible, even though `tokens` and
`api_nanos` describe retained hours.

Every account with valid readings in the provider's selected pool and window
must have a configured plan. Otherwise every plan row names it in
`unconfigured_accounts`, has null ratios and is ineligible. Configured plans
without a resolved monthly price likewise have null ratios and cannot be eligible. Events with an
unknown account reference are treated as unattributed, not silently assigned to
another configured account.

New rows are per **(quota provider, plan)**. There is no new pooled provider row.
Plans can differ in capacity and usage behavior; normalizing their fees does
not make their quota points interchangeable.
Historical provider rows remain unchanged; their new fields default to null
plan, false eligibility and `legacy_estimator`. Schema 35 preserves their
append-only history and gives each plan an independent supersession chain.
In the report, `latest` is ordered by quota provider and plan. A legacy pooled
head appears there only while its provider has no plan-specific observation;
`series` retains both legacy and plan-specific heads in the requested range.

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
not tunable configuration. Shares use all configured account-hours in the plan
as denominator, including unobserved hours. Exclusions may overlap, so shares
need not sum to one.

| Requirement | Reason code when rejected | Rationale |
|---|---|---|
| At least 24 clean account-hours total | `insufficient_clean_hours` | Require a minimum matched sample, not a fixed coverage share. |
| At most 25% saturated account-hours | `saturation` | Capped periods select heavy-use traffic, so excluding too much can bias the estimate. Saturated exposure is censored, not free. |
| Reset metadata present throughout observed exposure | `unknown_reset` | A percentage drop alone cannot identify a period. |
| No unconfigured observed account | `unconfigured_accounts` | Numerator/denominator membership must agree. |
| Every included plan price resolved | `unpriced_plan` | No subscription-cost denominator can be fabricated. |
| At most 5% unattributed events when plans differ | `unattributed_plan_usage` | Do not assign unattributed events to a guessed plan. This is an event-share budget, not a money bound. |
| At least 20 clean points | `insufficient_points` | One point of endpoint quantization is 5% of twenty points. |
| At least 3 active UTC days | `insufficient_stability_days` | Leave-one-day-out evaluation must retain at least two training days. |
| Conditional day-block 95% interval width at most 50% of pooled USD/point | `unstable_estimate` | Limit uncertainty in the retained estimate rather than requiring a fixed coverage share. |
| Positive finite multiplier | `nonpositive_ratio` | Missing value or zero burn is not a free routing price. |
| In-scope sources fresh and drained | `source_backlog` | A successful bounded page can leave older usage pending. |

There is no clean-, gap- or unknown-share veto. Excluding hours from both
value and points reduces sample size but does not itself prove bias. The
minimum sample and measured stability now govern adequacy. `clean_hours_share`
and `gap_share` remain diagnostics, as does `unpriced_share`. Unpriced models
still exclude their hours and are never valued at zero. Saturation keeps its
25% veto: capped periods select the heaviest-use traffic, so their exclusion
can bias the retained estimate even when it is numerically stable.

### How the thresholds were chosen

On one development store in October 2026:

- Reset times within one quota period ranged by up to 312 seconds; consecutive
  changes reached 61 seconds, while separate periods were at least 85,737
  seconds apart. Ten minutes accommodates that jitter without merging those
  periods; the half-window cap preserves short-window rollovers.
- Saturation occupied 19% of Codex account-time and 40.9% of Claude account-time,
  motivating a 25% limit on heavy-use censoring.
- About 2.2% of Codex events lacked a matched account, supporting a 5% omission
  budget without inventing a plan allocation.
- Conditional interval widths were about 10–15% for pro_200 measurements and
  about 62–100% for pro_100, Claude and Muse measurements. A 50% width budget
  separated those stable and variable estimates on that store.

These are dated observations, not universal provider guarantees. To compare
another store, use seven-day UTC windows and inspect `window_sample` reset
times, event/account matches, and the shares and stability diagnostics from
`spend.multiplier.report`. Validation holds out each UTC day and fits on the
remaining days, as described below.

### Stability and held-out-day validation

The calculation retains exact rate-times-token numerators, points and clean
hours in six-hour bins from the same owned snapshot as the observation. Codex
plan validation is account-matched; a single configured account uses its
provider scope conditionally, and unattributed multi-account single-plan usage
uses provider/plan bins without inventing account assignment. Empty zero/zero
bins do not inflate fit metrics; zero-point bins with list value remain.

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
JSON diagnostics, explicitly shared at plan level; no table migration or guessed
allocation of provider-wide value is needed. `account_slot` identifies matched
bins in sorted configured plan-member order. Exact numerators are decimal
strings so JSON integer limits and per-bin rounding cannot alter row money.
Legacy observations without those inputs report null stability diagnostics.

Readiness requires `last_success_ms` at/after the interval end **and** no
`pending_known` or `pending_unknown`. Before two hours after the boundary the
operation waits; afterwards it may append a diagnostic row, never an eligible
one. Recompute rechecks the same readiness even for existing rows. Reader aliases
`opencode-v2` and `claude-code-workers` inherit their ledger producer's scope.
Sources absent from `source_runtime` are not independently certified; neither
this gate nor configuration proves that traffic outside ingestion is visible.

Rows also expose `unattributed_event_share`, `zero_measurement_events` and
`missing_quantity_events`. The report additionally derives
`unknown_quantity_measurements` from the persisted missing-quantity diagnostics;
several missing measurements in one event count separately there. Event and
measurement counts include excluded hours and apply to the plan's attributed
events plus provider-wide unattributed events. They distinguish an absent
quantity from an event with no measurements. Nonzero counts are compatible with
an eligible row when its retained sample is adequate and stable; neither counts
nor unknown-hour share are vetoes. No schema change is needed for the derived report count.

`spend.multiplier.report` serves all diagnostics additively. A numeric ratio
alone is not authorization: consumers must use `routing_eligible` and match the
plan and model/request mixture. Pace accepts `plan`, requires it when plans
differ, selects only eligible unsuperseded heads and requires a head ending no
more than two days before the requested end. It labels the result eligible and
estimated. It never falls back to a legacy or ineligible head. An eligible
Spark-only calibration does not supply a Wonder Full tariff or establish a
quota conversion for a different workload mixture.
