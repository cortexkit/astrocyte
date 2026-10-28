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

## Counter periods and boundaries

Use `COALESCE(raw_used_percent, used_percent)` and sort by fetched timestamp, not
arrival order. A period ends only when `resets_at` advances by **more than one
second**. Insula horizons jitter by less than a second (17:59:59.7 versus
18:00:00). Within a period count only rises above its high-water mark: rounding
dips, drops to zero and rebounds below that mark add nothing. A new period's
first observed value counts from zero, including a first reading greater than
the old period's last reading. A missing/unparseable reset horizon is not proof
of a reset and excludes the hour from calibration.
An older reset horizon arriving after a newer one (outside the same one-second
jitter tolerance) is stale cache data: exclude that sample rather than letting
an old allowance raise the current period's high-water mark. Account diagnostics
count these as `stale_reset_readings`.

Adjacent reading increases are divided across UTC hour buckets in proportion
to elapsed time, clipping both interval boundaries. This is an explicit
constant-burn approximation between samples, not exact event attribution. The
new period's first value is located at its first reading instead. No far-future
sample certifies an interval's tail: only the **15 minutes** after the end are
queried. A clean hour requires bracketing readings and no gap over 15 minutes,
including at its two boundaries. A gap never becomes clean just because there
are readings on both sides of the week.

Antigravity `vault` readings before **2026-10-20T19:08Z** are excluded. This is a
fixed historical correction: that frozen set queried Google's wrong production
quota pool, not an adjustable source preference. QTA confirms later `vault` and
`oauth` readings refer to the same counter (possibly one hour cached). Merge
those readings using the same reset/high-water rules. Claude readings have no
provenance tag; source splitting cannot recover it. An unchanged-reset drop is
always noise, including the observed zero/61 flap, never a new allowance.
Do not choose a source solely by history length: Antigravity's `vault` readings
before the fixed cutoff queried the wrong production quota pool.

## Matching numerator and denominator

A configured account-hour is clean only when fully bracketed within the gap
bound, all bracketing readings are below 100%, reset metadata is usable, and it
has no positive unpriced quantity. One positive unknown-price token is material:
premium-model quota burn need not be proportional to token count. Unknown
measurement quantities and events with no measurements are unknown usage, not
zero tokens; either makes the whole row unmeasurable.

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

New rows are per **(quota provider, plan)**. There is no new pooled provider row:
Codex's pro_100 and pro_200 empirical fits differ even after price normalization.
Historical provider rows remain unchanged; their new fields default to null
plan, false eligibility and `legacy_estimator`. Schema 35 preserves their
append-only history and gives each plan an independent supersession chain.

Money uses checked integer arithmetic. API list numerators are summed before
one half-even rounding of the row total. Independently displayed model lines
may differ from that total by rounding nanodollars; they are not the source of
the aggregate. Subscription/fee shares also round after aggregation.
`fee_paid_nanos` remains the recurring fee over the **whole observation
interval** for priced accounts of the plan, not the price of clean exposure;
`fee_multiplier` is therefore ancillary and is not the routing divisor.

## Eligibility policy

All policies are named constants in `multiplier/clean.rs`; they are deliberately
not tunable configuration. Shares use all configured account-hours in the plan
as denominator, including unobserved hours. Exclusions may overlap, so shares
need not sum to one.

| Requirement | Reason code when rejected | Rationale |
|---|---|---|
| At least 50% clean account-hours and 24 clean account-hours total | `insufficient_clean_hours` | Avoid fitting a week from a tiny selected subset; Claude had only one jointly clean active hour. |
| At most 25% saturated account-hours | `saturation` | Codex's observed 19% can pass; Claude's 40.9% cannot. Saturated exposure is censored, not free. |
| At most 25% gapped account-hours | `gaps` | Same conservative exclusion budget as saturation; clean-hour status uses the 15-minute gap bound above. |
| At most 25% unknown-price account-hours | `unpriced_usage` | Separate Spark-only hours may be useful; an unknown premium model used 43 of 73 Muse points on one day and must never be valued at zero. |
| No unknown measurement quantities | `missing_quantity` | Their size has no defensible upper bound. |
| No events without measurement rows | `missing_measurements` | Their size is unknown even when no quantity field exists to report. |
| Reset metadata present throughout observed exposure | `unknown_reset` | A percentage drop alone cannot identify a period. |
| No unconfigured observed account | `unconfigured_accounts` | Numerator/denominator membership must agree. |
| Every included plan price resolved | `unpriced_plan` | No subscription-cost denominator can be fabricated. |
| At most 5% unattributed events when plans differ | `unattributed_plan_usage` | Codex has about 2.2% unmatched events; do not assign their value to a random plan. This is an event-share budget, not a money bound. |
| At least 20 clean points | `insufficient_points` | One point of endpoint quantization is 5% of twenty points. |
| Positive finite multiplier | `nonpositive_ratio` | Missing value or zero burn is not a free routing price. |
| In-scope sources fresh and drained | `source_backlog` | A successful bounded page can leave older usage pending. |

Readiness requires `last_success_ms` at/after the interval end **and** no
`pending_known` or `pending_unknown`. Before two hours after the boundary the
operation waits; afterwards it may append a diagnostic row, never an eligible
one. Recompute rechecks the same readiness even for existing rows. Reader aliases
`opencode-v2` and `claude-code-workers` inherit their ledger producer's scope.
Sources absent from `source_runtime` are not independently certified; neither
this gate nor configuration proves that traffic outside ingestion is visible.

Rows also expose `unattributed_event_share`, `zero_measurement_events` and
`missing_quantity_events`; the event counts include excluded hours and apply
to the plan's attributed events plus provider-wide unattributed events. They
distinguish an absent quantity on a measurement from an event with no
measurements. Both null the ratios, even if the remaining clean subset has a
positive list value. The hourly quality shares alone cannot disclose that
unknown quantity population.

`spend.multiplier.report` serves all diagnostics additively. A numeric ratio
alone is not authorization: consumers must use `routing_eligible` and match the
plan and model/request mixture. Pace accepts `plan`, requires it when plans
differ, selects only eligible unsuperseded heads and requires a head ending no
more than two days before the requested end. It labels the result eligible and
estimated. It never falls back to a legacy or ineligible head. An eligible
Spark-only calibration does not supply a Wonder Full tariff or establish a
quota conversion for a different workload mixture.
