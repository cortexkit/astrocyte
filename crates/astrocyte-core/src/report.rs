//! Astrocyte v2 read model.
//!
//! Reads use only durable v2 facts. Every aggregate publishes its population,
//! and an unavailable operational signal is declared rather than defaulted.
//!
//! Multi-query reads require a caller-owned transaction. Keep every SELECT in
//! the same read snapshot so a concurrent commit cannot mix ledger generations.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

use chrono::{DateTime, Datelike, Duration, Months, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use rusqlite::{params, Connection, OptionalExtension};

mod hourly;
pub use hourly::{SummaryMaintenance, SummaryOutcome};

pub(crate) fn summary_verdict(
    conn: &Connection,
    request: &crate::verdict::VerdictRequest,
) -> Option<crate::verdict::VerdictResponse> {
    hourly::verdict(conn, request).ok().flatten()
}
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::capacity::CapacityWindow;
use crate::fact::{ChargeBasis, Measurement, UsageEvent};
use crate::subscription::{recurring_cost_nanos, SubscriptionPeriod};

const INGEST_FRESHNESS_REASON: &str = "producer_relationship_state_unavailable";

// On the 6.5 GiB store copy, a 2 GiB mapping and 64 MiB page cache cut the
// release parity harness's subscription scan from 10.7s to 5.3s and the
// month spend scan from 106.5s to 95.6s under heavy host load. Mapping is
// capped at bundled SQLite's 0x7fff0000-byte SQLITE_MAX_MMAP_SIZE; the
// negative cache value requests KiB independent of database page size. mmap
// can fault if another process truncates the main database file underneath
// the mapping. The store's migrations delete rows but do not VACUUM; no live
// store path runs VACUUM or truncates the main file. Keep offline compaction
// away from a running reader (a truncated WAL is not the mapped main file).
pub const REPORT_MMAP_BYTES: i64 = 0x7fff0000;
pub const REPORT_CACHE_KIB: i64 = -65_536;

/// Tune only a read-only report connection, never the ingest writer.
pub fn configure_report_reader(conn: &Connection) -> rusqlite::Result<()> {
    for (name, wanted) in [
        ("mmap_size", REPORT_MMAP_BYTES),
        ("cache_size", REPORT_CACHE_KIB),
    ] {
        conn.pragma_update(None, name, wanted)?;
        let actual: i64 = conn.pragma_query_value(None, name, |row| row.get(0))?;
        if actual != wanted {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                Some(format!(
                    "report reader {name}: requested {wanted}, got {actual}"
                )),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportRequest {
    pub grouping: Vec<String>,
    pub grain: String,
    pub timezone: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Grouping {
    Provider,
    Model,
    Account,
    AuthSelection,
    ChargeBasis,
    WorkClass,
    Project,
    /// Read by the work-class report only. `parse` never yields it, so a
    /// spend report cannot be grouped by producer.
    Producer,
}

impl Grouping {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "provider" => Some(Self::Provider),
            "model" => Some(Self::Model),
            "account" => Some(Self::Account),
            "auth_selection" => Some(Self::AuthSelection),
            "charge_basis" => Some(Self::ChargeBasis),
            "work_class" => Some(Self::WorkClass),
            "project" => Some(Self::Project),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::Model => "model",
            Self::Account => "account",
            Self::AuthSelection => "auth_selection",
            Self::ChargeBasis => "charge_basis",
            Self::WorkClass => "work_class",
            Self::Project => "project",
            Self::Producer => "producer",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Grain {
    Day,
    Week,
    Month,
}

impl Grain {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "day" => Some(Self::Day),
            "week" => Some(Self::Week),
            "month" => Some(Self::Month),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Availability {
    Unavailable,
    Available,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IngestFreshness {
    pub status: Availability,
    /// Present only when `status` is `Unavailable`, naming why.
    pub reason: Option<&'static str>,
    /// Per-producer state, one row per configured source. EMPTY MEANS NEVER
    /// DRAINED, which is why `status` is Unavailable rather than Available with
    /// zero lag: a producer nobody has contacted is not a producer we are
    /// current with, and reporting zero would assert agreement we never
    /// established.
    pub producers: Vec<ProducerFreshness>,
}

/// One producer's drain state, read from `source_runtime` (ruling 141).
///
/// This is operational state about a PRODUCER RELATIONSHIP, not fact identity:
/// it lives per-producer rather than on any event, so cursor coordinates do
/// not belong on the fact table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProducerFreshness {
    pub source_key: String,
    pub source_module: String,
    pub last_success_ms: Option<i64>,
    /// `consumed` and `high_water` are the producer's own coordinates, carried
    /// verbatim. `lag` is their difference and is NULL when either is absent —
    /// never zero, because an unknown position is not a caught-up one.
    pub lineage: Option<String>,
    pub consumed_ordinal: Option<i64>,
    pub high_water_ordinal: Option<i64>,
    pub lag: Option<i64>,
    pub pending_known: i64,
    /// True when the producer could not state how much it holds. A reader must
    /// not treat `pending_known` as a total while this is set.
    pub pending_unknown: bool,
}

/// Reads per-producer drain state. AN EMPTY TABLE YIELDS THE UNAVAILABLE
/// DEFAULT rather than an Available block with no rows: absence means never
/// drained, and the two are opposite claims (ruling 141).
fn load_ingest_freshness(conn: &Connection) -> Result<IngestFreshness, ReportError> {
    let mut stmt = conn.prepare(
        "SELECT source_key,source_module,last_success_ms,lineage,consumed_ordinal,
                high_water_ordinal,pending_known,pending_unknown
         FROM source_runtime ORDER BY source_key",
    )?;
    let producers = stmt
        .query_map([], |row| {
            let consumed: Option<i64> = row.get(4)?;
            let high_water: Option<i64> = row.get(5)?;
            Ok(ProducerFreshness {
                source_key: row.get(0)?,
                source_module: row.get(1)?,
                last_success_ms: row.get(2)?,
                lineage: row.get(3)?,
                consumed_ordinal: consumed,
                high_water_ordinal: high_water,
                lag: match (consumed, high_water) {
                    (Some(consumed), Some(high_water)) => high_water.checked_sub(consumed),
                    _ => None,
                },
                pending_known: row.get(6)?,
                pending_unknown: row.get::<_, i64>(7)? != 0,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // ROW PRESENCE IS NOT THE DISCRIMINATOR. The v2 migration seeds one
    // `source_runtime` row per configured producer with every coordinate NULL, so
    // "no rows" is unreachable and a presence check would report Available for a
    // producer nobody has ever contacted -- the exact zero-lag claim ruling 141
    // forbids. Freshness is available only once every configured producer row
    // has actually reported a successful drain.
    if producers.is_empty()
        || producers
            .iter()
            .any(|producer| producer.last_success_ms.is_none())
    {
        return Ok(IngestFreshness {
            status: Availability::Unavailable,
            reason: Some(INGEST_FRESHNESS_REASON),
            producers,
        });
    }
    Ok(IngestFreshness {
        status: Availability::Available,
        reason: None,
        producers,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RateStaleness {
    pub as_of_ms: i64,
    pub oldest_resolved_at_ms: Option<i64>,
    pub age_ms: Option<i64>,
    pub priced_measurements: u64,
    pub measurements_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UndatedCoverage {
    pub events_undated: u64,
    pub events_report_total: u64,
    pub reasons: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SpendRow {
    pub bucket_start_ms: i64,
    pub bucket_end_ms: i64,
    #[serde(flatten)]
    pub grouping: BTreeMap<String, Option<String>>,
    pub currency: Option<String>,
    /// Catalog value after signed event adjustments; subscription-covered
    /// usage contributes its recorded zero. This equals money charged only
    /// when no account-level arrangement discounts metered API traffic.
    pub total_nanos: i64,
    /// The portion of `total_nanos` from events whose charge basis is
    /// `metered_api`, which is the portion an account-level arrangement can
    /// discount. It agrees with money charged only when no such arrangement
    /// exists. For example, one OpenAI usage export reconciled with this
    /// ledger to 22 requests, 799,750 input tokens, 711,168 cached input
    /// tokens, and 111 output tokens while the provider's cost export was
    /// exactly zero because the organization was on an incentivized tier.
    pub metered_nanos: i64,
    pub events_total: u64,
    pub events_priced: u64,
    pub events_unpriced: u64,
    pub cost_sample_n: u64,
    pub zero_measurement_events: u64,
    /// How many measurements the events in this row hold in total. It is the
    /// denominator of the per-measurement breakdown below, which counts a
    /// subset of these same measurements.
    pub measurements_total: u64,
    /// WHY EVENTS WERE EXCLUDED, one entry per excluded EVENT. Counts events,
    /// out of `events_total`, and sums to `events_unpriced`. An event with two
    /// blocking causes is one event and holds one key naming both.
    pub unpriced_events_by_reason: BTreeMap<String, u64>,
    /// WHICH MEASUREMENTS A PRICING PASS DECLINED, one entry per MEASUREMENT,
    /// out of `measurements_total`. A DIFFERENT DENOMINATOR from the map above
    /// and never a subtotal of it: a declined measurement of zero units leaves
    /// its event's total exact, so the event is priced and counted nowhere
    /// above while its reason is counted here. This is the only place a
    /// catalog gap under a zero quantity is visible.
    pub unpriced_measurements_by_reason: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SpendTotal {
    pub currency: Option<String>,
    /// Catalog value after signed event adjustments; subscription-covered
    /// usage contributes its recorded zero. This equals money charged only
    /// when no account-level arrangement discounts metered API traffic.
    pub total_nanos: i64,
    /// The portion of `total_nanos` from events whose charge basis is
    /// `metered_api`, which is the portion an account-level arrangement can
    /// discount. It agrees with money charged only when no such arrangement
    /// exists. For example, one OpenAI usage export reconciled with this
    /// ledger to 22 requests, 799,750 input tokens, 711,168 cached input
    /// tokens, and 111 output tokens while the provider's cost export was
    /// exactly zero because the organization was on an incentivized tier.
    pub metered_nanos: i64,
    pub events_total: u64,
    pub events_priced: u64,
    pub events_unpriced: u64,
    pub cost_sample_n: u64,
    pub zero_measurement_events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SpendReport {
    pub totals: Vec<SpendTotal>,
    pub rows: Vec<SpendRow>,
    pub undated: UndatedCoverage,
    pub ingest_freshness: IngestFreshness,
    pub rate_staleness: RateStaleness,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hierarchy_resolution: Option<Availability>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportRefusal {
    UnknownGrouping(String),
    DuplicateGrouping(String),
    UnknownGrain(String),
    UnknownTimezone(String),
    MissingEventId,
    EventNotFound(String),
}

impl ReportRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnknownGrouping(_) | Self::DuplicateGrouping(_) => "invalid_grouping",
            Self::UnknownGrain(_) => "invalid_grain",
            Self::UnknownTimezone(_) => "invalid_timezone",
            Self::MissingEventId => "malformed",
            Self::EventNotFound(_) => "not_found",
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::UnknownGrouping(value) => format!("unknown grouping dimension '{value}'"),
            Self::DuplicateGrouping(value) => format!("duplicate grouping dimension '{value}'"),
            Self::UnknownGrain(value) => format!("unknown report grain '{value}'"),
            Self::UnknownTimezone(value) => format!("unknown IANA timezone '{value}'"),
            Self::MissingEventId => "event_id is required".into(),
            Self::EventNotFound(event_id) => format!("event '{event_id}' was not found"),
        }
    }
}

#[derive(Debug)]
pub enum ReportError {
    Refused(ReportRefusal),
    Store(rusqlite::Error),
    Arithmetic,
    Calendar,
    /// A stored row or an in-memory ledger relationship violates an invariant.
    Integrity(&'static str),
}

impl From<rusqlite::Error> for ReportError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error)
    }
}

/// True when a measurement leaves its event's total UNKNOWN.
///
/// `head_outcome` is the outcome of the measurement's current charge head, or
/// `None` when no pricing pass has examined it. `stated_zero` is true only when
/// the producer stated a quantity of exactly zero.
///
/// A declined measurement of zero stated units contributes exactly zero
/// whatever rate nobody served, so the event's total stays exact and the
/// declined measurement must not drag the event's verdict down. The three
/// quantity states stay apart on purpose:
///
///   - a stated zero is arithmetically determined and does not block;
///   - a stated non-zero has an unknown contribution and blocks;
///   - NO STATED QUANTITY IS NOT A ZERO. A producer that never said how
///     much it used has said nothing about the size, so it blocks.
///     Defaulting the absent value to zero here would report an
///     unmeasured measurement as a free one.
///
/// A measurement no pass has examined blocks too: never examined is a
/// different claim from examined and declined, and no quantity makes an
/// unasked question answered.
fn blocks_event_total(head_outcome: Option<&str>, stated_zero: bool) -> bool {
    match head_outcome {
        Some("priced" | "covered") => false,
        Some(_) => !stated_zero,
        None => true,
    }
}

/// The label a declining head contributes to the unpriced breakdowns. A head
/// that declined without naming a reason reports the bare `"unpriced"` the
/// event-level breakdown has always used for that case.
fn declined_reason(unpriced_reason: Option<&str>) -> &str {
    unpriced_reason.unwrap_or("unpriced")
}

/// Number of `Grouping` dimensions, including the producer dimension only the
/// work-class report reads; `Grouping` values index arrays of this length.
const GROUPING_DIMENSIONS: usize = Grouping::Producer as usize + 1;

/// A fast non-cryptographic hasher for the in-memory joins of the ledger-wide
/// pass, which hash every charge and event id in the store. The keys are the
/// store's own ids read by a local report, so the flooding resistance of the
/// default SipHash buys nothing here, while its cost was a large share of the
/// pass. The mixing step is the one rustc's own hash tables use.
#[derive(Debug, Default, Clone, Copy)]
struct FastHasher(u64);

impl FastHasher {
    fn add(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
}

impl Hasher for FastHasher {
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0_u8; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.add(u64::from_le_bytes(word));
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.add(u64::from(value));
    }

    fn write_u32(&mut self, value: u32) {
        self.add(u64::from(value));
    }

    fn write_u64(&mut self, value: u64) {
        self.add(value);
    }

    fn write_usize(&mut self, value: usize) {
        self.add(value as u64);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<FastHasher>>;
type FastSet<T> = HashSet<T, BuildHasherDefault<FastHasher>>;

/// Dense ids for repeated strings, so the ledger-wide pass keeps one copy of
/// each distinct provider, model, reason and so on instead of one per event.
#[derive(Debug, Default)]
struct Interner {
    ids: FastMap<Box<str>, u32>,
    names: Vec<Box<str>>,
}

impl Interner {
    fn intern(&mut self, value: &str) -> Result<u32, ReportError> {
        if let Some(id) = self.ids.get(value) {
            return Ok(*id);
        }
        let id = u32::try_from(self.names.len()).map_err(|_| ReportError::Arithmetic)?;
        self.ids.insert(value.into(), id);
        self.names.push(value.into());
        Ok(id)
    }

    fn get(&self, value: &str) -> Option<u32> {
        self.ids.get(value).copied()
    }

    fn name(&self, id: u32) -> &str {
        &self.names[id as usize]
    }

    fn optional_name(&self, id: Option<u32>) -> Option<String> {
        id.map(|id| self.name(id).to_string())
    }
}

/// One usage event reduced to what the ledger-wide reports read, with its
/// measurements and current charge heads already folded in.
#[derive(Debug)]
struct LedgerEvent {
    key: i64,
    occurred_at_ms: Option<i64>,
    /// Interned in `Ledger::values`.
    currency: Option<u32>,
    charge_basis: ChargeBasis,
    /// Interned in `Ledger::values`, indexed by `Grouping as usize`. Only the
    /// dimensions the caller asked `load_ledger` for are filled in.
    dimensions: [Option<u32>; GROUPING_DIMENSIONS],
    measurements: u32,
    /// Measurements that have a current charge head, i.e. that some pricing
    /// pass has examined.
    examined: u32,
    /// True when at least one measurement states a quantity of exactly zero;
    /// only such events need the per-measurement zero lookup.
    has_stated_zero: bool,
    /// Bit `n` is set when a measurement declined for reason id `n` (in
    /// `Ledger::reasons`) blocks this event's total.
    blocking_reasons: u64,
    /// Every declined measurement, blocking or not, as (reason id, count).
    declined: Vec<(u32, u64)>,
    /// Sum of `charge_nanos` over current heads whose outcome is `priced`.
    priced_nanos: i128,
    adjustment_nanos: i128,
    priced_measurements: u64,
    covered_measurements: u64,
    oldest_resolved_at_ms: Option<i64>,
}

impl LedgerEvent {
    /// True when every measurement's contribution to this event's total is
    /// known: a pricing pass answered for it, or arithmetic answers for it
    /// because the producer stated a quantity of zero. An event with an exact
    /// total is not an unpriced event, however incomplete the catalog behind
    /// it is -- that incompleteness is reported per measurement instead, and
    /// never dropped, by `declined`.
    fn is_fully_priced(&self) -> bool {
        self.measurements > 0 && self.examined == self.measurements && self.blocking_reasons == 0
    }

    fn current_head_total(&self) -> Result<i64, ReportError> {
        i64::try_from(self.priced_nanos).map_err(|_| ReportError::Arithmetic)
    }

    fn report_total(&self) -> Result<i64, ReportError> {
        checked_add(
            self.current_head_total()?,
            i64::try_from(self.adjustment_nanos).map_err(|_| ReportError::Arithmetic)?,
        )
    }

    fn exclusion_reason(&self) -> Exclusion {
        if self.measurements == 0 {
            Exclusion::NoMeasurements
        } else if self.occurred_at_ms.is_none() {
            Exclusion::Undated
        } else if self.blocking_reasons != 0 {
            Exclusion::Blocking(self.blocking_reasons)
        } else if self.examined < self.measurements {
            Exclusion::NotYetPriced
        } else {
            Exclusion::Unpriced
        }
    }
}

/// Why an event is excluded from the priced population, kept compact while
/// counting and rendered to its published label once per group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Exclusion {
    NoMeasurements,
    Undated,
    /// EVERY blocking reason, not the first one found. Taking the first hid
    /// the second cause of an event that had two, so a catalog gap could be
    /// fixed and the event stay excluded for a reason the report had never
    /// shown. Only BLOCKING measurements are consulted: a declined
    /// zero-quantity measurement did not exclude its event, and naming it
    /// here would attribute the exclusion to a measurement that is not
    /// responsible. Its reason is reported per measurement instead.
    Blocking(u64),
    /// No pricing pass has examined some measurement yet -- the catalog was
    /// pending, or the event landed after the last tick. That is a different
    /// claim from every unpriced_reason, which all mean a pass ran and
    /// declined; labelling it "unpriced" would fuse the two.
    NotYetPriced,
    /// An excluded event always has a blocking measurement, and that
    /// measurement is either unexamined or declined with a reason (a head
    /// that declined without naming one reports the bare "unpriced"). This
    /// arm keeps the classification total if that ever stops holding.
    Unpriced,
}

impl Exclusion {
    fn label(self, reasons: &Interner) -> String {
        match self {
            Self::NoMeasurements => "no_measurements".into(),
            Self::Undated => "undated".into(),
            Self::Blocking(mask) => {
                // Sorted by name, so the same set of causes always yields the
                // same key and the keys stay countable.
                let names: BTreeSet<&str> = (0..64)
                    .filter(|bit| mask & (1_u64 << bit) != 0)
                    .map(|bit| reasons.name(bit))
                    .collect();
                names.into_iter().collect::<Vec<_>>().join("+")
            }
            Self::NotYetPriced => "not_yet_priced".into(),
            Self::Unpriced => "unpriced".into(),
        }
    }
}

/// Adds per-group exclusion counts into a published reason map. Two compact
/// keys can render to the same label (a head that declined without a reason
/// renders as "unpriced", as does the fallback), so counts are merged.
fn render_exclusions(
    counts: FastMap<Exclusion, u64>,
    reasons: &Interner,
) -> Result<BTreeMap<String, u64>, ReportError> {
    let mut rendered: BTreeMap<String, u64> = BTreeMap::new();
    for (exclusion, count) in counts {
        let entry = rendered.entry(exclusion.label(reasons)).or_default();
        *entry = entry.checked_add(count).ok_or(ReportError::Arithmetic)?;
    }
    Ok(rendered)
}

fn render_reason_counts(counts: FastMap<u32, u64>, reasons: &Interner) -> BTreeMap<String, u64> {
    counts
        .into_iter()
        .map(|(reason, count)| (reasons.name(reason).to_string(), count))
        .collect()
}

/// The whole ledger reduced to one `LedgerEvent` per usage event, plus the
/// report-wide rate-staleness inputs.
#[derive(Debug)]
struct Ledger {
    events: Vec<LedgerEvent>,
    /// Currency and grouping-dimension strings.
    values: Interner,
    /// Declined-measurement reasons.
    reasons: Interner,
    measurements_total: u64,
    priced_measurements: u64,
    oldest_resolved_at_ms: Option<i64>,
}

impl Ledger {
    fn rate_staleness(&self, now_ms: i64) -> RateStaleness {
        RateStaleness {
            as_of_ms: now_ms,
            oldest_resolved_at_ms: self.oldest_resolved_at_ms,
            age_ms: self
                .oldest_resolved_at_ms
                .map(|instant| now_ms.saturating_sub(instant)),
            priced_measurements: self.priced_measurements,
            measurements_total: self.measurements_total,
        }
    }
}

/// Finds an event's position from its id. Measurement and charge rows are
/// stored in runs that share an event, so the previous answer is checked
/// before the hash map.
struct EventLookup<'a> {
    index: &'a FastMap<i64, u32>,
    last_id: i64,
    last_position: Option<u32>,
}

impl<'a> EventLookup<'a> {
    fn new(index: &'a FastMap<i64, u32>) -> Self {
        Self {
            index,
            last_id: 0,
            last_position: None,
        }
    }

    fn find(&mut self, event_key: i64) -> Result<usize, ReportError> {
        if let Some(position) = self.last_position.filter(|_| self.last_id == event_key) {
            return Ok(position as usize);
        }
        // A measurement or charge always belongs to a recorded event (both
        // tables carry a foreign key to it), so a miss is a store fault.
        let position = *self.index.get(&event_key).ok_or(ReportError::Integrity(
            "stored row references an event absent from the ledger",
        ))?;
        self.last_id = event_key;
        self.last_position = Some(position);
        Ok(position as usize)
    }
}

/// Reads a TEXT column without copying it. NULL reads as `None`; any other
/// storage class is a decode failure, as it is for `row.get::<_, String>`.
fn text_column<'row>(
    row: &'row rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<&'row str>> {
    row.get_ref(index)?.as_str_or_null().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn required_text_column<'row>(
    row: &'row rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<&'row str> {
    text_column(row, index)?.ok_or_else(|| {
        rusqlite::Error::InvalidColumnType(index, "NULL".into(), rusqlite::types::Type::Null)
    })
}

fn parse_charge_basis(value: &str, column: usize) -> rusqlite::Result<ChargeBasis> {
    ChargeBasis::parse(value).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unknown stored charge_basis",
            )),
        )
    })
}

/// Folds every usage event, measurement, current charge head and adjustment
/// into one `LedgerEvent` per event.
///
/// THE COST IS FOUR SEQUENTIAL TABLE SCANS plus one read of the superseded
/// charge ids, never an index lookup per event, measurement or charge. On a ledger of a million events an indexed join from each
/// measurement to its charge head spends almost all of its time on random
/// page reads, while a plain scan of each table streams in about a second.
/// The scans are joined here, in memory, on the event id.
///
/// The measurement-to-head join the event verdict needs is avoided rather than
/// performed: every charge head belongs to a measurement (foreign key), and a
/// measurement has at most one head (one chain root per measurement, and
/// `supersedes` is UNIQUE, so the chain cannot fork). So the measurements a
/// pass has not examined number `measurements - heads`, and the only
/// per-measurement fact a declined head needs from its measurement -- whether
/// the producer stated exactly zero units -- is looked up in a set of the
/// zero-quantity measurements, and only for events that have one.
///
/// `dimensions` names the grouping columns the caller will read; the others
/// stay `None` so the pass does not intern strings nobody uses.
fn load_ledger(conn: &Connection, dimensions: &[Grouping]) -> Result<Ledger, ReportError> {
    load_scoped_ledger(conn, dimensions, None)
}

fn load_scoped_ledger(
    conn: &Connection,
    dimensions: &[Grouping],
    filter: Option<&str>,
) -> Result<Ledger, ReportError> {
    let mut values = Interner::default();
    let mut reasons = Interner::default();
    let mut index: FastMap<i64, u32> = FastMap::default();
    let mut events = Vec::new();

    let event_filter = filter.map_or_else(String::new, |filter| format!(" WHERE {filter}"));
    let child_filter = filter.map_or_else(String::new, |filter| {
        format!(" WHERE event_key IN (SELECT event_key FROM usage_event WHERE {filter})")
    });
    let mut statement = conn.prepare(&format!(
        "SELECT event_key,occurred_at_ms,currency,charge_basis,attempt_ordinal,
                 pricing_generation,provider,model,account_ref,auth_selection,
                 work_class,project_root,producer
         FROM usage_event{event_filter}"
    ))?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let charge_basis = parse_charge_basis(required_text_column(row, 3)?, 3)?;
        // Decoded only to refuse the same malformed rows reading a full
        // `UsageEvent` refuses; the values are not otherwise used here.
        row.get::<_, Option<i64>>(4)?
            .map(u32::try_from)
            .transpose()
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
        u64::try_from(row.get::<_, i64>(5)?).map_err(|_| rusqlite::Error::InvalidQuery)?;
        let mut event_dimensions = [None; GROUPING_DIMENSIONS];
        for dimension in dimensions {
            let value = match dimension {
                Grouping::Provider => text_column(row, 6)?,
                Grouping::Model => text_column(row, 7)?,
                Grouping::Account => text_column(row, 8)?,
                Grouping::AuthSelection => text_column(row, 9)?,
                Grouping::ChargeBasis => Some(charge_basis.as_str()),
                Grouping::WorkClass => text_column(row, 10)?,
                Grouping::Project => text_column(row, 11)?,
                Grouping::Producer => text_column(row, 12)?,
            };
            event_dimensions[*dimension as usize] =
                value.map(|value| values.intern(value)).transpose()?;
        }
        let position = u32::try_from(events.len()).map_err(|_| ReportError::Arithmetic)?;
        index.insert(row.get(0)?, position);
        events.push(LedgerEvent {
            key: row.get(0)?,
            occurred_at_ms: row.get(1)?,
            currency: text_column(row, 2)?
                .map(|value| values.intern(value))
                .transpose()?,
            charge_basis,
            dimensions: event_dimensions,
            measurements: 0,
            examined: 0,
            has_stated_zero: false,
            blocking_reasons: 0,
            declined: Vec::new(),
            priced_nanos: 0,
            adjustment_nanos: 0,
            priced_measurements: 0,
            covered_measurements: 0,
            oldest_resolved_at_ms: None,
        });
    }
    drop(rows);
    drop(statement);

    // Measurements: count per event, and remember which ones state exactly
    // zero units, keyed by (event position, role id, unit id).
    let mut roles = Interner::default();
    let mut units = Interner::default();
    let mut stated_zero: FastSet<(usize, u32, u32)> = FastSet::default();
    let mut measurements_total = 0_u64;
    {
        let mut lookup = EventLookup::new(&index);
        let mut statement = conn.prepare(&format!(
            "SELECT event_key,role,unit,quantity_units FROM measurement{child_filter}"
        ))?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let position = lookup.find(row.get(0)?)?;
            let event = &mut events[position];
            event.measurements = event
                .measurements
                .checked_add(1)
                .ok_or(ReportError::Arithmetic)?;
            measurements_total = measurements_total
                .checked_add(1)
                .ok_or(ReportError::Arithmetic)?;
            if row.get::<_, Option<i64>>(3)? == Some(0) {
                event.has_stated_zero = true;
                stated_zero.insert((
                    position,
                    roles.intern(required_text_column(row, 1)?)?,
                    units.intern(required_text_column(row, 2)?)?,
                ));
            }
        }
    }

    // Current charge heads: a charge is current when NOTHING points at it,
    // that is when no charge's `supersedes` names its id. (A NULL `supersedes`
    // marks a chain's ORIGINAL, which is a different set.) The predicate is
    // evaluated as an anti-join against the ids that ARE superseded. The set
    // has one id per non-root charge; there is no bound on correction depth,
    // so it can approach the full history size. This trades that memory for
    // sequential reads instead of an index probe per charge.
    let superseded: FastSet<i64> = {
        let superseded_filter = child_filter
            .strip_prefix(" WHERE ")
            .map_or_else(String::new, |filter| format!(" AND {filter}"));
        let mut statement = conn.prepare(&format!(
            "SELECT supersedes FROM priced_charge WHERE supersedes IS NOT NULL{superseded_filter}"
        ))?;
        let mut rows = statement.query([])?;
        let mut superseded = FastSet::default();
        while let Some(row) = rows.next()? {
            superseded.insert(row.get::<_, i64>(0)?);
        }
        superseded
    };
    let mut priced_measurements = 0_u64;
    let mut oldest_resolved_at_ms: Option<i64> = None;
    {
        let mut lookup = EventLookup::new(&index);
        let mut statement = conn.prepare(&format!(
            "SELECT event_key,role,unit,outcome,charge_nanos,unpriced_reason,resolved_at_ms,
                    charge_key
             FROM priced_charge{child_filter}"
        ))?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            if superseded.contains(&row.get::<_, i64>(7)?) {
                continue;
            }
            let position = lookup.find(row.get(0)?)?;
            let outcome = required_text_column(row, 3)?;
            // Nullable columns read through `?`, never `.ok()`: a column that
            // fails to decode is a store fault, and swallowing it into None
            // would render a corrupt charge as "no amount".
            let charge_nanos: Option<i64> = row.get(4)?;
            let unpriced_reason = text_column(row, 5)?;
            let resolved_at_ms: Option<i64> = row.get(6)?;
            let event = &mut events[position];
            event.examined = event
                .examined
                .checked_add(1)
                .ok_or(ReportError::Arithmetic)?;
            match outcome {
                "priced" => {
                    let charge_nanos = charge_nanos.ok_or(ReportError::Integrity(
                        "priced head is missing charge_nanos",
                    ))?;
                    let resolved_at_ms = resolved_at_ms.ok_or(ReportError::Integrity(
                        "priced head is missing resolved_at_ms",
                    ))?;
                    event.priced_nanos = event
                        .priced_nanos
                        .checked_add(i128::from(charge_nanos))
                        .ok_or(ReportError::Arithmetic)?;
                    add_count(&mut priced_measurements, 1)?;
                    add_count(&mut event.priced_measurements, 1)?;
                    event.oldest_resolved_at_ms = Some(
                        event
                            .oldest_resolved_at_ms
                            .map_or(resolved_at_ms, |old| old.min(resolved_at_ms)),
                    );
                    oldest_resolved_at_ms = Some(
                        oldest_resolved_at_ms
                            .map_or(resolved_at_ms, |oldest| oldest.min(resolved_at_ms)),
                    );
                }
                "covered" => add_count(&mut event.covered_measurements, 1)?,
                _ => {
                    let reason = reasons.intern(declined_reason(unpriced_reason))?;
                    match event
                        .declined
                        .iter_mut()
                        .find(|(declined, _)| *declined == reason)
                    {
                        Some((_, count)) => add_count(count, 1)?,
                        None => event.declined.push((reason, 1)),
                    }
                    let is_stated_zero = event.has_stated_zero
                        && match (
                            roles.get(required_text_column(row, 1)?),
                            units.get(required_text_column(row, 2)?),
                        ) {
                            (Some(role), Some(unit)) => {
                                stated_zero.contains(&(position, role, unit))
                            }
                            _ => false,
                        };
                    if blocks_event_total(Some(outcome), is_stated_zero) {
                        // The unpriced_reason column is closed by a CHECK to a
                        // short list, so the mask cannot run out of bits on a
                        // well-formed store.
                        if reason >= 64 {
                            return Err(ReportError::Integrity(
                                "decline reasons exceed the ledger reason mask",
                            ));
                        }
                        event.blocking_reasons |= 1_u64 << reason;
                    }
                }
            }
        }
    }

    {
        let mut lookup = EventLookup::new(&index);
        let mut statement = conn.prepare(&format!(
            "SELECT event_key,amount_nanos FROM adjustment{child_filter}"
        ))?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let position = lookup.find(row.get(0)?)?;
            let event = &mut events[position];
            event.adjustment_nanos = event
                .adjustment_nanos
                .checked_add(i128::from(row.get::<_, i64>(1)?))
                .ok_or(ReportError::Arithmetic)?;
        }
    }
    // Every event's adjustments must sum within range whether or not the
    // calling report reads the total, as summing them per event always did.
    if events
        .iter()
        .any(|event| i64::try_from(event.adjustment_nanos).is_err())
    {
        return Err(ReportError::Arithmetic);
    }

    Ok(Ledger {
        events,
        values,
        reasons,
        measurements_total,
        priced_measurements,
        oldest_resolved_at_ms,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BucketKey {
    start_ms: i64,
    end_ms: i64,
    grouping: Vec<(String, Option<String>)>,
    currency: Option<String>,
}

/// A spend bucket while counting: bounds, interned grouping values in request
/// order, and interned currency.
type CompactBucketKey = (i64, i64, [Option<u32>; GROUPING_DIMENSIONS], Option<u32>);

#[derive(Debug, Default)]
struct SpendAccumulator {
    total_nanos: i128,
    metered_nanos: i128,
    population: EventPopulation,
}

/// Counts only events the caller includes. Spend supplies dated events;
/// ledger coverage supplies all events. Subscription attribution and work-class
/// currency samples have different exclusions and keep their own populations.
#[derive(Debug, Default)]
struct EventPopulation {
    events_total: u64,
    events_priced: u64,
    zero_measurement_events: u64,
    measurements_total: u64,
    unpriced_events_by_reason: FastMap<Exclusion, u64>,
    unpriced_measurements_by_reason: FastMap<u32, u64>,
}

impl EventPopulation {
    fn observe(&mut self, event: &LedgerEvent) -> Result<(), ReportError> {
        add_count(&mut self.events_total, 1)?;
        add_count(&mut self.measurements_total, u64::from(event.measurements))?;
        // Declined zeros still disclose their gap even when the event is fully priced.
        for (reason, count) in &event.declined {
            add_count(
                self.unpriced_measurements_by_reason
                    .entry(*reason)
                    .or_default(),
                *count,
            )?;
        }
        if event.is_fully_priced() {
            add_count(&mut self.events_priced, 1)?;
        } else {
            add_count(
                self.unpriced_events_by_reason
                    .entry(event.exclusion_reason())
                    .or_default(),
                1,
            )?;
        }
        if event.measurements == 0 {
            add_count(&mut self.zero_measurement_events, 1)?;
        }
        Ok(())
    }
}

/// Read dated spend buckets and ledger-wide disclosures in one caller-owned transaction/snapshot.
pub fn spend_report(
    conn: &Connection,
    request: &ReportRequest,
    now_ms: i64,
) -> Result<SpendReport, ReportError> {
    if let Ok(Some(report)) = hourly::spend(conn, request, now_ms) {
        return Ok(report);
    }
    spend_report_base(conn, request, now_ms, None)
}

fn spend_report_base(
    conn: &Connection,
    request: &ReportRequest,
    now_ms: i64,
    filter: Option<&str>,
) -> Result<SpendReport, ReportError> {
    let grain = Grain::parse(&request.grain)
        .ok_or_else(|| ReportError::Refused(ReportRefusal::UnknownGrain(request.grain.clone())))?;
    let timezone: Tz = request.timezone.parse().map_err(|_| {
        ReportError::Refused(ReportRefusal::UnknownTimezone(request.timezone.clone()))
    })?;
    let grouping = parse_grouping(&request.grouping)?;
    let ledger = load_scoped_ledger(conn, &grouping, filter)?;
    spend_report_from_ledger(conn, &ledger, &grouping, grain, timezone, now_ms)
}

fn spend_report_from_ledger(
    conn: &Connection,
    ledger: &Ledger,
    grouping: &[Grouping],
    grain: Grain,
    timezone: Tz,
    now_ms: i64,
) -> Result<SpendReport, ReportError> {
    let mut buckets: FastMap<CompactBucketKey, SpendAccumulator> = FastMap::default();
    let mut undated = 0_u64;
    // Bucket bounds depend only on the event's local date, and neighbouring
    // events usually share one, so the last date's bounds are reused.
    let mut last_bounds: Option<(NaiveDate, (i64, i64))> = None;

    for event in &ledger.events {
        let Some(occurred_at_ms) = event.occurred_at_ms else {
            undated = undated.checked_add(1).ok_or(ReportError::Arithmetic)?;
            continue;
        };
        let date = local_date(occurred_at_ms, timezone)?;
        let (start_ms, end_ms) = match last_bounds {
            Some((last_date, bounds)) if last_date == date => bounds,
            _ => {
                let bounds = bucket_bounds_for_date(date, timezone, grain)?;
                last_bounds = Some((date, bounds));
                bounds
            }
        };
        let mut grouping_values = [None; GROUPING_DIMENSIONS];
        for (slot, dimension) in grouping.iter().enumerate() {
            grouping_values[slot] = event.dimensions[*dimension as usize];
        }
        let accumulator = buckets
            .entry((start_ms, end_ms, grouping_values, event.currency))
            .or_default();
        accumulator.population.observe(event)?;
        let event_total = event.report_total()?;
        accumulator.total_nanos = accumulator
            .total_nanos
            .checked_add(i128::from(event_total))
            .ok_or(ReportError::Arithmetic)?;
        if event.charge_basis == ChargeBasis::MeteredApi {
            accumulator.metered_nanos = accumulator
                .metered_nanos
                .checked_add(i128::from(event_total))
                .ok_or(ReportError::Arithmetic)?;
        }
    }

    let mut keyed = buckets
        .into_iter()
        .map(
            |((start_ms, end_ms, grouping_values, currency), accumulator)| {
                let key = BucketKey {
                    start_ms,
                    end_ms,
                    grouping: grouping
                        .iter()
                        .zip(grouping_values)
                        .map(|(dimension, value)| {
                            (
                                dimension.name().to_string(),
                                ledger.values.optional_name(value),
                            )
                        })
                        .collect(),
                    currency: ledger.values.optional_name(currency),
                };
                (key, accumulator)
            },
        )
        .collect::<Vec<_>>();
    keyed.sort_by(|left, right| left.0.cmp(&right.0));
    let rows = keyed
        .into_iter()
        .map(|(key, accumulator)| {
            let population = accumulator.population;
            let events_unpriced = population
                .events_total
                .checked_sub(population.events_priced)
                .ok_or(ReportError::Arithmetic)?;
            Ok(SpendRow {
                bucket_start_ms: key.start_ms,
                bucket_end_ms: key.end_ms,
                grouping: key.grouping.into_iter().collect(),
                currency: key.currency,
                total_nanos: i64::try_from(accumulator.total_nanos)
                    .map_err(|_| ReportError::Arithmetic)?,
                metered_nanos: i64::try_from(accumulator.metered_nanos)
                    .map_err(|_| ReportError::Arithmetic)?,
                events_total: population.events_total,
                events_priced: population.events_priced,
                events_unpriced,
                cost_sample_n: population.events_priced,
                zero_measurement_events: population.zero_measurement_events,
                measurements_total: population.measurements_total,
                unpriced_events_by_reason: render_exclusions(
                    population.unpriced_events_by_reason,
                    &ledger.reasons,
                )?,
                unpriced_measurements_by_reason: render_reason_counts(
                    population.unpriced_measurements_by_reason,
                    &ledger.reasons,
                ),
            })
        })
        .collect::<Result<Vec<_>, ReportError>>()?;

    let mut headline: BTreeMap<Option<String>, SpendTotal> = BTreeMap::new();
    for row in &rows {
        let total = headline.entry(row.currency.clone()).or_insert(SpendTotal {
            currency: row.currency.clone(),
            total_nanos: 0,
            metered_nanos: 0,
            events_total: 0,
            events_priced: 0,
            events_unpriced: 0,
            cost_sample_n: 0,
            zero_measurement_events: 0,
        });
        total.total_nanos = checked_add(total.total_nanos, row.total_nanos)?;
        total.metered_nanos = checked_add(total.metered_nanos, row.metered_nanos)?;
        total.events_total = total
            .events_total
            .checked_add(row.events_total)
            .ok_or(ReportError::Arithmetic)?;
        total.events_priced = total
            .events_priced
            .checked_add(row.events_priced)
            .ok_or(ReportError::Arithmetic)?;
        total.events_unpriced = total
            .events_unpriced
            .checked_add(row.events_unpriced)
            .ok_or(ReportError::Arithmetic)?;
        total.cost_sample_n = total
            .cost_sample_n
            .checked_add(row.cost_sample_n)
            .ok_or(ReportError::Arithmetic)?;
        total.zero_measurement_events = total
            .zero_measurement_events
            .checked_add(row.zero_measurement_events)
            .ok_or(ReportError::Arithmetic)?;
    }
    let mut undated_reasons = BTreeMap::new();
    if undated > 0 {
        undated_reasons.insert("missing_occurred_at".into(), undated);
    }
    Ok(SpendReport {
        totals: headline.into_values().collect(),
        rows,
        undated: UndatedCoverage {
            events_undated: undated,
            events_report_total: to_u64(ledger.events.len())?,
            reasons: undated_reasons,
        },
        ingest_freshness: load_ingest_freshness(conn)?,
        rate_staleness: ledger.rate_staleness(now_ms),
        hierarchy_resolution: grouping
            .contains(&Grouping::Project)
            .then_some(Availability::Unavailable),
    })
}

fn parse_grouping(values: &[String]) -> Result<Vec<Grouping>, ReportError> {
    let mut parsed = Vec::with_capacity(values.len());
    let mut seen = BTreeSet::new();
    for value in values {
        let dimension = Grouping::parse(value)
            .ok_or_else(|| ReportError::Refused(ReportRefusal::UnknownGrouping(value.clone())))?;
        if !seen.insert(dimension) {
            return Err(ReportError::Refused(ReportRefusal::DuplicateGrouping(
                value.clone(),
            )));
        }
        parsed.push(dimension);
    }
    Ok(parsed)
}

/// The calendar bucket holding instant `ms`, in `timezone`. Spend reports
/// compute the same bounds in two steps so they can reuse them per local date.
#[cfg(test)]
fn bucket_bounds(ms: i64, timezone: Tz, grain: Grain) -> Result<(i64, i64), ReportError> {
    bucket_bounds_for_date(local_date(ms, timezone)?, timezone, grain)
}

/// The wall-clock date instant `ms` falls on in `timezone`.
fn local_date(ms: i64, timezone: Tz) -> Result<NaiveDate, ReportError> {
    Ok(Utc
        .timestamp_millis_opt(ms)
        .single()
        .ok_or(ReportError::Calendar)?
        .with_timezone(&timezone)
        .date_naive())
}

/// The bounds of the calendar bucket containing local `date`: from local
/// day's first existing instant at its start to that at its end. A skipped
/// midnight or date advances to the first instant after the clock's gap.
fn bucket_bounds_for_date(
    date: NaiveDate,
    timezone: Tz,
    grain: Grain,
) -> Result<(i64, i64), ReportError> {
    let start_date = match grain {
        Grain::Day => date,
        Grain::Week => date
            .checked_sub_signed(Duration::days(i64::from(
                date.weekday().num_days_from_monday(),
            )))
            .ok_or(ReportError::Calendar)?,
        Grain::Month => {
            NaiveDate::from_ymd_opt(date.year(), date.month(), 1).ok_or(ReportError::Calendar)?
        }
    };
    let end_date = match grain {
        Grain::Day => start_date.succ_opt().ok_or(ReportError::Calendar)?,
        Grain::Week => start_date
            .checked_add_signed(Duration::days(7))
            .ok_or(ReportError::Calendar)?,
        Grain::Month => {
            let (year, month) = if start_date.month() == 12 {
                (start_date.year() + 1, 1)
            } else {
                (start_date.year(), start_date.month() + 1)
            };
            NaiveDate::from_ymd_opt(year, month, 1).ok_or(ReportError::Calendar)?
        }
    };
    Ok((
        local_midnight_ms(timezone, start_date)?,
        local_midnight_ms(timezone, end_date)?,
    ))
}

fn local_midnight_ms(timezone: Tz, date: NaiveDate) -> Result<i64, ReportError> {
    let midnight = date.and_hms_opt(0, 0, 0).ok_or(ReportError::Calendar)?;
    timezone
        .from_local_datetime(&midnight)
        .earliest()
        .or_else(|| chrono_tz::GapInfo::new(&midnight, &timezone).and_then(|gap| gap.end))
        .map(|instant| instant.timestamp_millis())
        .ok_or(ReportError::Calendar)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionReportRequest {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CoveredQuantity {
    pub unit: String,
    pub role: String,
    pub quantity_units: i64,
    pub quantity_exponent: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SubscriptionRow {
    pub provider: String,
    pub account_ref: String,
    pub period_start_ms: i64,
    pub period_end_ms: i64,
    pub plan_cost_nanos: i64,
    pub covered_events: u64,
    pub covered_measurements: u64,
    pub quantities: Vec<CoveredQuantity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SubscriptionCoverage {
    pub events_total: u64,
    pub events_unpriced: u64,
    pub events_undated: u64,
    pub zero_measurement_events: u64,
    /// Dated events that record no account. Every one of them is also counted
    /// in `events_unpriced`: a plan covers one provider account, so an event
    /// with no account can never be covered, and the account is a producer
    /// fact that is never inferred. Counted on its own so these read as usage
    /// nobody attributed rather than as usage no plan was declared for.
    pub events_without_account: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SubscriptionReport {
    pub rows: Vec<SubscriptionRow>,
    pub coverage: SubscriptionCoverage,
    pub ingest_freshness: IngestFreshness,
}

#[derive(Debug, Clone)]
struct SubscriptionView {
    account_ref: String,
    provider: String,
    recurring_cost_minor: i64,
    amount_exponent: u32,
    period: SubscriptionPeriod,
    effective_from_ms: i64,
    effective_to_ms: Option<i64>,
}

struct IndexedSubscription<'a> {
    plan: &'a SubscriptionView,
    /// Maximum end through this entry; None is an unbounded interval.
    latest_end_ms: Option<i64>,
}

/// Plans arrive in start order. Prefix maxima let lookup find the first
/// in-force plan even if stored intervals overlap, preserving start-order
/// precedence without scanning unrelated providers or expired plans.
#[derive(Default)]
struct SubscriptionIndex<'a> {
    accounts: BTreeMap<&'a str, BTreeMap<&'a str, Vec<IndexedSubscription<'a>>>>,
}

impl<'a> SubscriptionIndex<'a> {
    fn new(plans: &'a [SubscriptionView]) -> Self {
        let mut index = Self::default();
        for plan in plans {
            let entries = index
                .accounts
                .entry(&plan.provider)
                .or_default()
                .entry(&plan.account_ref)
                .or_default();
            let latest_end_ms = match entries.last() {
                None => plan.effective_to_ms,
                Some(previous) => match (previous.latest_end_ms, plan.effective_to_ms) {
                    (Some(previous), Some(end)) => Some(previous.max(end)),
                    _ => None,
                },
            };
            entries.push(IndexedSubscription {
                plan,
                latest_end_ms,
            });
        }
        index
    }

    fn find(&self, provider: &str, account: &str, instant_ms: i64) -> Option<&'a SubscriptionView> {
        let entries = self.accounts.get(provider)?.get(account)?;
        let position = entries
            .partition_point(|entry| entry.latest_end_ms.is_some_and(|end| end <= instant_ms));
        let plan = entries.get(position)?.plan;
        (plan.effective_from_ms <= instant_ms).then_some(plan)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SubscriptionBucketKey {
    account_ref: String,
    provider: String,
    start_ms: i64,
    end_ms: i64,
    plan_cost_nanos: i64,
}

#[derive(Debug, Default)]
struct SubscriptionAccumulator {
    covered_events: u64,
    covered_measurements: u64,
    quantities: BTreeMap<(String, String), DecimalSum>,
}

#[derive(Debug, Clone, Copy, Default)]
struct DecimalSum {
    units: i128,
    exponent: i32,
    initialized: bool,
}

impl DecimalSum {
    fn add(&mut self, units: i64, exponent: i32) -> Result<(), ReportError> {
        if !self.initialized {
            self.units = i128::from(units);
            self.exponent = exponent;
            self.initialized = true;
            return Ok(());
        }
        let target = self.exponent.min(exponent);
        let left_scale = pow10_i128(self.exponent.abs_diff(target))?;
        let right_scale = pow10_i128(exponent.abs_diff(target))?;
        self.units = self
            .units
            .checked_mul(left_scale)
            .and_then(|value| value.checked_add(i128::from(units).checked_mul(right_scale)?))
            .ok_or(ReportError::Arithmetic)?;
        self.exponent = target;
        Ok(())
    }
}

/// Read subscription usage in one caller-owned transaction/snapshot.
pub fn subscription_report(
    conn: &Connection,
    request: &SubscriptionReportRequest,
) -> Result<SubscriptionReport, ReportError> {
    if let Ok(Some(report)) = hourly::subscription(conn, request) {
        return Ok(report);
    }
    subscription_report_base(conn, request, None)
}

fn subscription_report_base(
    conn: &Connection,
    _request: &SubscriptionReportRequest,
    filter: Option<&str>,
) -> Result<SubscriptionReport, ReportError> {
    let events = load_subscription_events(conn, filter)?;
    let subscriptions = load_subscriptions(conn)?;
    let plans = SubscriptionIndex::new(&subscriptions);
    let mut rows: BTreeMap<SubscriptionBucketKey, SubscriptionAccumulator> = BTreeMap::new();
    let mut coverage = SubscriptionCoverage {
        events_total: 0,
        events_unpriced: 0,
        events_undated: 0,
        zero_measurement_events: 0,
        events_without_account: 0,
    };

    for event in &events {
        coverage.events_total = coverage
            .events_total
            .checked_add(1)
            .ok_or(ReportError::Arithmetic)?;
        if event.measurements.is_empty() {
            coverage.zero_measurement_events = coverage
                .zero_measurement_events
                .checked_add(1)
                .ok_or(ReportError::Arithmetic)?;
        }
        let Some(occurred_at_ms) = event.occurred_at_ms else {
            coverage.events_undated = coverage
                .events_undated
                .checked_add(1)
                .ok_or(ReportError::Arithmetic)?;
            continue;
        };
        if event
            .account_ref
            .as_deref()
            .is_none_or(|account| account.is_empty())
        {
            add_count(&mut coverage.events_without_account, 1)?;
        }
        if !event.is_fully_priced() {
            coverage.events_unpriced = coverage
                .events_unpriced
                .checked_add(1)
                .ok_or(ReportError::Arithmetic)?;
            continue;
        }
        let Some(provider) = event.provider.as_deref() else {
            add_count(&mut coverage.events_unpriced, 1)?;
            continue;
        };
        let Some(account_ref) = event.account_ref.as_deref() else {
            add_count(&mut coverage.events_unpriced, 1)?;
            continue;
        };
        let Some(plan) = plans.find(provider, account_ref, occurred_at_ms) else {
            add_count(&mut coverage.events_unpriced, 1)?;
            continue;
        };
        let (period_start_ms, natural_end_ms) = subscription_period_bounds(plan, occurred_at_ms)?;
        let period_end_ms = plan.effective_to_ms.map_or(natural_end_ms, |effective_to| {
            natural_end_ms.min(effective_to)
        });
        let plan_cost_nanos = recurring_cost_nanos(plan.recurring_cost_minor, plan.amount_exponent)
            .map_err(|_| ReportError::Arithmetic)?;
        let key = SubscriptionBucketKey {
            account_ref: account_ref.into(),
            provider: provider.into(),
            start_ms: period_start_ms,
            end_ms: period_end_ms,
            plan_cost_nanos,
        };
        let accumulator = rows.entry(key).or_default();
        // The ordered loader yields each event once, and each event enters one bucket.
        add_count(&mut accumulator.covered_events, 1)?;
        for measurement in &event.measurements {
            if measurement.head_outcome.as_deref() != Some("covered") {
                continue;
            }
            accumulator.covered_measurements = accumulator
                .covered_measurements
                .checked_add(1)
                .ok_or(ReportError::Arithmetic)?;
            if let (Some(units), Some(exponent)) =
                (measurement.quantity_units, measurement.quantity_exponent)
            {
                accumulator
                    .quantities
                    .entry((measurement.unit.clone(), measurement.role.clone()))
                    .or_default()
                    .add(units, exponent)?;
            }
        }
    }

    let rows = rows
        .into_iter()
        .map(|(key, accumulator)| {
            let quantities = accumulator
                .quantities
                .into_iter()
                .map(|((unit, role), sum)| {
                    Ok(CoveredQuantity {
                        unit,
                        role,
                        quantity_units: i64::try_from(sum.units)
                            .map_err(|_| ReportError::Arithmetic)?,
                        quantity_exponent: sum.exponent,
                    })
                })
                .collect::<Result<Vec<_>, ReportError>>()?;
            Ok(SubscriptionRow {
                provider: key.provider,
                account_ref: key.account_ref,
                period_start_ms: key.start_ms,
                period_end_ms: key.end_ms,
                plan_cost_nanos: key.plan_cost_nanos,
                covered_events: accumulator.covered_events,
                covered_measurements: accumulator.covered_measurements,
                quantities,
            })
        })
        .collect::<Result<Vec<_>, ReportError>>()?;

    Ok(SubscriptionReport {
        rows,
        coverage,
        ingest_freshness: load_ingest_freshness(conn)?,
    })
}

/// A subscription-included event with each measurement's current head outcome.
#[derive(Debug)]
struct SubscriptionEvent {
    occurred_at_ms: Option<i64>,
    provider: Option<String>,
    account_ref: Option<String>,
    measurements: Vec<SubscriptionMeasurement>,
}

#[derive(Debug)]
struct SubscriptionMeasurement {
    role: String,
    unit: String,
    quantity_units: Option<i64>,
    quantity_exponent: Option<i32>,
    /// `None` when no pricing pass has examined the measurement.
    head_outcome: Option<String>,
}

impl SubscriptionEvent {
    fn is_fully_priced(&self) -> bool {
        !self.measurements.is_empty()
            && !self.measurements.iter().any(|measurement| {
                blocks_event_total(
                    measurement.head_outcome.as_deref(),
                    measurement.quantity_units == Some(0),
                )
            })
    }
}

/// Reads only the subscription-included events, with their measurements and
/// current charge heads, in one statement. The report never looks at any
/// other event, so the charge-basis filter runs in SQL.
fn load_subscription_events(
    conn: &Connection,
    filter: Option<&str>,
) -> Result<Vec<SubscriptionEvent>, ReportError> {
    // Ordered by the event's rowid, which is the order the outer table scan
    // already produces, so every event's measurement rows arrive together.
    let filter = filter.map_or_else(String::new, |filter| format!(" AND ({filter})"));
    let mut statement = conn.prepare(&format!(
        "SELECT e.rowid,e.occurred_at_ms,e.provider,e.account_ref,
                m.role,m.unit,m.quantity_units,m.quantity_exponent,head.outcome
         FROM usage_event e
         LEFT JOIN measurement m ON m.event_key=e.event_key
         LEFT JOIN priced_charge head
           ON head.event_key=m.event_key AND head.role=m.role AND head.unit=m.unit
          AND NOT EXISTS (
              SELECT 1 FROM priced_charge child WHERE child.supersedes=head.charge_key
          )
         WHERE e.charge_basis='subscription_included'{filter}
         ORDER BY e.rowid"
    ))?;
    let mut rows = statement.query([])?;
    let mut events: Vec<SubscriptionEvent> = Vec::new();
    let mut last_rowid = None;
    while let Some(row) = rows.next()? {
        let rowid: i64 = row.get(0)?;
        if last_rowid != Some(rowid) {
            last_rowid = Some(rowid);
            events.push(SubscriptionEvent {
                occurred_at_ms: row.get(1)?,
                provider: row.get(2)?,
                account_ref: row.get(3)?,
                measurements: Vec::new(),
            });
        }
        // A NULL role is the LEFT JOIN's row for an event with no measurements.
        let Some(role) = row.get::<_, Option<String>>(4)? else {
            continue;
        };
        let event = events.last_mut().ok_or(ReportError::Integrity(
            "stored row references an event absent from the ledger",
        ))?;
        event.measurements.push(SubscriptionMeasurement {
            role,
            unit: row.get(5)?,
            quantity_units: row.get(6)?,
            quantity_exponent: row.get(7)?,
            head_outcome: row.get(8)?,
        });
    }
    Ok(events)
}

fn load_subscriptions(conn: &Connection) -> Result<Vec<SubscriptionView>, ReportError> {
    let mut statement = conn.prepare(
        "SELECT account_ref,provider,recurring_cost_minor,amount_exponent,period,
                effective_from_ms,effective_to_ms
         FROM account_subscription ORDER BY provider,account_ref,effective_from_ms",
    )?;
    let rows = statement.query_map([], |row| {
        let period = match row.get::<_, String>(4)?.as_str() {
            "month" => SubscriptionPeriod::Month,
            "year" => SubscriptionPeriod::Year,
            _ => return Err(rusqlite::Error::InvalidQuery),
        };
        Ok(SubscriptionView {
            account_ref: row.get(0)?,
            provider: row.get(1)?,
            recurring_cost_minor: row.get(2)?,
            amount_exponent: u32::try_from(row.get::<_, i64>(3)?)
                .map_err(|_| rusqlite::Error::InvalidQuery)?,
            period,
            effective_from_ms: row.get(5)?,
            effective_to_ms: row.get(6)?,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(ReportError::Store)
}

fn subscription_period_bounds(
    plan: &SubscriptionView,
    instant_ms: i64,
) -> Result<(i64, i64), ReportError> {
    let anchor = Utc
        .timestamp_millis_opt(plan.effective_from_ms)
        .single()
        .ok_or(ReportError::Calendar)?;
    if instant_ms < plan.effective_from_ms {
        return Ok((
            plan.effective_from_ms,
            add_subscription_period(anchor, plan.period, 1)?.timestamp_millis(),
        ));
    }
    let instant = Utc
        .timestamp_millis_opt(instant_ms)
        .single()
        .ok_or(ReportError::Calendar)?;
    let instant_month = i32::try_from(instant.month()).map_err(|_| ReportError::Calendar)?;
    let anchor_month = i32::try_from(anchor.month()).map_err(|_| ReportError::Calendar)?;
    let months = (instant.year() - anchor.year()) * 12 + instant_month - anchor_month;
    let months_per_period = match plan.period {
        SubscriptionPeriod::Month => 1,
        SubscriptionPeriod::Year => 12,
    };
    let mut ordinal =
        u32::try_from(months / months_per_period).map_err(|_| ReportError::Calendar)?;
    // Month distance can overshoot when the billing day/time has not arrived.
    // Every boundary still clamps from the original anchor, not the last boundary.
    let mut start = add_subscription_period(anchor, plan.period, ordinal)?;
    if instant < start {
        ordinal = ordinal.checked_sub(1).ok_or(ReportError::Calendar)?;
        start = add_subscription_period(anchor, plan.period, ordinal)?;
    }
    let end = add_subscription_period(
        anchor,
        plan.period,
        ordinal.checked_add(1).ok_or(ReportError::Calendar)?,
    )?;
    Ok((start.timestamp_millis(), end.timestamp_millis()))
}

fn add_subscription_period(
    instant: DateTime<Utc>,
    period: SubscriptionPeriod,
    ordinal: u32,
) -> Result<DateTime<Utc>, ReportError> {
    let months = ordinal
        .checked_mul(match period {
            SubscriptionPeriod::Month => 1,
            SubscriptionPeriod::Year => 12,
        })
        .ok_or(ReportError::Calendar)?;
    instant
        .checked_add_months(Months::new(months))
        .ok_or(ReportError::Calendar)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkClassReportRequest {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExcludedSample {
    pub events: u64,
    pub of_events: u64,
    pub reasons: BTreeMap<String, u64>,
}

/// One row per `(producer, work_class, currency)` triple: unlike currencies
/// cannot share a cost distribution. Work-class labels are each
/// producer's own taxonomy, so the same label string from two producers is two
/// rows, never one, and a label is reported byte-for-byte as stored: no case
/// folding, trimming or aliasing. Events without a label form their own row
/// per producer (`work_class: null`, and separately `""` if a producer stored
/// an empty string).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkClassRow {
    pub producer: String,
    pub work_class: Option<String>,
    pub currency: Option<String>,
    pub events_total: u64,
    pub cost_sample_n: u64,
    pub excluded_from_sample: ExcludedSample,
    pub p50_nanos: Option<i64>,
    pub p90_nanos: Option<i64>,
}

/// How much of one producer's events carry a work-class label: the
/// denominator a reader needs before treating that producer's labelled rows
/// as representative of all its events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProducerLabelCoverage {
    pub producer: String,
    pub events_total: u64,
    /// Events whose `work_class` is present and not the empty string.
    pub events_labelled: u64,
    /// Events whose `work_class` is absent or the empty string;
    /// `events_labelled + events_unlabelled == events_total`.
    pub events_unlabelled: u64,
    /// OBSERVED FROM THE STORED EVENTS, NOT DECLARED BY THE PRODUCER: true
    /// when at least one of this producer's events, over all time, carries a
    /// label, false when none ever has. A producer that does not report labels
    /// and one that does but has not yet sent one are indistinguishable here;
    /// a single labelled event flips it to true.
    pub reports_labels: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkClassReport {
    /// Sorted by producer, then work class, then currency.
    pub rows: Vec<WorkClassRow>,
    /// One entry per producer with any event, sorted by producer. For each
    /// producer, the `events_total` of its rows sums to its entry's
    /// `events_total`.
    pub producers: Vec<ProducerLabelCoverage>,
}

#[derive(Debug, Default)]
struct LabelCounts {
    events_total: u64,
    events_labelled: u64,
}

#[derive(Debug, Default)]
struct WorkClassAccumulator {
    events_total: u64,
    sample: Vec<i64>,
    excluded: FastMap<Exclusion, u64>,
    currency_unknown: u64,
}

/// Read work-class distributions in one caller-owned transaction/snapshot.
pub fn work_class_report(
    conn: &Connection,
    request: &WorkClassReportRequest,
) -> Result<WorkClassReport, ReportError> {
    if let Ok(Some(report)) = hourly::workclass(conn) {
        return Ok(report);
    }
    work_class_report_base(conn, request)
}

fn work_class_report_base(
    conn: &Connection,
    _request: &WorkClassReportRequest,
) -> Result<WorkClassReport, ReportError> {
    let ledger = load_ledger(conn, &[Grouping::Producer, Grouping::WorkClass])?;
    let mut grouped: FastMap<(u32, Option<u32>, Option<u32>), WorkClassAccumulator> =
        FastMap::default();
    let mut coverage: FastMap<u32, LabelCounts> = FastMap::default();
    for event in &ledger.events {
        // `usage_event.producer` is NOT NULL; a row without one is malformed.
        let producer = event.dimensions[Grouping::Producer as usize]
            .ok_or(ReportError::Store(rusqlite::Error::InvalidQuery))?;
        let work_class = event.dimensions[Grouping::WorkClass as usize];
        let counts = coverage.entry(producer).or_default();
        counts.events_total = counts
            .events_total
            .checked_add(1)
            .ok_or(ReportError::Arithmetic)?;
        if work_class.is_some_and(|label| !ledger.values.name(label).is_empty()) {
            counts.events_labelled = counts
                .events_labelled
                .checked_add(1)
                .ok_or(ReportError::Arithmetic)?;
        }
        let accumulator = grouped
            .entry((producer, work_class, event.currency))
            .or_default();
        accumulator.events_total = accumulator
            .events_total
            .checked_add(1)
            .ok_or(ReportError::Arithmetic)?;
        let complete = event.occurred_at_ms.is_some() && event.is_fully_priced();
        match (complete, event.currency) {
            (true, Some(_)) => {
                accumulator.sample.push(event.current_head_total()?);
            }
            // An event whose every measurement is a DECLINED ZERO has an exact
            // total of zero and no currency: no head ever priced, so nothing
            // ever stated the unit of account. Its total is complete, so it is
            // not unpriced, but a zero in no currency cannot enter a
            // currency-scoped distribution -- it would arrive wearing whatever
            // currency its neighbours happen to use. It is excluded under its
            // own name instead.
            (true, None) => {
                accumulator.currency_unknown = accumulator
                    .currency_unknown
                    .checked_add(1)
                    .ok_or(ReportError::Arithmetic)?;
            }
            (false, _) => {
                add_count(
                    accumulator
                        .excluded
                        .entry(event.exclusion_reason())
                        .or_default(),
                    1,
                )?;
            }
        }
    }
    let mut grouped = grouped
        .into_iter()
        .map(|((producer, work_class, currency), accumulator)| {
            (
                (
                    ledger.values.name(producer).to_string(),
                    ledger.values.optional_name(work_class),
                    ledger.values.optional_name(currency),
                ),
                accumulator,
            )
        })
        .collect::<Vec<_>>();
    grouped.sort_by(|left, right| left.0.cmp(&right.0));
    let mut producers = coverage
        .into_iter()
        .map(|(producer, counts)| {
            Ok(ProducerLabelCoverage {
                producer: ledger.values.name(producer).to_string(),
                events_total: counts.events_total,
                events_labelled: counts.events_labelled,
                events_unlabelled: counts
                    .events_total
                    .checked_sub(counts.events_labelled)
                    .ok_or(ReportError::Arithmetic)?,
                reports_labels: counts.events_labelled > 0,
            })
        })
        .collect::<Result<Vec<_>, ReportError>>()?;
    producers.sort_by(|left, right| left.producer.cmp(&right.producer));
    let rows = grouped
        .into_iter()
        .map(|((producer, work_class, currency), mut accumulator)| {
            accumulator.sample.sort_unstable();
            let cost_sample_n = to_u64(accumulator.sample.len())?;
            let excluded_events = accumulator
                .events_total
                .checked_sub(cost_sample_n)
                .ok_or(ReportError::Arithmetic)?;
            let mut reasons = render_exclusions(accumulator.excluded, &ledger.reasons)?;
            if accumulator.currency_unknown > 0 {
                let entry = reasons.entry("currency_unknown".into()).or_default();
                *entry = entry
                    .checked_add(accumulator.currency_unknown)
                    .ok_or(ReportError::Arithmetic)?;
            }
            Ok(WorkClassRow {
                producer,
                work_class,
                currency,
                events_total: accumulator.events_total,
                cost_sample_n,
                excluded_from_sample: ExcludedSample {
                    events: excluded_events,
                    of_events: accumulator.events_total,
                    reasons,
                },
                p50_nanos: nearest_rank(&accumulator.sample, 50),
                p90_nanos: nearest_rank(&accumulator.sample, 90),
            })
        })
        .collect::<Result<Vec<_>, ReportError>>()?;
    Ok(WorkClassReport { rows, producers })
}

fn nearest_rank(sorted: &[i64], percentile: usize) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = percentile.checked_mul(sorted.len())?.div_ceil(100).max(1);
    sorted.get(rank - 1).copied()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerCoverageRequest {}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProviderCapacityReading {
    pub provider: String,
    pub api_provider: Option<String>,
    pub pool_key: String,
    pub slot_kind: String,
    pub used_percent: f64,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CapacityCoverage {
    pub labeled_windows: Vec<CapacityWindow>,
    pub provider_readings: Vec<ProviderCapacityReading>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SetAsideCoverage {
    pub records: u64,
    pub newest_last_seen_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LedgerCoverage {
    pub set_aside_by_source: BTreeMap<String, SetAsideCoverage>,
    pub events_total: u64,
    pub events_unpriced: u64,
    pub events_undated: u64,
    pub zero_measurement_events: u64,
    /// WHY EVENTS WERE EXCLUDED, one entry per excluded EVENT, out of
    /// `events_total`; sums to `events_unpriced`.
    pub unpriced_events_by_reason: BTreeMap<String, u64>,
    /// WHICH MEASUREMENTS A PRICING PASS DECLINED, one entry per MEASUREMENT,
    /// out of `rate_staleness.measurements_total`. A DIFFERENT DENOMINATOR
    /// from the map above: a declined measurement of zero units leaves its
    /// event's total exact, so the event is priced and appears in no reason
    /// count above while its reason is counted here.
    pub unpriced_measurements_by_reason: BTreeMap<String, u64>,
    pub ingest_freshness: IngestFreshness,
    pub rate_staleness: RateStaleness,
    pub capacity: CapacityCoverage,
}

/// Read ledger and operational coverage in one caller-owned transaction/snapshot.
pub fn ledger_coverage(
    conn: &Connection,
    request: &LedgerCoverageRequest,
    now_ms: i64,
) -> Result<LedgerCoverage, ReportError> {
    if let Ok(Some(report)) = hourly::coverage(conn, now_ms) {
        return Ok(report);
    }
    ledger_coverage_base(conn, request, now_ms)
}

fn ledger_coverage_base(
    conn: &Connection,
    _request: &LedgerCoverageRequest,
    now_ms: i64,
) -> Result<LedgerCoverage, ReportError> {
    let ledger = load_ledger(conn, &[])?;
    let mut events_undated = 0_u64;
    let mut population = EventPopulation::default();
    for event in &ledger.events {
        if event.occurred_at_ms.is_none() {
            add_count(&mut events_undated, 1)?;
        }
        population.observe(event)?;
    }

    // Labeled windows carry custodian-vouched account identity. Provider-current
    // readings structurally carry none, so they are read into a separate type and
    // never unioned or joined into an account-shaped result.
    let labeled_windows = crate::capacity::windows(conn)?;
    let provider_readings = load_provider_capacity_readings(conn)?;
    Ok(LedgerCoverage {
        set_aside_by_source: load_set_aside_coverage(conn)?,
        events_total: population.events_total,
        events_unpriced: population
            .events_total
            .checked_sub(population.events_priced)
            .ok_or(ReportError::Arithmetic)?,
        events_undated,
        zero_measurement_events: population.zero_measurement_events,
        unpriced_events_by_reason: render_exclusions(
            population.unpriced_events_by_reason,
            &ledger.reasons,
        )?,
        unpriced_measurements_by_reason: render_reason_counts(
            population.unpriced_measurements_by_reason,
            &ledger.reasons,
        ),
        ingest_freshness: load_ingest_freshness(conn)?,
        rate_staleness: ledger.rate_staleness(now_ms),
        capacity: CapacityCoverage {
            labeled_windows,
            provider_readings,
        },
    })
}

fn load_set_aside_coverage(
    conn: &Connection,
) -> Result<BTreeMap<String, SetAsideCoverage>, ReportError> {
    let mut stmt = conn.prepare("SELECT source.source_key,COUNT(evidence.locator),MAX(evidence.last_seen_ms)
        FROM (SELECT source_key FROM source_runtime UNION SELECT source_key FROM set_aside_record) source
        LEFT JOIN set_aside_record evidence ON evidence.source_key=source.source_key
        GROUP BY source.source_key ORDER BY source.source_key")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            SetAsideCoverage {
                records: row.get(1)?,
                newest_last_seen_ms: row.get(2)?,
            },
        ))
    })?;
    Ok(rows.collect::<Result<_, _>>()?)
}

fn load_provider_capacity_readings(
    conn: &Connection,
) -> Result<Vec<ProviderCapacityReading>, ReportError> {
    let mut statement = conn.prepare(
        "SELECT provider,api_provider,pool_key,slot_kind,used_percent,observed_at_ms
         FROM capacity_provider_reading ORDER BY provider,pool_key",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(ProviderCapacityReading {
            provider: row.get(0)?,
            api_provider: row.get(1)?,
            pool_key: row.get(2)?,
            slot_kind: row.get(3)?,
            used_percent: row.get(4)?,
            observed_at_ms: row.get(5)?,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(ReportError::Store)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChargeExplainRequest {
    pub event_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExplainedCharge {
    pub pricing_generation: u64,
    pub outcome: String,
    pub charge_nanos: Option<i64>,
    pub unpriced_reason: Option<String>,
    pub rate_units: Option<i64>,
    pub rate_exponent: Option<i32>,
    pub catalog_source: Option<String>,
    pub resolved_at_ms: Option<i64>,
    pub catalog_entry: Option<Value>,
    pub supersedes: Option<u64>,
    pub origin: String,
    pub recorded_at_ms: i64,
    pub current: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExplainedMeasurement {
    pub measurement: Measurement,
    pub charges: Vec<ExplainedCharge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExplainedAdjustment {
    pub adjustment_id: String,
    pub currency: String,
    pub amount_nanos: i64,
    pub reason: String,
    pub occurred_at_ms: i64,
    pub recorded_at_ms: i64,
    pub origin: String,
    pub operator: Option<String>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExplainedTransition {
    pub from_state: String,
    pub to_state: String,
    pub reason: String,
    pub resolved_by: String,
    pub occurred_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ChargeExplanation {
    pub event: UsageEvent,
    pub measurements: Vec<ExplainedMeasurement>,
    pub adjustments: Vec<ExplainedAdjustment>,
    pub transition: Option<ExplainedTransition>,
}

/// Explain an event's charge chains in one caller-owned transaction/snapshot.
pub fn explain_charge(
    conn: &Connection,
    request: &ChargeExplainRequest,
) -> Result<ChargeExplanation, ReportError> {
    if request.event_id.is_empty() {
        return Err(ReportError::Refused(ReportRefusal::MissingEventId));
    }
    let event = load_event(conn, &request.event_id)?.ok_or_else(|| {
        ReportError::Refused(ReportRefusal::EventNotFound(request.event_id.clone()))
    })?;
    let measurements = load_explained_measurements(conn, &request.event_id)?;
    let adjustments = load_explained_adjustments(conn, &request.event_id)?;
    let transition = conn
        .query_row(
            "SELECT from_state,to_state,reason,resolved_by,occurred_at_ms
             FROM chargeability_transition WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
            [&request.event_id],
            |row| {
                Ok(ExplainedTransition {
                    from_state: row.get(0)?,
                    to_state: row.get(1)?,
                    reason: row.get(2)?,
                    resolved_by: row.get(3)?,
                    occurred_at_ms: row.get(4)?,
                })
            },
        )
        .optional()?;
    Ok(ChargeExplanation {
        event,
        measurements,
        adjustments,
        transition,
    })
}

fn load_explained_measurements(
    conn: &Connection,
    event_id: &str,
) -> Result<Vec<ExplainedMeasurement>, ReportError> {
    let mut measurement_statement = conn.prepare(
        "SELECT event.event_id,m.role,m.unit,m.kind,m.source,m.quantity_units,m.quantity_exponent,m.event_key
         FROM usage_event event JOIN measurement m USING(event_key)
         WHERE event.event_id=?1 ORDER BY m.role,m.unit",
    )?;
    let measurements = measurement_statement
        .query_map([event_id], |row| {
            Ok((read_measurement(row)?, row.get::<_, i64>(7)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut output = Vec::with_capacity(measurements.len());
    let mut charge_statement = conn.prepare(
        "SELECT charge.pricing_generation,charge.outcome,
                    charge.charge_nanos,charge.unpriced_reason,charge.rate_units,
                    charge.rate_exponent,charge.catalog_source,charge.resolved_at_ms,
                    charge.catalog_entry,predecessor.pricing_generation,charge.origin,charge.recorded_at_ms,
                    NOT EXISTS (SELECT 1 FROM priced_charge child
                                WHERE child.supersedes=charge.charge_key)
             FROM priced_charge charge
             LEFT JOIN priced_charge predecessor ON predecessor.charge_key=charge.supersedes
             WHERE charge.event_key=?1 AND charge.role=?2 AND charge.unit=?3
             ORDER BY charge.pricing_generation",
    )?;
    for (measurement, event_key) in measurements {
        let charges = charge_statement
            .query_map(
                params![event_key, measurement.role, measurement.unit],
                |row| {
                    let catalog_entry = row
                        .get::<_, Option<String>>(8)?
                        .map(|raw| serde_json::from_str(&raw))
                        .transpose()
                        .map_err(|_| rusqlite::Error::InvalidQuery)?;
                    Ok(ExplainedCharge {
                        pricing_generation: u64::try_from(row.get::<_, i64>(0)?)
                            .map_err(|_| rusqlite::Error::InvalidQuery)?,
                        outcome: row.get(1)?,
                        charge_nanos: row.get(2)?,
                        unpriced_reason: row.get(3)?,
                        rate_units: row.get(4)?,
                        rate_exponent: row.get(5)?,
                        catalog_source: row.get(6)?,
                        resolved_at_ms: row.get(7)?,
                        catalog_entry,
                        supersedes: row
                            .get::<_, Option<i64>>(9)?
                            .map(u64::try_from)
                            .transpose()
                            .map_err(|_| rusqlite::Error::InvalidQuery)?,
                        origin: row.get(10)?,
                        recorded_at_ms: row.get(11)?,
                        current: row.get(12)?,
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        output.push(ExplainedMeasurement {
            measurement,
            charges,
        });
    }
    Ok(output)
}

fn load_explained_adjustments(
    conn: &Connection,
    event_id: &str,
) -> Result<Vec<ExplainedAdjustment>, ReportError> {
    let mut statement = conn.prepare(
        "SELECT adjustment_id,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,
                origin,operator,note
         FROM adjustment WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) ORDER BY recorded_at_ms,adjustment_id",
    )?;
    let rows = statement.query_map([event_id], |row| {
        Ok(ExplainedAdjustment {
            adjustment_id: row.get(0)?,
            currency: row.get(1)?,
            amount_nanos: row.get(2)?,
            reason: row.get(3)?,
            occurred_at_ms: row.get(4)?,
            recorded_at_ms: row.get(5)?,
            origin: row.get(6)?,
            operator: row.get(7)?,
            note: row.get(8)?,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(ReportError::Store)
}

fn load_event(conn: &Connection, event_id: &str) -> Result<Option<UsageEvent>, ReportError> {
    conn.query_row(
        "SELECT event_id,producer,producer_lineage,producer_ref,prior_event_ref,harness,
                project_root,session,run_ref,work_class,occurred_at_ms,emitted_at_ms,
                recorded_at_ms,terminal_status,attempt_ordinal,vendor_request_id,provider,
                model,account_ref,charge_basis,pricing_generation,currency,auth_selection
         FROM usage_event WHERE event_id=?1",
        [event_id],
        |row| {
            let charge_basis = parse_charge_basis(required_text_column(row, 19)?, 19)?;
            let attempt_ordinal = row
                .get::<_, Option<i64>>(14)?
                .map(u32::try_from)
                .transpose()
                .map_err(|_| rusqlite::Error::InvalidQuery)?;
            Ok(UsageEvent {
                event_id: row.get(0)?,
                producer: row.get(1)?,
                producer_lineage: row.get(2)?,
                producer_ref: row.get(3)?,
                prior_event_ref: row.get(4)?,
                harness: row.get(5)?,
                project_root: row.get(6)?,
                session: row.get(7)?,
                run_ref: row.get(8)?,
                work_class: row.get(9)?,
                occurred_at_ms: row.get(10)?,
                emitted_at_ms: row.get(11)?,
                recorded_at_ms: row.get(12)?,
                terminal_status: row.get(13)?,
                attempt_ordinal,
                vendor_request_id: row.get(15)?,
                provider: row.get(16)?,
                model: row.get(17)?,
                account_ref: row.get(18)?,
                auth_selection: row.get(22)?,
                charge_basis,
                pricing_generation: u64::try_from(row.get::<_, i64>(20)?)
                    .map_err(|_| rusqlite::Error::InvalidQuery)?,
                currency: row.get(21)?,
            })
        },
    )
    .optional()
    .map_err(ReportError::Store)
}

fn read_measurement(row: &rusqlite::Row<'_>) -> rusqlite::Result<Measurement> {
    Ok(Measurement {
        event_id: row.get(0)?,
        role: row.get(1)?,
        unit: row.get(2)?,
        kind: row.get(3)?,
        source: row.get(4)?,
        quantity_units: row.get(5)?,
        quantity_exponent: row.get(6)?,
    })
}

fn checked_add(left: i64, right: i64) -> Result<i64, ReportError> {
    left.checked_add(right).ok_or(ReportError::Arithmetic)
}

fn add_count(count: &mut u64, amount: u64) -> Result<(), ReportError> {
    *count = count.checked_add(amount).ok_or(ReportError::Arithmetic)?;
    Ok(())
}

fn to_u64(value: usize) -> Result<u64, ReportError> {
    u64::try_from(value).map_err(|_| ReportError::Arithmetic)
}

fn pow10_i128(exponent: u32) -> Result<i128, ReportError> {
    (0..exponent).try_fold(1_i128, |value, _| {
        value.checked_mul(10).ok_or(ReportError::Arithmetic)
    })
}

// Small fixtures ensure the integer casts in these tests cannot truncate or wrap.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::conn;

    fn event(
        conn: &Connection,
        id: &str,
        work_class: Option<&str>,
        occurred_at_ms: Option<i64>,
        basis: &str,
    ) {
        conn.execute(
            "INSERT INTO usage_event (
                event_id,producer,producer_lineage,producer_ref,work_class,occurred_at_ms,
                recorded_at_ms,provider,model,account_ref,project_root,charge_basis
             ) VALUES (?1,'producer','lineage',?1,?2,?3,1,'provider','model','account','/raw/project',?4)",
            params![id, work_class, occurred_at_ms, basis],
        )
        .unwrap();
    }

    fn measurement(conn: &Connection, event_id: &str, role: &str, unit: &str, quantity: i64) {
        conn.execute(
            "INSERT INTO measurement
                (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),?2,?3,'billed','producer',?4,0)",
            params![event_id, role, unit, quantity],
        )
        .unwrap();
    }

    /// A measurement whose size the producer never stated: both quantity
    /// columns absent together, which is what the store holds when the wire
    /// record carried no quantity at all. NOT a zero, and never written as
    /// one.
    fn measurement_without_quantity(conn: &Connection, event_id: &str, role: &str, unit: &str) {
        conn.execute(
            "INSERT INTO measurement
                (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),?2,?3,'billed','producer',NULL,NULL)",
            params![event_id, role, unit],
        )
        .unwrap();
    }

    /// A head a pricing pass wrote after examining the measurement and
    /// declining it, carrying the reason it named. The event's generation is
    /// advanced without a currency, because a pass that priced nothing never
    /// learned one. The head takes the EVENT'S OWN charge basis, as a real
    /// pass does, so this works on a subscription event as well as a metered
    /// one -- pricing declines under either basis.
    fn unpriced_head(conn: &Connection, event_id: &str, role: &str, unit: &str, reason: &str) {
        conn.execute(
            "UPDATE usage_event SET pricing_generation=1 WHERE event_id=?1",
            [event_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO priced_charge (
                charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,
                basis_covered,unpriced_reason,catalog_source,resolved_at_ms,origin,recorded_at_ms
             )
             SELECT ?1,event_key,?3,?4,1,'unpriced',charge_basis,0,?5,'fusiform',100,
                    'pricing_pass',100
             FROM usage_event WHERE event_id=?2",
            params![rusqlite::types::Null, event_id, role, unit, reason],
        )
        .unwrap();
    }

    /// BROCA's drill at export_seq 122092: three measurements the catalog
    /// priced and one it declined. `cache_read` IS ALSO ZERO AND PRICES
    /// NORMALLY -- fusiform serves a cache-read rate -- so this fixture can
    /// tell "a declined zero does not block" apart from "no zero ever
    /// blocks", which a fixture whose every zero was declined could not.
    fn drill_event(
        conn: &Connection,
        id: &str,
        reasoning_quantity: Option<i64>,
        reasoning_reason: &str,
    ) {
        event(
            conn,
            id,
            Some("class"),
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(conn, id, "cache_read", "token", 0);
        measurement(conn, id, "input", "token", 13);
        measurement(conn, id, "output", "token", 6);
        match reasoning_quantity {
            Some(quantity) => measurement(conn, id, "reasoning", "token", quantity),
            None => measurement_without_quantity(conn, id, "reasoning", "token"),
        }
        priced_heads(
            conn,
            id,
            &[
                ("cache_read", "token", 0),
                ("input", "token", 130),
                ("output", "token", 60),
            ],
        );
        unpriced_head(conn, id, "reasoning", "token", reasoning_reason);
    }

    /// What `drill_event`'s three priced heads come to, whatever its fourth
    /// measurement does to the event's verdict.
    const DRILL_TOTAL_NANOS: i64 = 190;

    fn priced_heads(conn: &Connection, event_id: &str, charges: &[(&str, &str, i64)]) {
        conn.execute(
            "UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_id=?1",
            [event_id],
        )
        .unwrap();
        for (role, unit, charge) in charges {
            conn.execute(
                "INSERT INTO priced_charge (
                    charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,
                    basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,
                    catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms
                 ) VALUES (?1,(SELECT event_key FROM usage_event WHERE event_id=?2),?3,?4,1,'priced','metered_api',0,'USD',?5,1,9,
                           'unknown','fusiform',100,'{}','pricing_pass',100)",
                params![
                    rusqlite::types::Null,
                    event_id,
                    role,
                    unit,
                    charge
                ],
            )
            .unwrap();
        }
    }

    fn covered_heads(conn: &Connection, event_id: &str, measurements: &[(&str, &str)]) {
        conn.execute(
            "UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_id=?1",
            [event_id],
        )
        .unwrap();
        for (role, unit) in measurements {
            conn.execute(
                "INSERT INTO priced_charge (
                    charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,
                    basis_covered,currency,charge_nanos,origin,recorded_at_ms
                 ) VALUES (?1,(SELECT event_key FROM usage_event WHERE event_id=?2),?3,?4,1,'covered','subscription_included',1,'USD',0,'pricing_pass',100)",
                params![
                    rusqlite::types::Null,
                    event_id,
                    role,
                    unit
                ],
            )
            .unwrap();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn seed_source_runtime(
        conn: &Connection,
        source_key: &str,
        consumed: Option<i64>,
        high_water: Option<i64>,
        last_success_ms: Option<i64>,
        pending_known: i64,
        pending_unknown: bool,
    ) {
        conn.execute(
            "UPDATE source_runtime SET source_key=?1,source_module=?2,
                 lineage='inst-1:1',
                  consumed_ordinal=?3,high_water_ordinal=?4,last_success_ms=?5,
                 pending_known=?6,pending_unknown=?7
             WHERE source_key='broca'",
            rusqlite::params![
                source_key,
                source_key,
                consumed,
                high_water,
                last_success_ms,
                pending_known,
                i64::from(pending_unknown),
            ],
        )
        .unwrap();
    }

    fn request(timezone: &str) -> ReportRequest {
        ReportRequest {
            grouping: Vec::new(),
            grain: "day".into(),
            timezone: timezone.into(),
        }
    }

    fn utc_ms(year: i32, month: u32, day: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, 0, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis()
    }

    #[test]
    fn subscription_events_are_counted_once_across_measurements_and_plan_intervals() {
        let conn = conn();
        subscription_plan(&conn, "provider", utc_ms(2000, 1, 31), "month");
        conn.execute(
            "UPDATE account_subscription SET effective_to_ms=?1 WHERE provider='provider'",
            [utc_ms(2024, 4, 1)],
        )
        .unwrap();
        subscription_plan(&conn, "provider", utc_ms(2024, 4, 1), "year");
        for (id, instant) in [
            ("first", utc_ms(2024, 3, 30)),
            ("same-bucket", utc_ms(2024, 3, 30)),
            ("second", utc_ms(2024, 3, 31)),
            ("renewed", utc_ms(2024, 4, 1)),
        ] {
            event(&conn, id, None, Some(instant), "subscription_included");
            measurement(&conn, id, "input", "token", 2);
            measurement(&conn, id, "output", "token", 3);
            covered_heads(&conn, id, &[("input", "token"), ("output", "token")]);
        }
        let report = subscription_report(&conn, &SubscriptionReportRequest {}).unwrap();
        assert_eq!(report.coverage.events_total, 4);
        assert_eq!(report.rows.len(), 3);
        assert_eq!(report.rows[0].period_start_ms, utc_ms(2024, 2, 29));
        assert_eq!(report.rows[0].period_end_ms, utc_ms(2024, 3, 31));
        assert_eq!(report.rows[1].period_start_ms, utc_ms(2024, 3, 31));
        assert_eq!(report.rows[1].period_end_ms, utc_ms(2024, 4, 1));
        assert_eq!(report.rows[2].period_start_ms, utc_ms(2024, 4, 1));
        assert_eq!(report.rows[2].period_end_ms, utc_ms(2025, 4, 1));
        for (row, count) in report.rows.into_iter().zip([2, 1, 1]) {
            assert_eq!(row.covered_events, count);
            assert_eq!(row.covered_measurements, count * 2);
            assert_eq!(row.quantities.len(), 2);
            assert_eq!(row.quantities[0].quantity_units, count as i64 * 2);
            assert_eq!(row.quantities[1].quantity_units, count as i64 * 3);
        }
    }

    #[test]
    fn subscription_period_lookup_matches_anchored_walk() {
        for anchor in [
            utc_ms(1999, 1, 31),
            utc_ms(2000, 2, 29),
            utc_ms(2024, 12, 30) + 12345,
        ] {
            for period in [SubscriptionPeriod::Month, SubscriptionPeriod::Year] {
                let plan = SubscriptionView {
                    account_ref: "account".into(),
                    provider: "provider".into(),
                    recurring_cost_minor: 10,
                    amount_exponent: 0,
                    period,
                    effective_from_ms: anchor,
                    effective_to_ms: None,
                };
                let anchor_date = Utc.timestamp_millis_opt(anchor).unwrap();
                let mut previous = anchor;
                for ordinal in 1..=300 {
                    let months = match period {
                        SubscriptionPeriod::Month => ordinal,
                        SubscriptionPeriod::Year => ordinal * 12,
                    };
                    let end = anchor_date
                        .checked_add_months(Months::new(months))
                        .unwrap()
                        .timestamp_millis();
                    for instant in [previous, end - 1] {
                        assert_eq!(
                            subscription_period_bounds(&plan, instant).unwrap(),
                            (previous, end)
                        );
                    }
                    previous = end;
                }
            }
        }
    }

    #[test]
    fn report_count_overflow_preserves_the_previous_count() {
        let mut count = u64::MAX;
        assert!(matches!(
            add_count(&mut count, 1),
            Err(ReportError::Arithmetic)
        ));
        assert_eq!(count, u64::MAX);
        count = 5;
        add_count(&mut count, 3).unwrap();
        assert_eq!(count, 8);
    }

    #[test]
    fn explanation_rebinds_each_measurement_without_mixing_charge_histories() {
        let conn = conn();
        event(
            &conn,
            "explain",
            None,
            Some(utc_ms(2024, 1, 1)),
            "metered_api",
        );
        measurement(&conn, "explain", "input", "token", 2);
        measurement(&conn, "explain", "output", "token", 3);
        priced_heads(
            &conn,
            "explain",
            &[("input", "token", 20), ("output", "token", 30)],
        );
        unsafe extern "C" fn count_selects(
            data: *mut std::ffi::c_void,
            action: std::ffi::c_int,
            _first: *const std::ffi::c_char,
            _second: *const std::ffi::c_char,
            _database: *const std::ffi::c_char,
            _trigger: *const std::ffi::c_char,
        ) -> std::ffi::c_int {
            if action == rusqlite::ffi::SQLITE_SELECT {
                // The caller keeps this counter alive until the hook is removed.
                let count = unsafe { &*data.cast::<std::cell::Cell<usize>>() };
                count.set(count.get() + 1);
            }
            rusqlite::ffi::SQLITE_OK
        }
        let prepares = std::cell::Cell::new(0_usize);
        // SQLite authorizes SELECTs while compiling, not while stepping a rebound
        // statement. The connection and counter stay on this thread throughout.
        let handle = unsafe { conn.handle() };
        let registered = unsafe {
            rusqlite::ffi::sqlite3_set_authorizer(
                handle,
                Some(count_selects),
                (&prepares as *const std::cell::Cell<usize>)
                    .cast_mut()
                    .cast(),
            )
        };
        assert_eq!(registered, rusqlite::ffi::SQLITE_OK);
        let measurements = load_explained_measurements(&conn, "explain");
        let removed =
            unsafe { rusqlite::ffi::sqlite3_set_authorizer(handle, None, std::ptr::null_mut()) };
        assert_eq!(removed, rusqlite::ffi::SQLITE_OK);
        let measurements = measurements.unwrap();
        // One measurement SELECT plus the charge SELECT and its head subquery.
        assert_eq!(prepares.get(), 3);
        assert_eq!(measurements.len(), 2);
        assert_eq!(measurements[0].measurement.role, "input");
        assert_eq!(measurements[1].measurement.role, "output");
        for (measurement, amount) in measurements.iter().zip([20, 30]) {
            assert_eq!(measurement.charges.len(), 1);
            assert_eq!(measurement.charges[0].charge_nanos, Some(amount));
            assert!(measurement.charges[0].current);
        }
    }

    #[test]
    fn subscription_index_preserves_first_match_across_gaps_and_overlaps() {
        let plans =
            [(10, Some(20)), (30, Some(50)), (40, Some(45)), (60, None)].map(|(from, to)| {
                SubscriptionView {
                    provider: "provider".into(),
                    account_ref: "account".into(),
                    recurring_cost_minor: from,
                    amount_exponent: 0,
                    period: SubscriptionPeriod::Month,
                    effective_from_ms: from,
                    effective_to_ms: to,
                }
            });
        let index = SubscriptionIndex::new(&plans);
        for (instant, expected) in [
            (9, None),
            (10, Some(10)),
            (19, Some(10)),
            (20, None),
            (29, None),
            (30, Some(30)),
            (40, Some(30)),
            (45, Some(30)),
            (50, None),
            (60, Some(60)),
            (i64::MAX, Some(60)),
        ] {
            assert_eq!(
                index
                    .find("provider", "account", instant)
                    .map(|plan| plan.recurring_cost_minor),
                expected
            );
        }
        assert!(index.find("other", "account", 15).is_none());
        assert!(index.find("provider", "other", 15).is_none());
    }

    #[test]
    fn corrupt_priced_head_is_refused_without_rendering_zero_money() {
        for column in ["charge_nanos", "resolved_at_ms"] {
            let conn = conn();
            event(
                &conn,
                "priced",
                None,
                Some(utc_ms(2024, 1, 1)),
                "metered_api",
            );
            measurement(&conn, "priced", "input", "token", 1);
            conn.execute("UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_id='priced'", []).unwrap();
            // Simulate a damaged store by bypassing CHECKs only for this insert.
            conn.pragma_update(None, "ignore_check_constraints", true)
                .unwrap();
            let (amount, resolved) = if column == "charge_nanos" {
                (None, Some(100_i64))
            } else {
                (Some(10_i64), None)
            };
            conn.execute(
                "INSERT INTO priced_charge (charge_key,event_key,role,unit,pricing_generation,
                 outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,
                 catalog_source,resolved_at_ms,origin,recorded_at_ms)
                 VALUES (1,(SELECT event_key FROM usage_event WHERE event_id='priced'),'input','token',1,'priced','metered_api',0,'USD',?1,
                         1,0,'catalog',?2,'pricing_pass',100)",
                params![amount, resolved],
            )
            .unwrap();
            conn.pragma_update(None, "ignore_check_constraints", false)
                .unwrap();
            let result = spend_report(&conn, &request("UTC"), 100);
            assert!(
                result.is_err(),
                "a missing {column} must not produce a spend reply: {result:?}"
            );
            assert!(
                format!("{result:?}").contains(column),
                "the fault must name {column}: {result:?}"
            );
            let stored: i64 = conn
                .query_row(
                    "SELECT count(*) FROM priced_charge WHERE charge_key=1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(stored, 1, "reporting never repairs immutable charges");
        }
    }

    #[test]
    #[ignore = "manual read-path timing fixture"]
    fn report_read_path_performance() {
        use std::time::Instant;
        let conn = conn();
        let sqlite: String = conn
            .query_row("SELECT sqlite_version()", [], |row| row.get(0))
            .unwrap();
        event(
            &conn,
            "explain",
            None,
            Some(utc_ms(2024, 1, 1)),
            "metered_api",
        );
        for index in 0..1000 {
            measurement(&conn, "explain", "input", &format!("unit-{index:04}"), 1);
        }
        let start = Instant::now();
        for _ in 0..20 {
            let measurements = load_explained_measurements(&conn, "explain").unwrap();
            assert_eq!(measurements.len(), 1000);
            assert!(measurements
                .iter()
                .all(|measurement| measurement.charges.is_empty()));
        }
        eprintln!(
            "SQLite {sqlite}; explain: 20 reads x 1000 measurements, {:?}",
            start.elapsed()
        );

        for index in 0..100 {
            subscription_plan(
                &conn,
                &format!("other-{index}"),
                utc_ms(2000, 1, 31),
                "month",
            );
        }
        subscription_plan(&conn, "provider", utc_ms(2000, 1, 31), "month");
        for index in 0..5000 {
            let id = format!("covered-event-{index:04}");
            event(
                &conn,
                &id,
                None,
                Some(utc_ms(2025, 3, 30)),
                "subscription_included",
            );
            measurement(&conn, &id, "input", "token", 2);
            measurement(&conn, &id, "output", "token", 3);
            covered_heads(&conn, &id, &[("input", "token"), ("output", "token")]);
        }
        let start = Instant::now();
        for _ in 0..5 {
            let report = subscription_report(&conn, &SubscriptionReportRequest {}).unwrap();
            assert_eq!(report.rows.len(), 1);
            assert_eq!(report.rows[0].covered_events, 5000);
            assert_eq!(report.rows[0].covered_measurements, 10000);
            assert_eq!(report.rows[0].period_start_ms, utc_ms(2025, 2, 28));
            assert_eq!(report.rows[0].quantities[0].quantity_units, 10000);
        }
        eprintln!("subscription: 5 reads x 5000 events / 10000 measurements / 101 plans / 25-year anchor, {:?}", start.elapsed());
    }

    #[test]
    fn regression_midnight_gap_report_starts_at_first_existing_local_instant() {
        let conn = conn();
        for (id, day) in [("before", 7), ("gap", 8)] {
            event(
                &conn,
                id,
                Some("class"),
                Some(utc_ms(2024, 9, day) + 12 * 60 * 60 * 1000),
                "metered_api",
            );
            measurement(&conn, id, "input", "token", 1);
            priced_heads(&conn, id, &[("input", "token", 10)]);
        }
        let report = spend_report(&conn, &request("America/Santiago"), utc_ms(2024, 9, 9))
            .expect("a missing local midnight must not fail the spend report");
        assert_eq!(report.rows.len(), 2);
        let gap = &report.rows[1];
        assert_eq!(gap.bucket_start_ms, utc_ms(2024, 9, 8) + 4 * 60 * 60 * 1000);
        let zone: Tz = "America/Santiago".parse().unwrap();
        assert_eq!(
            zone.timestamp_millis_opt(gap.bucket_start_ms)
                .single()
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            "2024-09-08 01:00:00"
        );
        assert_eq!(report.rows[0].bucket_end_ms, gap.bucket_start_ms);
        assert_eq!(gap.bucket_end_ms - gap.bucket_start_ms, 23 * 60 * 60 * 1000);
        assert_eq!(gap.total_nanos, 10);
        assert_eq!(gap.events_priced, 1);
    }

    #[test]
    fn regression_skipped_local_date_ends_at_next_existing_day() {
        let conn = conn();
        for (id, instant) in [
            ("before", utc_ms(2011, 12, 29) + 12 * 60 * 60 * 1000),
            ("after", utc_ms(2011, 12, 30) + 12 * 60 * 60 * 1000),
        ] {
            event(&conn, id, None, Some(instant), "metered_api");
            measurement(&conn, id, "input", "token", 1);
            priced_heads(&conn, id, &[("input", "token", 10)]);
        }
        let report = spend_report(&conn, &request("Pacific/Apia"), utc_ms(2012, 1, 1))
            .expect("a skipped local date must not fail the neighbouring bucket");
        assert_eq!(report.rows.len(), 2);
        let transition = utc_ms(2011, 12, 30) + 10 * 60 * 60 * 1000;
        assert_eq!(report.rows[0].bucket_end_ms, transition);
        assert_eq!(report.rows[1].bucket_start_ms, transition);
        assert_eq!(report.rows[0].total_nanos, 10);
        assert_eq!(report.rows[1].total_nanos, 10);
    }

    #[test]
    fn regression_overflowing_ingest_lag_is_unknown_on_the_wire() {
        for (consumed, high_water) in [(i64::MIN, i64::MAX), (i64::MAX, i64::MIN)] {
            let conn = conn();
            // Exercise corrupt runtime coordinates even if a later schema refuses them.
            conn.execute_batch("PRAGMA ignore_check_constraints=ON")
                .unwrap();
            seed_source_runtime(
                &conn,
                "broca",
                Some(consumed),
                Some(high_water),
                Some(100),
                0,
                false,
            );
            let report = spend_report(&conn, &request("UTC"), 100).unwrap();
            let wire = serde_json::to_value(report).unwrap();
            let producer = &wire["ingest_freshness"]["producers"][0];
            assert_eq!(producer["consumed_ordinal"], consumed);
            assert_eq!(producer["high_water_ordinal"], high_water);
            assert!(
                producer["lag"].is_null(),
                "an unrepresentable lag must be unknown"
            );
        }
    }

    /// The subscription report's coverage block over one fixture: a covered
    /// event, an attributed event no plan covers, an undated event with no
    /// account, and, when `with_unattributed` is set, a dated event with no
    /// account. Returns the served JSON so the assertion reads the wire field.
    fn subscription_coverage_with(with_unattributed: bool) -> serde_json::Value {
        let conn = conn();
        subscription_plan(&conn, "provider", utc_ms(2024, 1, 1), "month");
        event(
            &conn,
            "covered",
            None,
            Some(utc_ms(2024, 1, 15)),
            "subscription_included",
        );
        measurement(&conn, "covered", "input", "token", 1);
        covered_heads(&conn, "covered", &[("input", "token")]);
        event(
            &conn,
            "before-plan",
            None,
            Some(utc_ms(2023, 12, 15)),
            "subscription_included",
        );
        measurement(&conn, "before-plan", "input", "token", 1);
        unpriced_head(
            &conn,
            "before-plan",
            "input",
            "token",
            "no_subscription_in_force",
        );
        let mut accountless = vec![("undated-no-account", None)];
        if with_unattributed {
            accountless.push(("no-account", Some(utc_ms(2024, 1, 15))));
        }
        for (id, occurred_at_ms) in accountless {
            conn.execute(
                "INSERT INTO usage_event
                    (event_id,producer,producer_lineage,producer_ref,occurred_at_ms,
                     recorded_at_ms,provider,charge_basis)
                 VALUES (?1,'producer','lineage',?1,?2,1,'provider','subscription_included')",
                params![id, occurred_at_ms],
            )
            .unwrap();
            measurement(&conn, id, "input", "token", 1);
            unpriced_head(&conn, id, "input", "token", "no_subscription_in_force");
        }
        let report = subscription_report(&conn, &SubscriptionReportRequest {}).unwrap();
        serde_json::to_value(report).unwrap()["coverage"].clone()
    }

    /// A dated subscription event with no account is counted as unattributed
    /// on its own line, and still inside `events_unpriced`, whose meaning and
    /// value do not change. The undated one without an account is counted
    /// only as undated.
    #[test]
    fn subscription_report_counts_dated_events_with_no_account_separately() {
        let coverage = subscription_coverage_with(true);
        assert_eq!(coverage["events_without_account"], 1);
        assert_eq!(coverage["events_unpriced"], 2);
        assert_eq!(coverage["events_undated"], 1);
        assert_eq!(coverage["events_total"], 4);
    }

    /// The same fixture with every dated event attributed reports the field
    /// as zero, present rather than omitted.
    #[test]
    fn subscription_report_counts_zero_unattributed_events_when_all_carry_an_account() {
        let coverage = subscription_coverage_with(false);
        assert_eq!(coverage["events_without_account"], 0);
        assert_eq!(coverage["events_unpriced"], 1);
        assert_eq!(coverage["events_undated"], 1);
        assert_eq!(coverage["events_total"], 3);
    }

    fn subscription_plan(conn: &Connection, provider: &str, start: i64, period: &str) {
        conn.execute(
            "INSERT INTO account_subscription
                (account_ref,provider,plan_name,recurring_cost_minor,currency,amount_exponent,
                 period,effective_from_ms,effective_to_ms,operator)
             VALUES ('account',?1,'plan',10,'USD',0,?2,?3,NULL,NULL)",
            params![provider, period, start],
        )
        .unwrap();
    }

    #[test]
    fn regression_monthly_subscription_keeps_original_anniversary_across_a_year() {
        let conn = conn();
        subscription_plan(&conn, "provider", utc_ms(2024, 1, 31), "month");
        let boundaries = [
            utc_ms(2024, 1, 31),
            utc_ms(2024, 2, 29),
            utc_ms(2024, 3, 31),
            utc_ms(2024, 4, 30),
            utc_ms(2024, 5, 31),
            utc_ms(2024, 6, 30),
            utc_ms(2024, 7, 31),
            utc_ms(2024, 8, 31),
            utc_ms(2024, 9, 30),
            utc_ms(2024, 10, 31),
            utc_ms(2024, 11, 30),
            utc_ms(2024, 12, 31),
            utc_ms(2025, 1, 31),
        ];
        for (index, pair) in boundaries.windows(2).enumerate() {
            for (edge, instant) in [("start", pair[0]), ("end", pair[1] - 1)] {
                let id = format!("covered-{index}-{edge}");
                event(&conn, &id, None, Some(instant), "subscription_included");
                measurement(&conn, &id, "input", "token", 1);
                covered_heads(&conn, &id, &[("input", "token")]);
            }
        }
        let report = subscription_report(&conn, &SubscriptionReportRequest {}).unwrap();
        assert_eq!(report.rows.len(), 12);
        for (row, pair) in report.rows.iter().zip(boundaries.windows(2)) {
            assert_eq!((row.period_start_ms, row.period_end_ms), (pair[0], pair[1]));
            assert_eq!(row.covered_events, 2);
            assert_eq!(row.quantities[0].quantity_units, 2);
        }
    }

    #[test]
    fn regression_yearly_subscription_returns_to_leap_day_anniversary() {
        let conn = conn();
        subscription_plan(&conn, "provider", utc_ms(2020, 2, 29), "year");
        let boundaries = [
            utc_ms(2020, 2, 29),
            utc_ms(2021, 2, 28),
            utc_ms(2022, 2, 28),
            utc_ms(2023, 2, 28),
            utc_ms(2024, 2, 29),
            utc_ms(2025, 2, 28),
        ];
        for (index, instant) in boundaries[..5].iter().enumerate() {
            let id = format!("covered-{index}");
            event(&conn, &id, None, Some(*instant), "subscription_included");
            measurement(&conn, &id, "input", "token", 1);
            covered_heads(&conn, &id, &[("input", "token")]);
        }
        let report = subscription_report(&conn, &SubscriptionReportRequest {}).unwrap();
        assert_eq!(report.rows.len(), 5);
        for (row, pair) in report.rows.iter().zip(boundaries.windows(2)) {
            assert_eq!((row.period_start_ms, row.period_end_ms), (pair[0], pair[1]));
            assert_eq!(row.covered_events, 1);
        }
    }

    #[test]
    fn regression_subscription_wire_rows_keep_provider_identity() {
        let conn = conn();
        for provider in ["provider-a", "provider-b"] {
            subscription_plan(&conn, provider, utc_ms(2024, 1, 1), "month");
            conn.execute(
                "INSERT INTO usage_event
                    (event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,
                     provider,account_ref,charge_basis)
                 VALUES (?1,'producer','lineage',?1,?2,1,?1,'account','subscription_included')",
                params![provider, utc_ms(2024, 1, 15)],
            )
            .unwrap();
            measurement(&conn, provider, "input", "token", 1);
            covered_heads(&conn, provider, &[("input", "token")]);
        }
        let report = subscription_report(&conn, &SubscriptionReportRequest {}).unwrap();
        let wire = serde_json::to_value(report).unwrap();
        let rows = wire["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["provider"], "provider-a");
        assert_eq!(rows[1]["provider"], "provider-b");
        for row in rows {
            assert_eq!(row["account_ref"], "account");
            assert_eq!(row["covered_events"], 1);
            assert_eq!(row["period_start_ms"], utc_ms(2024, 1, 1));
        }
    }

    #[test]
    fn regression_workclass_partitions_currencies_like_spend_report() {
        let conn = conn();
        for (currency, amount) in [("USD", 100), ("JPY", 200)] {
            event(&conn, currency, Some("class"), Some(100), "metered_api");
            measurement(&conn, currency, "input", "token", 1);
            conn.execute(
                "UPDATE usage_event SET pricing_generation=1,currency=?1 WHERE event_id=?1",
                [currency],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO priced_charge
                    (charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,
                     basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,
                     catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms)
                 VALUES (NULL,(SELECT event_key FROM usage_event WHERE event_id=?1),'input','token',1,'priced','metered_api',0,?1,?2,1,9,
                         'unknown','fusiform',100,'{}','pricing_pass',100)",
                params![currency, amount],
            )
            .unwrap();
        }
        event(&conn, "unknown", Some("class"), Some(100), "metered_api");
        let spend = spend_report(&conn, &request("UTC"), 100).unwrap();
        let work = work_class_report(&conn, &WorkClassReportRequest {})
            .expect("unlike currencies must be separate distributions, not fail the report");
        assert_eq!(work.rows.len(), 3);
        for row in &work.rows {
            let spend_row = spend
                .rows
                .iter()
                .find(|other| other.currency == row.currency)
                .unwrap();
            assert_eq!(row.events_total, spend_row.events_total);
            assert_eq!(row.cost_sample_n, spend_row.cost_sample_n);
            match row.currency.as_deref() {
                Some("USD") => assert_eq!((row.p50_nanos, row.p90_nanos), (Some(100), Some(100))),
                Some("JPY") => assert_eq!((row.p50_nanos, row.p90_nanos), (Some(200), Some(200))),
                None => {
                    assert_eq!(row.p50_nanos, None);
                    assert_eq!(row.excluded_from_sample.reasons["no_measurements"], 1);
                }
                other => panic!("unexpected currency {other:?}"),
            }
        }
        assert_eq!(work.producers[0].events_total, 3);
        assert_eq!(work.rows.iter().map(|row| row.events_total).sum::<u64>(), 3);
    }

    #[test]
    fn timezone_argument_changes_bucket_boundaries_for_the_same_request() {
        let conn = conn();
        event(
            &conn,
            "event",
            Some("class"),
            Some(1_772_344_800_000),
            "metered_api",
        );
        measurement(&conn, "event", "input", "token", 1);
        priced_heads(&conn, "event", &[("input", "token", 10)]);
        let utc = spend_report(&conn, &request("UTC"), 2_000_000_000_000).unwrap();
        let los_angeles =
            spend_report(&conn, &request("America/Los_Angeles"), 2_000_000_000_000).unwrap();
        assert_ne!(
            utc.rows[0].bucket_start_ms,
            los_angeles.rows[0].bucket_start_ms
        );
    }

    #[test]
    fn calendar_boundaries_are_wall_clock_across_both_dst_transitions() {
        let spring_instant = Utc
            .with_ymd_and_hms(2026, 3, 8, 7, 30, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        let fall_instant = Utc
            .with_ymd_and_hms(2026, 11, 1, 6, 30, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        let spring = bucket_bounds(
            spring_instant,
            "America/New_York".parse().unwrap(),
            Grain::Day,
        )
        .unwrap();
        let fall = bucket_bounds(
            fall_instant,
            "America/New_York".parse().unwrap(),
            Grain::Day,
        )
        .unwrap();
        assert_eq!(spring.1 - spring.0, 23 * 60 * 60 * 1000);
        assert_eq!(fall.1 - fall.0, 25 * 60 * 60 * 1000);
    }

    #[test]
    fn never_drained_producer_reports_unavailable_never_zero_lag() {
        // Ruling 141: the migration SEEDS a producer row with null coordinates, so
        // a fresh store has a row and no drain. That is NEVER DRAINED, not current
        // -- and a presence check would call it Available with zero lag, asserting
        // agreement with a producer nobody has ever contacted.
        let conn = conn();
        let report = spend_report(&conn, &request("UTC"), 2_000).unwrap();
        assert_eq!(report.ingest_freshness.status, Availability::Unavailable);
        assert_eq!(
            report.ingest_freshness.reason,
            Some(INGEST_FRESHNESS_REASON)
        );
        assert_eq!(report.ingest_freshness.producers.len(), 1);
        let producer = &report.ingest_freshness.producers[0];
        assert_eq!(producer.last_success_ms, None);
        assert_eq!(producer.consumed_ordinal, None);
        assert_eq!(producer.high_water_ordinal, None);
        assert_eq!(producer.lag, None);
        assert_eq!(producer.pending_known, 0);
        assert!(producer.pending_unknown);
    }

    #[test]
    fn drained_producer_reports_available_with_its_own_lag() {
        let conn = conn();
        seed_source_runtime(&conn, "broca", Some(90), Some(100), Some(1_700), 4, false);
        let report = spend_report(&conn, &request("UTC"), 2_000).unwrap();
        let freshness = &report.ingest_freshness;
        assert_eq!(freshness.status, Availability::Available);
        assert_eq!(freshness.reason, None);
        assert_eq!(freshness.producers.len(), 1);
        assert_eq!(freshness.producers[0].source_key, "broca");
        assert_eq!(freshness.producers[0].lag, Some(10));
        assert_eq!(freshness.producers[0].last_success_ms, Some(1_700));
        assert!(!freshness.producers[0].pending_unknown);
    }

    #[test]
    fn drained_no_sequence_producer_reports_available_with_absent_ordinals_and_lag() {
        let conn = conn();
        conn.execute(
            "UPDATE source_runtime SET lineage=NULL,consumed_ordinal=NULL,
             high_water_ordinal=NULL,last_success_ms=1700,pending_known=0,pending_unknown=0
             WHERE source_key='broca'",
            [],
        )
        .unwrap();

        let freshness = spend_report(&conn, &request("UTC"), 2_000)
            .unwrap()
            .ingest_freshness;
        assert_eq!(freshness.status, Availability::Available);
        assert_eq!(freshness.reason, None);
        assert_eq!(freshness.producers.len(), 1);
        let producer = &freshness.producers[0];
        assert_eq!(producer.last_success_ms, Some(1_700));
        assert_eq!(producer.lineage, None);
        assert_eq!(producer.consumed_ordinal, None);
        assert_eq!(producer.high_water_ordinal, None);
        assert_eq!(producer.lag, None);
        assert_eq!(producer.pending_known, 0);
        assert!(!producer.pending_unknown);
    }

    /// The contract puts `ingest_freshness` on every money-over-window read, not
    /// only on `spend.report`. A read that carries the field but never consults
    /// `source_runtime` renders `unavailable` on a drained store -- a claim about
    /// the producer that is false, wearing the shape of the honest default. The
    /// unavailable-on-fresh-store test cannot see that, because the default and
    /// the real read agree there; only a drained fixture separates them.
    #[test]
    fn one_never_drained_producer_keeps_aggregate_freshness_unavailable() {
        let conn = conn();
        conn.execute(
            "INSERT INTO source_runtime (source_key,source_module,desired_generation,lineage,consumed_ordinal,high_water_ordinal,last_success_ms,pending_known,pending_unknown) VALUES ('opencode','opencode',(SELECT generation FROM desired_config WHERE id=1),'opencode-lineage',1,1,1700,0,0)",
            [],
        )
        .unwrap();
        let freshness = spend_report(&conn, &request("UTC"), 2_000)
            .unwrap()
            .ingest_freshness;
        assert_eq!(freshness.status, Availability::Unavailable);
        assert_eq!(freshness.producers.len(), 2);
        assert_eq!(freshness.producers[0].source_key, "broca");
        assert_eq!(freshness.producers[0].lag, None);
        assert_eq!(freshness.producers[1].source_key, "opencode");
    }

    #[test]
    fn ingest_freshness_reports_broca_and_opencode_independently() {
        let conn = conn();
        conn.execute(
            "INSERT INTO source_runtime (source_key,source_module,desired_generation,lineage,consumed_ordinal,high_water_ordinal,last_success_ms,pending_known,pending_unknown) VALUES ('opencode','opencode',(SELECT generation FROM desired_config WHERE id=1),'opencode-lineage',1,1,1700,0,0)",
            [],
        )
        .unwrap();
        seed_source_runtime(&conn, "broca", Some(90), Some(100), Some(1_700), 4, false);
        let freshness = spend_report(&conn, &request("UTC"), 2_000)
            .unwrap()
            .ingest_freshness;
        assert_eq!(freshness.producers.len(), 2);
        assert_eq!(freshness.producers[0].source_key, "broca");
        assert_eq!(freshness.producers[0].lineage.as_deref(), Some("inst-1:1"));
        assert_eq!(freshness.producers[1].source_key, "opencode");
        assert_eq!(
            freshness.producers[1].lineage.as_deref(),
            Some("opencode-lineage")
        );
    }

    #[test]
    fn coverage_and_subscription_reads_consult_producer_state_not_the_default() {
        let conn = conn();
        seed_source_runtime(&conn, "broca", Some(90), Some(100), Some(1_700), 4, false);

        let coverage = ledger_coverage(&conn, &LedgerCoverageRequest {}, 2_000).unwrap();
        assert_eq!(coverage.ingest_freshness.status, Availability::Available);
        assert_eq!(coverage.ingest_freshness.producers[0].lag, Some(10));

        let subscription = subscription_report(&conn, &SubscriptionReportRequest {}).unwrap();
        assert_eq!(
            subscription.ingest_freshness.status,
            Availability::Available
        );
        assert_eq!(subscription.ingest_freshness.producers[0].lag, Some(10));
    }

    #[test]
    fn partial_producer_state_is_unrepresentable_without_effect() {
        // Sequence-bearing producers must supply lineage, both ordinals, and a
        // successful-drain time together. A file reader may omit the entire
        // sequence axis after a successful drain, but no producer may expose only
        // part of that axis, so the report never computes lag from a partial row.
        let conn = conn();
        let before: i64 = conn
            .query_row(
                "SELECT count(*) FROM source_runtime WHERE consumed_ordinal IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let result = conn.execute(
            "UPDATE source_runtime SET lineage='inst', consumed_ordinal=5 WHERE source_key='broca'",
            [],
        );
        assert!(result.is_err());
        let after: i64 = conn
            .query_row(
                "SELECT count(*) FROM source_runtime WHERE consumed_ordinal IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn undated_events_are_report_level_and_appear_in_no_bucket() {
        let conn = conn();
        event(&conn, "undated", Some("class"), None, "metered_api");
        let report = spend_report(&conn, &request("UTC"), 100).unwrap();
        assert!(report.rows.is_empty());
        assert_eq!(report.undated.events_undated, 1);
        assert_eq!(report.undated.events_report_total, 1);
        assert_eq!(report.undated.reasons["missing_occurred_at"], 1);
    }

    #[test]
    fn unknown_grouping_refuses_without_changing_stored_state() {
        let conn = conn();
        event(&conn, "event", None, Some(1), "metered_api");
        let before: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        let error = spend_report(
            &conn,
            &ReportRequest {
                grouping: vec!["invented".into()],
                ..request("UTC")
            },
            10,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ReportError::Refused(ReportRefusal::UnknownGrouping(_))
        ));
        let after: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn unknown_timezone_refuses_without_changing_stored_state() {
        let conn = conn();
        event(&conn, "event", None, Some(1), "metered_api");
        let before: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert!(matches!(
            spend_report(&conn, &request("not/a-zone"), 10),
            Err(ReportError::Refused(ReportRefusal::UnknownTimezone(_)))
        ));
        let after: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn required_grain_and_timezone_are_refused_by_request_decoding_without_effect() {
        let conn = conn();
        event(&conn, "event", None, Some(1), "metered_api");
        let before: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert!(serde_json::from_value::<ReportRequest>(serde_json::json!({
            "grouping": [], "timezone": "UTC"
        }))
        .is_err());
        assert!(serde_json::from_value::<ReportRequest>(serde_json::json!({
            "grouping": [], "grain": "day"
        }))
        .is_err());
        let after: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn project_and_work_class_grouping_are_verbatim_and_machine_limited() {
        let conn = conn();
        event(
            &conn,
            "event",
            Some("SideKick/Exact Label"),
            Some(1_700_000_000_000),
            "metered_api",
        );
        let report = spend_report(
            &conn,
            &ReportRequest {
                grouping: vec!["work_class".into(), "project".into()],
                ..request("UTC")
            },
            1_800_000_000_000,
        )
        .unwrap();
        assert_eq!(
            report.rows[0].grouping["work_class"].as_deref(),
            Some("SideKick/Exact Label")
        );
        assert_eq!(
            report.rows[0].grouping["project"].as_deref(),
            Some("/raw/project")
        );
        assert_eq!(report.hierarchy_resolution, Some(Availability::Unavailable));
    }

    #[test]
    fn auth_selection_grouping_splits_events_by_credential() {
        let conn = conn();
        // Two events with different auth_selection pins, same account_ref.
        conn.execute_batch(
            "INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,work_class,occurred_at_ms,
                  recorded_at_ms,provider,model,account_ref,auth_selection,project_root,charge_basis)
             VALUES
                 ('ev-a','producer','lineage','ev-a','class',1_700_000_000_000,1,
                  'provider','model','account','apikey:openai:astro','/raw/project','metered_api'),
                 ('ev-b','producer','lineage','ev-b','class',1_700_000_000_000,1,
                  'provider','model','account','apikey:xai:work','/raw/project','metered_api');",
        )
        .unwrap();
        let report = spend_report(
            &conn,
            &ReportRequest {
                grouping: vec!["auth_selection".into()],
                ..request("UTC")
            },
            1_800_000_000_000,
        )
        .unwrap();
        assert_eq!(report.rows.len(), 2);
        let pins: Vec<Option<&str>> = report
            .rows
            .iter()
            .map(|row| row.grouping["auth_selection"].as_deref())
            .collect();
        assert!(pins.contains(&Some("apikey:openai:astro")));
        assert!(pins.contains(&Some("apikey:xai:work")));
    }

    #[test]
    fn iso_week_buckets_start_on_monday() {
        let instant = Utc
            .with_ymd_and_hms(2026, 1, 1, 12, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        let bounds = bucket_bounds(instant, "UTC".parse().unwrap(), Grain::Week).unwrap();
        assert_eq!(
            bounds.0,
            Utc.with_ymd_and_hms(2025, 12, 29, 0, 0, 0)
                .single()
                .unwrap()
                .timestamp_millis()
        );
        assert_eq!(bounds.1 - bounds.0, 7 * 24 * 60 * 60 * 1000);
    }

    #[test]
    fn duplicate_grouping_and_unknown_grain_refuse_without_changing_stored_state() {
        let conn = conn();
        event(&conn, "event", None, Some(1), "metered_api");
        let before: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert!(matches!(
            spend_report(
                &conn,
                &ReportRequest {
                    grouping: vec!["provider".into(), "provider".into()],
                    ..request("UTC")
                },
                10
            ),
            Err(ReportError::Refused(ReportRefusal::DuplicateGrouping(_)))
        ));
        assert!(matches!(
            spend_report(
                &conn,
                &ReportRequest {
                    grain: "hour".into(),
                    ..request("UTC")
                },
                10
            ),
            Err(ReportError::Refused(ReportRefusal::UnknownGrain(_)))
        ));
        let after: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn partial_event_reports_partial_total_with_unpriced_population_and_headline_reconciliation() {
        let conn = conn();
        event(
            &conn,
            "mixed",
            Some("class"),
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(&conn, "mixed", "input", "token", 10);
        measurement(&conn, "mixed", "output", "token", 20);
        conn.execute(
            "UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_id='mixed'",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO priced_charge (
                charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,
                basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,
                catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms
             ) VALUES (NULL,(SELECT event_key FROM usage_event WHERE event_id='mixed'),'input','token',1,'priced','metered_api',0,'USD',50,
                       1,9,'unknown','fusiform',100,'{}','pricing_pass',100)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO priced_charge (
                charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,
                basis_covered,unpriced_reason,origin,recorded_at_ms
             ) VALUES (NULL,(SELECT event_key FROM usage_event WHERE event_id='mixed'),'output','token',1,'unpriced','metered_api',0,
                       'no_rate','pricing_pass',100)",
            [],
        )
        .unwrap();
        let report = spend_report(
            &conn,
            &ReportRequest {
                grouping: vec!["provider".into()],
                ..request("UTC")
            },
            1_800_000_000_000,
        )
        .unwrap();
        assert_eq!(report.rows[0].total_nanos, 50);
        assert_eq!(report.rows[0].events_total, 1);
        assert_eq!(report.rows[0].events_priced, 0);
        assert_eq!(report.rows[0].events_unpriced, 1);
        assert_eq!(report.rows[0].cost_sample_n, 0);
        assert_eq!(report.rows[0].unpriced_events_by_reason["no_rate"], 1);
        assert_eq!(report.totals[0].total_nanos, 50);
        assert_eq!(report.totals[0].events_total, 1);
        assert_eq!(report.totals[0].events_unpriced, 1);
    }

    /// A mixed-basis bucket is required to exercise the basis partition. The
    /// covered event's usage is a real zero; its event-level adjustment gives
    /// the non-metered side a non-zero value so summing every event into
    /// `metered_nanos` cannot pass accidentally. An all-covered or all-metered
    /// fixture cannot defend that predicate.
    #[test]
    fn mixed_covered_and_metered_bucket_reports_only_the_metered_part() {
        let conn = conn();
        event(
            &conn,
            "covered",
            Some("class"),
            Some(1_700_000_000_000),
            "subscription_included",
        );
        measurement(&conn, "covered", "input", "token", 125);
        covered_heads(&conn, "covered", &[("input", "token")]);
        conn.execute(
            "INSERT INTO adjustment (
                adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,
                recorded_at_ms,origin
             ) VALUES ('covered-adjustment',(SELECT event_key FROM usage_event WHERE event_id='covered'),'USD',30,'goodwill',
                       1700000000000,1700000000001,'correction_ingest')",
            [],
        )
        .unwrap();

        event(
            &conn,
            "metered",
            Some("class"),
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(&conn, "metered", "input", "token", 10);
        priced_heads(&conn, "metered", &[("input", "token", 70)]);

        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows.len(), 1);
        assert_eq!(report.rows[0].events_total, 2);
        assert_eq!(report.rows[0].total_nanos, 70 + 30);
        assert_eq!(report.rows[0].metered_nanos, 70);
    }

    /// Five named tests hold five distinct arms. The mixed-basis test includes
    /// `MeteredApi` and excludes valued `SubscriptionIncluded`; the
    /// covered-only test proves that excluded zero is still a populated row;
    /// the headline test holds row-to-total reconciliation; the unpriced test
    /// excludes heads with no charge; and this test excludes `Unknown` even
    /// when its event has an adjustment. Without this third basis value,
    /// `!= SubscriptionIncluded` is indistinguishable from the closed
    /// `MeteredApi` predicate.
    #[test]
    fn unknown_basis_adjustment_is_not_metered_value() {
        let conn = conn();
        event(
            &conn,
            "unknown",
            Some("class"),
            Some(1_700_000_000_000),
            "unknown",
        );
        measurement(&conn, "unknown", "input", "token", 300);
        conn.execute(
            "UPDATE usage_event SET currency='USD' WHERE event_id='unknown'",
            [],
        )
        .unwrap();
        unpriced_head(&conn, "unknown", "input", "token", "charge_basis_unknown");
        conn.execute(
            "INSERT INTO adjustment (
                adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,
                recorded_at_ms,origin
             ) VALUES ('unknown-adjustment',(SELECT event_key FROM usage_event WHERE event_id='unknown'),'USD',30,'goodwill',
                       1700000000000,1700000000001,'correction_ingest')",
            [],
        )
        .unwrap();

        event(
            &conn,
            "metered",
            Some("class"),
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(&conn, "metered", "input", "token", 10);
        priced_heads(&conn, "metered", &[("input", "token", 70)]);

        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows.len(), 1);
        assert_eq!(report.rows[0].events_total, 2);
        assert_eq!(report.rows[0].events_unpriced, 1);
        assert_eq!(report.rows[0].total_nanos, 70 + 30);
        assert_eq!(report.rows[0].metered_nanos, 70);
    }

    /// A covered-only bucket has a measured zero rather than an absent sum.
    /// The non-zero quantity and population assertions prove the event reached
    /// the row. This test cannot replace the mixed-basis predicate test above:
    /// both correct and indiscriminate metered sums are zero here.
    #[test]
    fn covered_only_bucket_reports_a_nonempty_real_zero() {
        let conn = conn();
        event(
            &conn,
            "covered",
            Some("class"),
            Some(1_700_000_000_000),
            "subscription_included",
        );
        measurement(&conn, "covered", "input", "token", 125);
        covered_heads(&conn, "covered", &[("input", "token")]);

        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows.len(), 1);
        assert_eq!(report.rows[0].events_total, 1);
        assert_eq!(report.rows[0].measurements_total, 1);
        assert_eq!(report.rows[0].total_nanos, 0);
        assert_eq!(report.rows[0].metered_nanos, 0);
    }

    /// Row correctness does not prove headline correctness because each value
    /// is accumulated on a separate path after rows are materialized.
    #[test]
    fn headline_metered_nanos_equals_the_sum_of_its_rows() {
        let conn = conn();
        event(
            &conn,
            "first",
            Some("class"),
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(&conn, "first", "input", "token", 4);
        priced_heads(&conn, "first", &[("input", "token", 40)]);
        event(
            &conn,
            "second",
            Some("class"),
            Some(1_700_086_400_000),
            "metered_api",
        );
        measurement(&conn, "second", "input", "token", 6);
        priced_heads(&conn, "second", &[("input", "token", 60)]);

        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows.len(), 2);
        assert_eq!(report.rows[0].metered_nanos, 40);
        assert_eq!(report.rows[1].metered_nanos, 60);
        assert_eq!(report.totals[0].metered_nanos, 100);
        assert_eq!(
            report.totals[0].metered_nanos,
            report.rows.iter().map(|row| row.metered_nanos).sum::<i64>()
        );
    }

    /// Basis alone does not manufacture value: an unpriced metered event has
    /// no charge and contributes to neither catalog-value field. The event and
    /// measurement assertions distinguish that zero from an empty report.
    #[test]
    fn unpriced_metered_event_contributes_to_neither_value() {
        let conn = conn();
        event(
            &conn,
            "unpriced",
            Some("class"),
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(&conn, "unpriced", "input", "token", 500);
        unpriced_head(&conn, "unpriced", "input", "token", "no_rate");

        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows.len(), 1);
        assert_eq!(report.rows[0].events_total, 1);
        assert_eq!(report.rows[0].events_unpriced, 1);
        assert_eq!(report.rows[0].measurements_total, 1);
        assert_eq!(report.rows[0].total_nanos, 0);
        assert_eq!(report.rows[0].metered_nanos, 0);
    }

    /// A measurement no pricing pass has examined -- the catalog was pending,
    /// or the event landed after the last tick -- is a different claim from
    /// every unpriced_reason, which all mean a pass ran and declined. The
    /// report names it separately so a reader can tell a cold catalog from a
    /// catalog that looked and found nothing.
    #[test]
    fn an_unexamined_measurement_reports_not_yet_priced_rather_than_unpriced() {
        let conn = conn();
        event(
            &conn,
            "pending",
            None,
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(&conn, "pending", "input", "token", 10);
        let report = spend_report(
            &conn,
            &ReportRequest {
                grouping: vec!["provider".into()],
                ..request("UTC")
            },
            1_800_000_000_000,
        )
        .unwrap();
        assert_eq!(report.rows[0].events_unpriced, 1);
        assert_eq!(
            report.rows[0]
                .unpriced_events_by_reason
                .get("not_yet_priced"),
            Some(&1)
        );
        assert_eq!(
            report.rows[0].unpriced_events_by_reason.get("unpriced"),
            None
        );
    }

    /// Ruling 144, first arm: a declined measurement of zero stated units
    /// contributes exactly zero whatever rate was missing, so the event's
    /// total is exact and its verdict must say so. The fixture's `cache_read`
    /// is zero too and prices normally, so a predicate that waved through
    /// every zero would pass this test for the wrong reason.
    #[test]
    fn a_declined_zero_quantity_measurement_does_not_make_its_event_unpriced() {
        let conn = conn();
        drill_event(&conn, "drill", Some(0), "no_rate");
        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows[0].events_priced, 1);
        assert_eq!(report.rows[0].events_unpriced, 0);
        assert!(report.rows[0].unpriced_events_by_reason.is_empty());
        assert_eq!(report.rows[0].measurements_total, 4);
        assert_eq!(report.rows[0].total_nanos, DRILL_TOTAL_NANOS);
    }

    /// Ruling 144, second arm: units the producer did state and the catalog
    /// could not price leave the event's total UNKNOWN, and an unknown total
    /// is what unpriced means.
    #[test]
    fn a_declined_non_zero_quantity_measurement_still_makes_its_event_unpriced() {
        let conn = conn();
        drill_event(&conn, "drill", Some(6), "no_rate");
        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows[0].events_priced, 0);
        assert_eq!(report.rows[0].events_unpriced, 1);
        assert_eq!(report.rows[0].unpriced_events_by_reason["no_rate"], 1);
    }

    /// Ruling 144, third arm, and THE ONE NO TOTAL CAN CATCH. Were an absent
    /// quantity read as a zero, the measurement would price at zero,
    /// contribute nothing to any sum, and every total in this report would
    /// stay correct while the completeness verdict lied -- an audit of the
    /// money finds nothing. So this asserts what became of the MEASUREMENT:
    /// it is counted among the declined and never among the priced.
    #[test]
    fn a_declined_measurement_with_no_stated_quantity_makes_its_event_unpriced() {
        let conn = conn();
        drill_event(&conn, "drill", None, "missing_quantity");
        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(
            report.rows[0].unpriced_measurements_by_reason["missing_quantity"],
            1
        );
        assert_eq!(report.rate_staleness.measurements_total, 4);
        assert_eq!(report.rate_staleness.priced_measurements, 3);
        assert_eq!(
            report.rows[0].unpriced_events_by_reason["missing_quantity"],
            1
        );
        assert_eq!(report.rows[0].events_priced, 0);
    }

    /// Ruling 144, fourth arm: a measurement no pass has examined blocks at
    /// ANY quantity, zero included. `not_yet_priced` means nobody looked,
    /// which is a different claim from a pass that looked and declined, and a
    /// stated zero does not answer a question nobody asked.
    #[test]
    fn an_unexamined_measurement_makes_its_event_unpriced_even_at_zero_quantity() {
        let conn = conn();
        event(
            &conn,
            "pending",
            Some("class"),
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(&conn, "pending", "input", "token", 13);
        measurement(&conn, "pending", "reasoning", "token", 0);
        priced_heads(&conn, "pending", &[("input", "token", 130)]);
        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows[0].events_priced, 0);
        assert_eq!(report.rows[0].events_unpriced, 1);
        assert_eq!(
            report.rows[0].unpriced_events_by_reason["not_yet_priced"],
            1
        );
        assert!(report.rows[0].unpriced_measurements_by_reason.is_empty());
    }

    /// The verdict moving must not consume the evidence. This event now reads
    /// priced, so no event-level reason names the catalog gap it sits on; the
    /// per-measurement count is the only place that gap is still visible, on
    /// both surfaces that publish a reason breakdown.
    #[test]
    fn an_event_priced_around_a_declined_zero_still_reports_that_measurement() {
        let conn = conn();
        drill_event(&conn, "drill", Some(0), "no_rate");
        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows[0].events_priced, 1);
        assert!(report.rows[0].unpriced_events_by_reason.is_empty());
        assert_eq!(report.rows[0].unpriced_measurements_by_reason["no_rate"], 1);
        // The fixture's three PRICED measurements must not appear: a priced
        // head answered for its measurement, and one of those answers came
        // back zero. Only the declined measurement is counted, so the map
        // holds exactly the one key.
        assert_eq!(report.rows[0].unpriced_measurements_by_reason.len(), 1);
        let coverage =
            ledger_coverage(&conn, &LedgerCoverageRequest {}, 1_800_000_000_000).unwrap();
        assert_eq!(coverage.events_unpriced, 0);
        assert!(coverage.unpriced_events_by_reason.is_empty());
        assert_eq!(coverage.unpriced_measurements_by_reason["no_rate"], 1);
        assert_eq!(coverage.unpriced_measurements_by_reason.len(), 1);
    }

    /// The decline count is a FILTER, and a filter is only as good as what it
    /// keeps OUT. A covered measurement was priced and the answer was zero --
    /// a real answer -- so it belongs in the population and in no decline
    /// count. Counting covered measurements as declined would leave every
    /// verdict and every total correct while filling this map with every
    /// subscription measurement in the store, which is worse than losing the
    /// count altogether: a map that is present and wrong retires the
    /// suspicion that a missing one would raise. The declined measurement
    /// beside the covered ones is what makes this "that one and not these",
    /// rather than "the map is empty" -- which an all-covered fixture would
    /// satisfy no matter what the filter did.
    #[test]
    fn a_covered_measurement_counts_in_the_population_and_never_as_declined() {
        let conn = conn();
        event(
            &conn,
            "covered",
            Some("class"),
            Some(1_700_000_000_000),
            "subscription_included",
        );
        measurement(&conn, "covered", "input", "token", 100);
        measurement(&conn, "covered", "output", "token", 25);
        measurement(&conn, "covered", "reasoning", "token", 0);
        covered_heads(&conn, "covered", &[("input", "token"), ("output", "token")]);
        unpriced_head(&conn, "covered", "reasoning", "token", "no_rate");
        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows[0].measurements_total, 3);
        assert_eq!(report.rows[0].unpriced_measurements_by_reason["no_rate"], 1);
        assert_eq!(
            report.rows[0]
                .unpriced_measurements_by_reason
                .get("unpriced"),
            None
        );
        assert_eq!(report.rows[0].unpriced_measurements_by_reason.len(), 1);
        let coverage =
            ledger_coverage(&conn, &LedgerCoverageRequest {}, 1_800_000_000_000).unwrap();
        assert_eq!(coverage.rate_staleness.measurements_total, 3);
        assert_eq!(coverage.unpriced_measurements_by_reason["no_rate"], 1);
        assert_eq!(
            coverage.unpriced_measurements_by_reason.get("unpriced"),
            None
        );
        assert_eq!(coverage.unpriced_measurements_by_reason.len(), 1);
    }

    /// The event-level breakdown took the FIRST reason it found, so the second
    /// cause of a two-cause event never reached the surface an operator reads:
    /// they could close the gap they were shown and watch the event stay
    /// excluded for one they were not. Both causes now key the same event, and
    /// each measurement is counted once beside them.
    #[test]
    fn an_event_with_two_distinct_blocking_reasons_reports_both() {
        let conn = conn();
        event(
            &conn,
            "two",
            Some("class"),
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(&conn, "two", "input", "token", 13);
        measurement(&conn, "two", "output", "token", 6);
        unpriced_head(&conn, "two", "input", "token", "no_rate");
        unpriced_head(&conn, "two", "output", "token", "catalog_unconfigured");
        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows[0].unpriced_events_by_reason.len(), 1);
        assert_eq!(
            report.rows[0].unpriced_events_by_reason["catalog_unconfigured+no_rate"],
            1
        );
        assert_eq!(report.rows[0].unpriced_measurements_by_reason["no_rate"], 1);
        assert_eq!(
            report.rows[0].unpriced_measurements_by_reason["catalog_unconfigured"],
            1
        );
    }

    /// The amendment moves a VERDICT, never a number. The same fixture, once
    /// with its declined measurement at zero units and once at six, reports
    /// the same money; only the completeness columns differ.
    #[test]
    fn a_verdict_that_flips_on_a_declined_zero_moves_no_money() {
        let flipped = conn();
        drill_event(&flipped, "drill", Some(0), "no_rate");
        let blocked = conn();
        drill_event(&blocked, "drill", Some(6), "no_rate");
        let flipped_report = spend_report(&flipped, &request("UTC"), 1_800_000_000_000).unwrap();
        let blocked_report = spend_report(&blocked, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(flipped_report.rows[0].total_nanos, DRILL_TOTAL_NANOS);
        assert_eq!(
            flipped_report.rows[0].total_nanos,
            blocked_report.rows[0].total_nanos
        );
        assert_eq!(
            flipped_report.totals[0].total_nanos,
            blocked_report.totals[0].total_nanos
        );
        assert_eq!(flipped_report.rows[0].events_priced, 1);
        assert_eq!(blocked_report.rows[0].events_priced, 0);
    }

    /// An event whose every measurement is a declined zero has an exact total
    /// of zero and NO CURRENCY: nothing priced, so nothing ever stated the
    /// unit of account. Complete, so not unpriced -- but a zero in no currency
    /// still cannot enter a currency-scoped cost distribution, where it would
    /// arrive wearing whatever currency its neighbours use. Before ruling 144
    /// this event was unpriced and the percentile read never saw it; now it
    /// leaves that read under its own name instead of failing the report.
    #[test]
    fn an_event_complete_only_through_declined_zeros_carries_no_currency() {
        let conn = conn();
        event(
            &conn,
            "zeros",
            Some("class"),
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(&conn, "zeros", "reasoning", "token", 0);
        unpriced_head(&conn, "zeros", "reasoning", "token", "no_rate");
        let report = spend_report(&conn, &request("UTC"), 1_800_000_000_000).unwrap();
        assert_eq!(report.rows[0].currency, None);
        assert_eq!(report.rows[0].events_priced, 1);
        assert_eq!(report.rows[0].total_nanos, 0);
        assert_eq!(report.rows[0].unpriced_measurements_by_reason["no_rate"], 1);
        let classes = work_class_report(&conn, &WorkClassReportRequest {})
            .unwrap()
            .rows;
        assert_eq!(classes[0].cost_sample_n, 0);
        assert_eq!(
            classes[0].excluded_from_sample.reasons["currency_unknown"],
            1
        );
        assert_eq!(classes[0].p50_nanos, None);
    }

    #[test]
    fn covered_quantities_group_by_unit_and_role_not_unit_alone() {
        let conn = conn();
        conn.execute(
            "INSERT INTO account_subscription
                (account_ref,provider,plan_name,recurring_cost_minor,currency,amount_exponent,
                 period,effective_from_ms,effective_to_ms,operator)
             VALUES ('account','provider','plan',10,'USD',0,'month',1773273600000,NULL,NULL)",
            [],
        )
        .unwrap();
        event(
            &conn,
            "covered",
            Some("class"),
            Some(1_774_137_600_000),
            "subscription_included",
        );
        measurement(&conn, "covered", "input", "token", 100);
        measurement(&conn, "covered", "output", "token", 25);
        covered_heads(&conn, "covered", &[("input", "token"), ("output", "token")]);
        let report = subscription_report(&conn, &SubscriptionReportRequest {}).unwrap();
        assert_eq!(report.rows.len(), 1);
        assert_eq!(report.rows[0].quantities.len(), 2);
        assert_eq!(report.rows[0].quantities[0].role, "input");
        assert_eq!(report.rows[0].quantities[0].quantity_units, 100);
        assert_eq!(report.rows[0].quantities[1].role, "output");
        assert_eq!(report.rows[0].quantities[1].quantity_units, 25);
        let serialized = serde_json::to_value(&report).unwrap();
        assert!(serialized["rows"][0]
            .get("equivalent_metered_nanos")
            .is_none());
    }

    #[test]
    fn subscription_rows_use_non_calendar_plan_boundaries() {
        let conn = conn();
        conn.execute(
            "INSERT INTO account_subscription
                (account_ref,provider,plan_name,recurring_cost_minor,currency,amount_exponent,
                 period,effective_from_ms,effective_to_ms,operator)
             VALUES ('account','provider','plan',10,'USD',0,'month',1773273600000,NULL,NULL)",
            [],
        )
        .unwrap();
        event(
            &conn,
            "covered",
            None,
            Some(1_774_137_600_000),
            "subscription_included",
        );
        measurement(&conn, "covered", "input", "token", 1);
        covered_heads(&conn, "covered", &[("input", "token")]);
        let row = subscription_report(&conn, &SubscriptionReportRequest {})
            .unwrap()
            .rows
            .remove(0);
        assert_eq!(row.period_start_ms, 1_773_273_600_000);
        assert_eq!(row.period_end_ms, 1_775_952_000_000);
    }

    #[test]
    fn nearest_rank_uses_observed_values_and_publishes_sample_n() {
        let conn = conn();
        for (index, cost) in [1, 2, 3, 100].into_iter().enumerate() {
            let id = format!("event-{index}");
            event(
                &conn,
                &id,
                Some("class"),
                Some(100 + index as i64),
                "metered_api",
            );
            measurement(&conn, &id, "input", "token", 1);
            priced_heads(&conn, &id, &[("input", "token", cost)]);
        }
        let rows = work_class_report(&conn, &WorkClassReportRequest {})
            .unwrap()
            .rows;
        assert_eq!(rows[0].events_total, 4);
        assert_eq!(rows[0].cost_sample_n, 4);
        assert_eq!(rows[0].p50_nanos, Some(2));
        assert_eq!(rows[0].p90_nanos, Some(100));
    }

    #[test]
    fn zero_measurement_events_are_counted_and_excluded_from_cost_sample() {
        let conn = conn();
        event(&conn, "empty", Some("class"), Some(100), "metered_api");
        event(&conn, "priced", Some("class"), Some(101), "metered_api");
        measurement(&conn, "priced", "input", "token", 1);
        priced_heads(&conn, "priced", &[("input", "token", 50)]);
        let rows = work_class_report(&conn, &WorkClassReportRequest {})
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2);
        let unknown = &rows[0];
        assert_eq!(unknown.currency, None);
        assert_eq!(unknown.events_total, 1);
        assert_eq!(unknown.cost_sample_n, 0);
        assert_eq!(unknown.excluded_from_sample.events, 1);
        assert_eq!(unknown.excluded_from_sample.of_events, 1);
        assert_eq!(unknown.excluded_from_sample.reasons["no_measurements"], 1);
        let priced = &rows[1];
        assert_eq!(priced.currency.as_deref(), Some("USD"));
        assert_eq!(priced.events_total, 1);
        assert_eq!(priced.cost_sample_n, 1);
        assert_eq!(priced.p50_nanos, Some(50));
        assert_eq!(priced.excluded_from_sample.events, 0);
    }

    fn producer_event(conn: &Connection, id: &str, producer: &str, work_class: Option<&str>) {
        conn.execute(
            "INSERT INTO usage_event (
                event_id,producer,producer_lineage,producer_ref,work_class,occurred_at_ms,
                recorded_at_ms,provider,model,account_ref,project_root,charge_basis
             ) VALUES (?1,?2,'lineage',?1,?3,100,1,'provider','model','account','/raw/project',
                       'metered_api')",
            params![id, producer, work_class],
        )
        .unwrap();
    }

    #[test]
    fn the_same_label_from_two_producers_is_two_rows() {
        let conn = conn();
        producer_event(&conn, "a-1", "opencode", Some("mason"));
        producer_event(&conn, "a-2", "opencode", Some("mason"));
        producer_event(&conn, "b-1", "prefrontal", Some("mason"));
        let report = work_class_report(&conn, &WorkClassReportRequest {}).unwrap();
        let keys = report
            .rows
            .iter()
            .map(|row| {
                (
                    row.producer.as_str(),
                    row.work_class.as_deref(),
                    row.events_total,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                ("opencode", Some("mason"), 2),
                ("prefrontal", Some("mason"), 1),
            ]
        );
    }

    #[test]
    fn a_label_with_a_zero_width_space_round_trips_byte_identical() {
        let conn = conn();
        let spaced = "Alfonso\u{200b} - CTO";
        producer_event(&conn, "spaced", "opencode", Some(spaced));
        producer_event(&conn, "plain", "opencode", Some("Alfonso - CTO"));
        let report = work_class_report(&conn, &WorkClassReportRequest {}).unwrap();
        assert_eq!(report.rows.len(), 2);
        assert!(report
            .rows
            .iter()
            .any(|row| row.work_class.as_deref().map(str::as_bytes) == Some(spaced.as_bytes())));
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(serialized.contains(spaced));
    }

    #[test]
    fn producer_label_coverage_counts_add_up_and_reports_labels_is_observed() {
        let conn = conn();
        producer_event(&conn, "o-1", "opencode", Some("Alfonso"));
        producer_event(&conn, "o-2", "opencode", Some("Sisyphus"));
        producer_event(&conn, "o-3", "opencode", None);
        producer_event(&conn, "o-4", "opencode", Some(""));
        producer_event(&conn, "s-1", "statement", None);
        producer_event(&conn, "s-2", "statement", None);
        producer_event(&conn, "s-3", "statement", Some(""));
        let report = work_class_report(&conn, &WorkClassReportRequest {}).unwrap();
        let coverage = report
            .producers
            .iter()
            .map(|entry| {
                (
                    entry.producer.as_str(),
                    entry.events_total,
                    entry.events_labelled,
                    entry.events_unlabelled,
                    entry.reports_labels,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            coverage,
            vec![("opencode", 4, 2, 2, true), ("statement", 3, 0, 3, false)]
        );
        for entry in &report.producers {
            assert_eq!(
                entry.events_labelled + entry.events_unlabelled,
                entry.events_total
            );
            let row_total: u64 = report
                .rows
                .iter()
                .filter(|row| row.producer == entry.producer)
                .map(|row| row.events_total)
                .sum();
            assert_eq!(row_total, entry.events_total, "{}", entry.producer);
        }
        // The unlabelled rows stay separate per producer, and an absent label
        // stays distinct from an empty one.
        let unlabelled = report
            .rows
            .iter()
            .filter(|row| row.work_class.as_deref().unwrap_or_default().is_empty())
            .map(|row| {
                (
                    row.producer.as_str(),
                    row.work_class.as_deref(),
                    row.events_total,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            unlabelled,
            vec![
                ("opencode", None, 1),
                ("opencode", Some(""), 1),
                ("statement", None, 2),
                ("statement", Some(""), 1),
            ]
        );
    }

    #[test]
    fn ingest_freshness_is_unavailable_never_zero_lag() {
        let conn = conn();
        let report = ledger_coverage(&conn, &LedgerCoverageRequest {}, 100).unwrap();
        let value = serde_json::to_value(report.ingest_freshness).unwrap();
        assert_eq!(value["status"], "unavailable");
        assert_eq!(value["reason"], INGEST_FRESHNESS_REASON);
        assert!(value.get("lag").is_none());
    }

    #[test]
    fn capacity_identity_and_provider_readings_remain_structurally_separate() {
        let conn = conn();
        conn.execute(
            "INSERT INTO capacity_window
                (provider,account_ref,pool_key,slot_kind,used_percent,observed_at_ms)
             VALUES ('provider','custodian-account','primary','primary',10.0,1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO capacity_provider_reading
                (provider,pool_key,slot_kind,used_percent,observed_at_ms)
             VALUES ('provider','primary','primary',20.0,1)",
            [],
        )
        .unwrap();
        let capacity = ledger_coverage(&conn, &LedgerCoverageRequest {}, 100)
            .unwrap()
            .capacity;
        assert_eq!(capacity.labeled_windows[0].account_ref, "custodian-account");
        let provider = serde_json::to_value(&capacity.provider_readings[0]).unwrap();
        assert!(provider.get("account_ref").is_none());
        assert_eq!(provider["used_percent"], 20.0);
    }

    /// The account-attribution join, owed to the report surface by ruling 143
    /// after its mapper-level test was removed with the v1 ledger. A usage
    /// event's `(provider, account_ref)` joins a capacity window on
    /// `(api_provider, account_ref)` -- the models.dev slug, NOT the window's
    /// own `provider`, which is insula's usage-source name ("claude" for
    /// anthropic). On a real store, events join on the slug axis and none on
    /// the insula name. A join written on the wrong axis returns a plausible
    /// empty result, so the negative half is the load-bearing assertion.
    /// Provider-current readings carry no account identity
    /// and are not joinable at the type level.
    #[test]
    fn account_attribution_joins_usage_events_to_capacity_windows_on_the_slug_axis() {
        let conn = conn();
        conn.execute(
            "INSERT INTO usage_event (
                event_id,producer,producer_ref,recorded_at_ms,occurred_at_ms,
                provider,model,account_ref,charge_basis
             ) VALUES ('ev-1','broca','ev-1',1,1000,'anthropic','claude-opus',
                       '00000000-0000-4000-8000-00000000000c','subscription_included')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO capacity_window
                (provider,api_provider,account_ref,pool_key,slot_kind,used_percent,observed_at_ms)
             VALUES ('claude','anthropic','00000000-0000-4000-8000-00000000000c','primary','primary',10.0,1),
                    ('claude','anthropic','00000000-0000-4000-8000-00000000000d','primary','primary',5.0,1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO capacity_provider_reading
                (provider,api_provider,pool_key,slot_kind,used_percent,observed_at_ms)
             VALUES ('antigravity','google','primary','primary',20.0,1)",
            [],
        )
        .unwrap();

        let coverage = ledger_coverage(&conn, &LedgerCoverageRequest {}, 100).unwrap();
        let report = spend_report(
            &conn,
            &ReportRequest {
                grouping: vec!["provider".into(), "account".into()],
                grain: "day".into(),
                timezone: "UTC".into(),
            },
            2_000,
        )
        .unwrap();
        assert_eq!(report.rows.len(), 1);
        let row = &report.rows[0];
        let event_provider = row.grouping["provider"].as_deref().unwrap();
        let event_account = row.grouping["account"].as_deref().unwrap();

        let on_slug: Vec<_> = coverage
            .capacity
            .labeled_windows
            .iter()
            .filter(|window| {
                window.api_provider.as_deref() == Some(event_provider)
                    && window.account_ref == event_account
            })
            .collect();
        assert_eq!(on_slug.len(), 1, "exactly the custodian's window joins");
        assert_eq!(on_slug[0].used_percent, 10.0);

        let on_insula_name = coverage
            .capacity
            .labeled_windows
            .iter()
            .filter(|window| {
                window.provider == event_provider && window.account_ref == event_account
            })
            .count();
        assert_eq!(
            on_insula_name, 0,
            "insula's source name is not the join axis"
        );

        let readings = serde_json::to_value(&coverage.capacity.provider_readings).unwrap();
        assert_eq!(readings.as_array().unwrap().len(), 1);
        assert!(
            readings[0].get("account_ref").is_none(),
            "unlabeled readings cannot join"
        );
    }

    #[test]
    fn durable_rows_derive_adjacent_hours_months_and_all_measurement_dimensions() {
        let conn = conn();
        let instants = [
            Utc.with_ymd_and_hms(2026, 1, 31, 10, 30, 0)
                .single()
                .unwrap()
                .timestamp_millis(),
            Utc.with_ymd_and_hms(2026, 1, 31, 11, 30, 0)
                .single()
                .unwrap()
                .timestamp_millis(),
            Utc.with_ymd_and_hms(2026, 2, 1, 0, 30, 0)
                .single()
                .unwrap()
                .timestamp_millis(),
        ];
        for (ordinal, instant) in instants.into_iter().enumerate() {
            let id = format!("derive-{ordinal}");
            event(&conn, &id, None, Some(instant), "metered_api");
            measurement(&conn, &id, "input", "token", 10);
            measurement(&conn, &id, "output", "character", 20);
            priced_heads(
                &conn,
                &id,
                &[("input", "token", 1), ("output", "character", 2)],
            );
        }
        let mut statement = conn
            .prepare(
                "SELECT event.occurred_at_ms / 3600000,
                        strftime('%Y-%m',event.occurred_at_ms / 1000,'unixepoch'),
                        event.provider,event.model,measurement.unit,measurement.role,
                        SUM(measurement.quantity_units),SUM(head.charge_nanos)
                 FROM usage_event event
                 JOIN measurement ON measurement.event_key=event.event_key
                 JOIN priced_charge head
                   ON head.event_key=measurement.event_key
                  AND head.role=measurement.role AND head.unit=measurement.unit
                  AND NOT EXISTS (
                      SELECT 1 FROM priced_charge child WHERE child.supersedes=head.charge_key
                  )
                 GROUP BY 1,2,3,4,5,6 ORDER BY 1,5,6",
            )
            .unwrap();
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 6);
        assert_eq!(
            rows.iter().map(|row| row.0).collect::<BTreeSet<_>>().len(),
            3
        );
        assert_eq!(
            rows.iter()
                .map(|row| row.1.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["2026-01", "2026-02"])
        );
        assert!(rows.iter().any(|row| {
            row.2 == "provider"
                && row.3 == "model"
                && row.4 == "token"
                && row.5 == "input"
                && row.6 == 10
                && row.7 == 1
        }));
        assert!(rows
            .iter()
            .any(|row| { row.4 == "character" && row.5 == "output" && row.6 == 20 && row.7 == 2 }));
    }

    #[test]
    fn empty_charge_id_refuses_without_changing_stored_state() {
        let conn = conn();
        event(&conn, "kept", None, Some(1), "metered_api");
        let before: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert!(matches!(
            explain_charge(
                &conn,
                &ChargeExplainRequest {
                    event_id: String::new()
                }
            ),
            Err(ReportError::Refused(ReportRefusal::MissingEventId))
        ));
        let after: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn missing_charge_explanation_refuses_without_changing_stored_state() {
        let conn = conn();
        event(&conn, "kept", None, Some(1), "metered_api");
        let before: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert!(matches!(
            explain_charge(
                &conn,
                &ChargeExplainRequest {
                    event_id: "missing".into()
                }
            ),
            Err(ReportError::Refused(ReportRefusal::EventNotFound(_)))
        ));
        let after: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn charge_explanation_includes_rate_edition_and_resolution_in_chain() {
        let conn = conn();
        event(&conn, "event", None, Some(1), "metered_api");
        measurement(&conn, "event", "input", "token", 1);
        priced_heads(&conn, "event", &[("input", "token", 50)]);
        let explanation = explain_charge(
            &conn,
            &ChargeExplainRequest {
                event_id: "event".into(),
            },
        )
        .unwrap();
        let charge = &explanation.measurements[0].charges[0];
        assert_eq!(charge.catalog_source.as_deref(), Some("fusiform"));
        assert_eq!(charge.resolved_at_ms, Some(100));
        assert!(charge.current);
    }

    /// Records a second input-token price for the event and leaves the first
    /// price unchanged. The new charge row has pricing generation 2, and its
    /// `supersedes` column holds the key of the generation-1 row it replaces.
    fn superseding_charge(conn: &Connection, event_id: &str, priced: Result<i64, &str>) {
        let superseded: i64 = conn.query_row(
            "SELECT charge_key FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) AND role='input' AND unit='token' AND pricing_generation=1",
            [event_id], |row| row.get(0)).unwrap();
        conn.execute(
            "UPDATE usage_event SET pricing_generation=2,currency='USD' WHERE event_id=?1",
            [event_id],
        )
        .unwrap();
        match priced {
            Ok(charge_nanos) => conn.execute(
                "INSERT INTO priced_charge (
                    charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,
                    basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,
                    catalog_source,resolved_at_ms,catalog_entry,supersedes,origin,recorded_at_ms
                 ) VALUES (?1,(SELECT event_key FROM usage_event WHERE event_id=?2),'input','token',2,'priced','metered_api',0,'USD',?3,1,9,
                           'unknown','fusiform',200,'{}',?4,'pricing_pass',200)",
                params![rusqlite::types::Null, event_id, charge_nanos, superseded],
            ),
            Err(reason) => conn.execute(
                "INSERT INTO priced_charge (
                    charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,
                    basis_covered,unpriced_reason,catalog_source,resolved_at_ms,supersedes,
                    origin,recorded_at_ms
                 ) VALUES (?1,(SELECT event_key FROM usage_event WHERE event_id=?2),'input','token',2,'unpriced','metered_api',0,?3,'fusiform',
                           200,?4,'pricing_pass',200)",
                params![rusqlite::types::Null, event_id, reason, superseded],
            ),
        }
        .unwrap();
    }

    /// Only the charge nothing supersedes speaks for a measurement. A chain's
    /// original (the row whose own `supersedes` is NULL) is the wrong row: it
    /// is what the measurement USED to cost. One event is declined first and
    /// priced on re-pricing; the other is priced first and declined later.
    #[test]
    fn only_the_charge_nothing_supersedes_counts_toward_verdict_and_total() {
        let conn = conn();
        event(
            &conn,
            "repriced",
            None,
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(&conn, "repriced", "input", "token", 13);
        unpriced_head(&conn, "repriced", "input", "token", "no_rate");
        superseding_charge(&conn, "repriced", Ok(70));
        event(
            &conn,
            "withdrawn",
            None,
            Some(1_700_000_000_000),
            "metered_api",
        );
        measurement(&conn, "withdrawn", "input", "token", 13);
        priced_heads(&conn, "withdrawn", &[("input", "token", 50)]);
        superseding_charge(&conn, "withdrawn", Err("rate_expired"));

        let report = spend_report(&conn, &request("UTC"), 1_000).unwrap();
        let row = &report.rows[0];
        assert_eq!(report.rows.len(), 1);
        assert_eq!(row.events_total, 2);
        assert_eq!(row.events_priced, 1);
        assert_eq!(row.total_nanos, 70);
        assert_eq!(
            row.unpriced_events_by_reason,
            BTreeMap::from([("rate_expired".to_string(), 1)])
        );
        assert_eq!(
            row.unpriced_measurements_by_reason,
            BTreeMap::from([("rate_expired".to_string(), 1)])
        );
        assert_eq!(report.rate_staleness.priced_measurements, 1);
        assert_eq!(report.rate_staleness.oldest_resolved_at_ms, Some(200));

        let coverage = ledger_coverage(&conn, &LedgerCoverageRequest {}, 1_000).unwrap();
        assert_eq!(coverage.events_unpriced, 1);
        assert_eq!(
            coverage.unpriced_events_by_reason,
            BTreeMap::from([("rate_expired".to_string(), 1)])
        );

        let work_classes = work_class_report(&conn, &WorkClassReportRequest {})
            .unwrap()
            .rows;
        assert_eq!(work_classes[0].cost_sample_n, 1);
        assert_eq!(work_classes[0].p50_nanos, Some(70));
    }

    /// Parity and timing harness for the four ledger-wide reports against a
    /// real store. It opens `ASTROCYTE_REPORT_PARITY_DB` read-only, runs each
    /// report with a fixed clock and several argument sets, prints the wall
    /// time of every call, and writes each result as JSON into
    /// `ASTROCYTE_REPORT_PARITY_OUT`. Running it before and after a change to
    /// the report queries and diffing the two output directories shows whether
    /// the change moved any published number. Point it at a COPY of the
    /// production store, never the live file.
    #[test]
    #[ignore = "needs ASTROCYTE_REPORT_PARITY_DB pointing at a copy of a real store"]
    fn report_parity_against_a_store_copy() {
        use rusqlite::OpenFlags;
        use std::time::Instant;

        let db = std::env::var("ASTROCYTE_REPORT_PARITY_DB")
            .expect("ASTROCYTE_REPORT_PARITY_DB must name a store copy");
        let out = std::path::PathBuf::from(
            std::env::var("ASTROCYTE_REPORT_PARITY_OUT")
                .expect("ASTROCYTE_REPORT_PARITY_OUT must name an output directory"),
        );
        std::fs::create_dir_all(&out).unwrap();
        let only = std::env::var("ASTROCYTE_REPORT_PARITY_ONLY").ok();
        let conn = Connection::open_with_flags(
            &db,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        configure_report_reader(&conn).unwrap();
        // A fixed clock keeps the rate-staleness age identical across runs.
        let now_ms = 1_790_000_000_000_i64;
        let wanted = |name: &str| {
            only.as_deref()
                .is_none_or(|only| only.split(',').any(|n| n == name))
        };
        let write = |name: &str, value: serde_json::Value, started: Instant| {
            println!("{name}: {:.3}s", started.elapsed().as_secs_f64());
            std::fs::write(
                out.join(format!("{name}.json")),
                serde_json::to_string_pretty(&value).unwrap(),
            )
            .unwrap();
        };
        let spend_cases: [(&str, &[&str], &str, &str); 4] = [
            ("spend_month_utc", &[], "month", "UTC"),
            (
                "spend_day_madrid_provider_model_basis",
                &["provider", "model", "charge_basis"],
                "day",
                "Europe/Madrid",
            ),
            (
                "spend_week_newyork_account_auth_work_project",
                &["account", "auth_selection", "work_class", "project"],
                "week",
                "America/New_York",
            ),
            (
                "spend_month_kolkata_work_class",
                &["work_class"],
                "month",
                "Asia/Kolkata",
            ),
        ];
        for (name, grouping, grain, timezone) in spend_cases {
            if !wanted(name) {
                continue;
            }
            let request = ReportRequest {
                grouping: grouping.iter().map(|value| (*value).to_string()).collect(),
                grain: grain.into(),
                timezone: timezone.into(),
            };
            let started = Instant::now();
            let report = spend_report(&conn, &request, now_ms).unwrap();
            write(name, serde_json::to_value(report).unwrap(), started);
        }
        if wanted("subscription") {
            let started = Instant::now();
            let report = subscription_report(&conn, &SubscriptionReportRequest {}).unwrap();
            write(
                "subscription",
                serde_json::to_value(report).unwrap(),
                started,
            );
        }
        if wanted("work_class") {
            let started = Instant::now();
            let report = work_class_report(&conn, &WorkClassReportRequest {}).unwrap();
            write("work_class", serde_json::to_value(report).unwrap(), started);
        }
        if wanted("coverage") {
            let started = Instant::now();
            let report = ledger_coverage(&conn, &LedgerCoverageRequest {}, now_ms).unwrap();
            write("coverage", serde_json::to_value(report).unwrap(), started);
        }
    }
}
