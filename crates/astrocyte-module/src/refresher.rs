//! Producer-runtime draining.
//!
//! A producer's read is deliberately completed before `drain_once` takes the
//! store writer. The successful observation and its cursor state are committed
//! in one transaction, so a failed fact write cannot make the producer look
//! current.

#[path = "attribution_source.rs"]
mod attribution_source;
#[path = "broca_source.rs"]
pub(crate) mod broca_source;
#[path = "claude_code_source.rs"]
mod claude_code_source;
#[path = "claude_thalamus_source.rs"]
mod claude_thalamus_source;
#[path = "codex_source.rs"]
mod codex_source;
#[path = "colliculus_source.rs"]
pub(crate) mod colliculus_source;
#[path = "declared_roots.rs"]
mod declared_roots;
#[cfg(test)]
#[path = "refresher_event_wakes_tests.rs"]
mod event_wakes_tests;
#[cfg(all(test, target_os = "linux"))]
#[path = "fast_lane_measure.rs"]
mod fast_lane_measure;
#[cfg(test)]
#[path = "fast_lane_tests.rs"]
mod fast_lane_tests;
#[path = "gemini_source.rs"]
mod gemini_source;
#[path = "goose_source.rs"]
mod goose_source;
#[path = "jsonl_schedule.rs"]
pub(crate) mod jsonl_schedule;
#[path = "kimi_source.rs"]
mod kimi_source;
#[path = "omp_source.rs"]
mod omp_source;
#[path = "opencode_source.rs"]
pub(crate) mod opencode_source;
#[path = "opencode_thalamus_source.rs"]
pub(crate) mod opencode_thalamus_source;
#[path = "opencode_v2_source.rs"]
pub(crate) mod opencode_v2_source;
#[path = "pi_source.rs"]
mod pi_source;
#[path = "pi_thalamus_source.rs"]
mod pi_thalamus_source;
#[cfg(test)]
#[path = "refresher_valuation_tests.rs"]
mod refresher_valuation_tests;
#[path = "thalamus_payer_join.rs"]
pub(crate) mod thalamus_payer_join;
#[path = "thalamus_payer_source.rs"]
pub(crate) mod thalamus_payer_source;

#[path = "refresher/catalog.rs"]
mod catalog;
#[path = "refresher/harness_drain.rs"]
pub(crate) mod harness_drain;
#[path = "refresher/multiplier.rs"]
mod multiplier_stage;
#[path = "refresher/pricing.rs"]
mod pricing;
#[path = "refresher/queue.rs"]
mod queue;
#[path = "refresher/source_drain.rs"]
mod source_drain;
#[path = "refresher/summaries.rs"]
mod summaries;

pub use catalog::*;
pub(crate) use multiplier_stage::*;
pub(crate) use pricing::*;
#[cfg(test)]
pub(crate) use queue::*;
#[cfg(not(test))]
use queue::*;
use source_drain::*;
use summaries::*;

#[cfg(test)]
pub use pricing::price_unpriced_events;
pub use source_drain::drain_once;

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use astrocyte_core::ingest::EventRecordRequest;
use astrocyte_core::{
    capacity::{apply_snapshot_in_tx, record_failed_attempt, CapacitySnapshot},
    catalog_client::{CatalogClient, CatalogConnection},
    multiplier::{self, MultiplierConfig, PlanPriceSource},
};
use rusqlite::{params, Transaction};

use crate::{
    config,
    state::{self, now_ms as current_time_ms, SharedState},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCursor {
    pub lineage: String,
    pub ordinal: i64,
}

#[derive(Debug, Clone)]
pub struct ExportPage {
    /// The producer position this page's facts run up to and the drain commits.
    pub cursor: SourceCursor,
    /// The producer's newest position when it served this page.
    pub high_water: SourceCursor,
    pub events: Vec<EventRecordRequest>,
    pub(crate) broca_facts: Vec<(i64, broca_source::BrocaFact)>,
    pub set_aside: Vec<SetAsideRecord>,
    /// This is a producer-provided exact count, not a sequence-number
    /// subtraction. Unknown lag remains explicit when the producer could not
    /// enumerate all pending records.
    pub pending_known: i64,
    pub pending_unknown: bool,
}

/// Evidence locators are independent of event identity: unreadable records never
/// need an invented producer reference in order to be retained.
#[derive(Debug, Clone)]
pub struct SetAsideRecord {
    pub locator: String,
    pub reason: String,
}

pub trait SourceClient: Send + Sync {
    fn pull(&self, since: Option<SourceCursor>, limit: u32) -> Result<ExportPage, PullError>;

    fn pull_fast(&self, since: Option<SourceCursor>, limit: u32) -> Result<ExportPage, PullError> {
        self.pull(since, limit)
    }

    /// The resolved store path, if known, used when logging whether the store is
    /// present or absent.
    fn resolved_path(&self) -> Option<PathBuf> {
        None
    }

    fn refuse_regression(
        &self,
        stored: Option<&SourceCursor>,
        page: &ExportPage,
    ) -> Result<(), PullError> {
        refuse_regression(stored, page)
    }
}

pub trait CapacitySource: Send + Sync {
    fn poll(&self) -> Result<CapacitySnapshot, PullError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullError {
    Unavailable(String),
    /// The producer's store does not exist: the producer is not installed.
    /// There is nothing to read and nothing to go stale, so the drain removes
    /// the source's runtime row instead of recording a failure.
    SourceAbsent,
    /// Shutdown interrupted the read. Nothing was committed and nothing about
    /// the source's health was learned.
    Stopped,
    InvalidResponse(String),
    /// The producer answered under the identity we hold but from a position
    /// behind our cursor: a restore, a clock rollback, or both. Advancing
    /// would either skip past facts (strand) or re-read them (double count);
    /// refusing leaves the cursor where it was and lets freshness age into a
    /// visible degradation with this reason on the log.
    LineageRegressed {
        stored: SourceCursor,
        offered: SourceCursor,
    },
    Store(String),
}

/// A producer whose connection cannot be opened. Test fixture only: production
/// producers come from `refresher_producers`, and a loop fed this stub looks
/// alive while draining nothing.
#[cfg(test)]
pub struct UnavailableSource;

#[cfg(test)]
impl SourceClient for UnavailableSource {
    fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
        Err(PullError::Unavailable(
            "producer connection is unavailable".into(),
        ))
    }
}

/// Why a file-backed drain (OpenCode, Pi) returned without an outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourceDrainError {
    /// Shutdown was requested and the drain abandoned its work uncommitted.
    /// This is not a failure: it must not degrade the source's health or
    /// write anything into `source_runtime`.
    Stopped,
    /// A peer is warming or changing routes. Retain health and retry next tick.
    Deferred,
    /// The drain failed.
    Failed(String),
}

impl From<String> for SourceDrainError {
    fn from(reason: String) -> Self {
        Self::Failed(reason)
    }
}

impl std::fmt::Display for SourceDrainError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped => formatter.write_str("drain stopped"),
            Self::Deferred => formatter.write_str("drain deferred"),
            Self::Failed(reason) => formatter.write_str(reason),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrainOutcome {
    /// Events the page carried, whatever the ledger did with each.
    pub events_recorded: usize,
    /// Events whose identity was already stored with a different payload.
    pub quarantined_redeliveries: usize,
    pub consumed_ordinal: Option<i64>,
    pub high_water_ordinal: Option<i64>,
}

pub fn hydrate_startup(state: &Arc<SharedState>) -> Result<(), String> {
    reload_config(state, state::user_config_path())?;
    // Runs on a blocking thread before serving starts; a failure only leaves
    // `usage.cache.steps` reading the store.
    state.rebuild_recent_steps();
    Ok(())
}

/// Exercise retention/refusal for invalid startup bytes, not successful publication.
#[cfg(any(test, feature = "test-support"))]
pub fn reject_startup_bytes_for_test(
    state: &Arc<SharedState>,
    contents: &[u8],
) -> Result<(), String> {
    match config::decode_bytes(contents) {
        Ok(_) => Err("test fixture unexpectedly decoded".into()),
        Err(error) => retain_committed_generation_or_refuse_seed(state, error.to_string()),
    }
}

fn retain_committed_generation_or_refuse_seed(
    state: &Arc<SharedState>,
    failure: String,
) -> Result<(), String> {
    state.mark_config_degraded(failure.clone());
    if state.has_committed_desired_generation() {
        log_config_retained(&failure);
        Ok(())
    } else {
        Err(format!(
            "startup desired configuration unavailable with no committed generation: {failure}"
        ))
    }
}

/// `resolved_path` is injected so a test can drive the production arms with a
/// temp file and without touching process environment; production passes
/// `state::user_config_path()`.
fn reload_config(
    state: &Arc<SharedState>,
    resolved_path: Result<std::path::PathBuf, String>,
) -> Result<(), String> {
    // An unresolvable path is not a missing file: Missing publishes
    // intentional-empty as a new generation, which is only correct when the
    // operator removed the file. Every failure arm below goes through
    // retain_committed_generation_or_refuse_seed, never straight to Err. An
    // early `?` here used to exit the daemon on a malformed or unreadable file
    // even when a committed generation existed to serve -- the retain arm was
    // written, tested through a helper, and reached by nothing on the
    // production path. The lint that reported it dead was the only witness.
    let path = match resolved_path {
        Ok(path) => path,
        Err(error) => return retain_committed_generation_or_refuse_seed(state, error),
    };
    let desired = match config::classify_read_result(std::fs::read(path)) {
        config::ConfigReadOutcome::Contents(contents) => match config::decode_bytes(&contents) {
            Ok(desired) => desired,
            Err(error) => {
                return retain_committed_generation_or_refuse_seed(state, error.to_string())
            }
        },
        config::ConfigReadOutcome::Missing => config::intentional_empty(),
        config::ConfigReadOutcome::Unreadable(error) => {
            return retain_committed_generation_or_refuse_seed(state, error.to_string())
        }
    };
    let now_ms = current_time_ms();
    let generation = state.ordered_write(|tx| publish_desired_transaction(tx, &desired, now_ms))?;
    state.publish_desired(
        desired.config,
        generation.unwrap_or_else(|| state.desired_generation()),
    );
    Ok(())
}

fn publish_desired_transaction(
    tx: &Transaction<'_>,
    desired: &config::DesiredContent,
    now_ms: i64,
) -> Result<(Option<u64>, bool), String> {
    let current: (i64, String) = tx
        .query_row(
            "SELECT generation,content_hash FROM desired_config WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| error.to_string())?;
    if current.1 == desired.content_hash {
        return Ok((None, false));
    }
    let generation = current
        .0
        .checked_add(1)
        .ok_or_else(|| "desired generation overflow".to_string())?;
    let multiplier =
        config::multiplier_column(&desired.config).map_err(|error| error.to_string())?;
    let ingest =
        config::harness_ingest_column(&desired.config).map_err(|error| error.to_string())?;
    // Keep the legacy path columns in the schema but never publish paths.
    tx.execute("UPDATE desired_config SET generation=?1,canonical_bytes=?2,content_hash=?3,poll_interval_secs=?4,qta_connection_file=NULL,broca_connection_file=NULL,fusiform_connection_file=NULL,updated_at_ms=?5,subscription_multiplier=?6,harness_ingest=?7 WHERE id=1", params![generation, desired.canonical_bytes, desired.content_hash, i64::try_from(desired.config.poll_interval_secs).map_err(|_| "poll interval exceeds SQLite integer".to_string())?, now_ms, multiplier, ingest]).map_err(|error| error.to_string())?;
    tx.execute(
        "UPDATE source_runtime SET desired_generation=?1",
        [generation],
    )
    .map_err(|error| error.to_string())?;
    Ok((
        Some(u64::try_from(generation).map_err(|_| "negative generation".to_string())?),
        true,
    ))
}

pub struct Producer {
    pub source_key: String,
    pub client: Arc<dyn SourceClient>,
}

/// The producers drained through `drain_once`: BROCA's store reader, which
/// needs no configuration. `stop` interrupts a read in progress on shutdown.
fn refresher_producers(stop: &Arc<AtomicBool>) -> Vec<Producer> {
    vec![Producer {
        source_key: broca_source::BROCA_SOURCE_KEY.into(),
        client: Arc::new(refresher_broca_source(stop)),
    }]
}

fn refresher_broca_source(stop: &Arc<AtomicBool>) -> broca_source::BrocaSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's live BROCA
        // store. Store resolution itself is exercised through `from_data_home`.
        let data_home = std::env::temp_dir().join(format!(
            "astrocyte-test-data-home-without-broca-{}",
            std::process::id()
        ));
        broca_source::BrocaSource::from_data_home(data_home, Arc::clone(stop))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        broca_source::BrocaSource::from_environment(Arc::clone(stop))
    }
}

pub(crate) fn configured_capacity(
    state: &SharedState,
    config: &config::AstrocyteConfig,
    access: Option<crate::peer_exchange::PeerAccess>,
) -> Result<Option<Arc<dyn CapacitySource>>, String> {
    let source = access
        .map(crate::qta_client::QtaClient::new)
        .transpose()?
        .map(|client| Arc::new(client) as Arc<dyn CapacitySource>);
    state.configure_capacity_runtime(
        source.is_some(),
        config.poll_interval_ms(),
        current_time_ms(),
    )?;
    Ok(source)
}

#[derive(Default)]
struct FileSources {
    opencode: Option<Arc<opencode_source::OpenCodeSource>>,
    pi: Option<Arc<pi_source::PiSource>>,
    claude_code: Option<Arc<claude_code_source::ClaudeCodeSource>>,
    codex: Option<Arc<codex_source::CodexSource>>,
    /// Additional harness readers, including the declared THALAMUS sessions.
    harnesses: Vec<Arc<dyn harness_drain::HarnessSource>>,
}

fn refresher_opencode_source() -> opencode_source::OpenCodeSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-opencode-{}",
            std::process::id()
        ));
        opencode_source::OpenCodeSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        opencode_source::OpenCodeSource::from_environment()
    }
}

fn refresher_pi_source() -> pi_source::PiSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-pi-{}",
            std::process::id()
        ));
        pi_source::PiSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        pi_source::PiSource::from_environment()
    }
}

fn refresher_claude_code_source() -> claude_code_source::ClaudeCodeSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-claude-code-{}",
            std::process::id()
        ));
        claude_code_source::ClaudeCodeSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        claude_code_source::ClaudeCodeSource::from_environment()
    }
}

fn refresher_codex_source() -> codex_source::CodexSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-codex-{}",
            std::process::id()
        ));
        codex_source::CodexSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        codex_source::CodexSource::from_environment()
    }
}

fn refresher_gemini_source() -> gemini_source::GeminiSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-gemini-{}",
            std::process::id()
        ));
        gemini_source::GeminiSource::from_home(Some(home.into_os_string()), None)
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        gemini_source::GeminiSource::from_environment()
    }
}

fn refresher_goose_source() -> goose_source::GooseSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-goose-{}",
            std::process::id()
        ));
        goose_source::GooseSource::from_home(Some(home.into_os_string()), None)
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        goose_source::GooseSource::from_environment()
    }
}

fn refresher_omp_source() -> omp_source::OmpSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-omp-{}",
            std::process::id()
        ));
        omp_source::OmpSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        omp_source::OmpSource::from_environment()
    }
}

fn refresher_kimi_source() -> kimi_source::KimiSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-kimi-{}",
            std::process::id()
        ));
        kimi_source::KimiSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        kimi_source::KimiSource::from_environment()
    }
}

fn refresher_harness_sources() -> Vec<Arc<dyn harness_drain::HarnessSource>> {
    vec![
        Arc::new(refresher_thalamus_payer_source()),
        Arc::new(opencode_v2_source::registered()),
        Arc::new(refresher_gemini_source()),
        Arc::new(refresher_goose_source()),
        Arc::new(refresher_omp_source()),
        Arc::new(refresher_kimi_source()),
        Arc::new(refresher_claude_thalamus_source()),
        Arc::new(refresher_pi_thalamus_source()),
        Arc::new(refresher_opencode_thalamus_source()),
    ]
}

fn refresher_thalamus_payer_source() -> thalamus_payer_source::ThalamusPayerSource {
    #[cfg(any(test, feature = "test-support"))]
    let home = std::env::temp_dir().join(format!(
        "astrocyte-test-data-without-payer-{}",
        std::process::id()
    ));
    #[cfg(not(any(test, feature = "test-support")))]
    let home = PathBuf::from(cortexkit_store_types::resolve_data_home());
    thalamus_payer_source::ThalamusPayerSource::from_data_home(home)
}

fn refresher_claude_thalamus_source() -> claude_thalamus_source::ClaudeThalamusSource {
    #[cfg(any(test, feature = "test-support"))]
    let home = std::env::temp_dir().join(format!(
        "astrocyte-test-data-without-claude-sessions-{}",
        std::process::id()
    ));
    #[cfg(not(any(test, feature = "test-support")))]
    let home = PathBuf::from(cortexkit_store_types::resolve_data_home());
    claude_thalamus_source::ClaudeThalamusSource::from_data_home(home)
}

fn refresher_pi_thalamus_source() -> pi_thalamus_source::PiThalamusSource {
    #[cfg(any(test, feature = "test-support"))]
    let home = std::env::temp_dir().join(format!(
        "astrocyte-test-data-without-pi-sessions-{}",
        std::process::id()
    ));
    #[cfg(not(any(test, feature = "test-support")))]
    let home = PathBuf::from(cortexkit_store_types::resolve_data_home());
    pi_thalamus_source::PiThalamusSource::from_data_home(home)
}

fn refresher_opencode_thalamus_source() -> opencode_thalamus_source::OpenCodeThalamusSource {
    #[cfg(any(test, feature = "test-support"))]
    let home = std::env::temp_dir().join(format!(
        "astrocyte-test-data-without-opencode-sessions-{}",
        std::process::id()
    ));
    #[cfg(not(any(test, feature = "test-support")))]
    let home = PathBuf::from(cortexkit_store_types::resolve_data_home());
    opencode_thalamus_source::OpenCodeThalamusSource::from_data_home(home)
}

/// Releases the pricing-pass hold on drop, so a failed assertion cannot keep
/// the pass held for the rest of the test process.
#[cfg(any(test, feature = "test-support"))]
pub struct PricingPassHold(SharedState);

#[cfg(any(test, feature = "test-support"))]
impl Drop for PricingPassHold {
    fn drop(&mut self) {
        self.0.set_pricing_pass_hold(false);
    }
}

/// Skip the loop's pricing pass on `state` until the returned guard drops.
/// A test that must be the one to price an event -- an operator-frame test
/// whose fake catalog peer serves a fixed number of calls -- holds this for
/// the frame's duration. Without it the loop's first pass can price the same
/// event first, and the frame's own catalog resolution then runs against a
/// peer that has already served its last answer. The hold lives on the state
/// rather than in a process-wide flag because tests run as threads in one
/// process: a global hold would also silence the pricing pass of every
/// `run_iteration_*` helper a parallel test drives on its own state, failing
/// that test's priced-row assertions. Discovery, drains, and capacity polls
/// are unaffected, and direct `price_unpriced_events` calls still price.
#[cfg(any(test, feature = "test-support"))]
pub fn hold_pricing_pass_for_test(state: &SharedState) -> PricingPassHold {
    state.set_pricing_pass_hold(true);
    PricingPassHold(state.clone())
}

/// Own blocking transport runtimes on the dedicated refresher thread so they
/// are never constructed or dropped on a tokio worker.
pub fn start_refresher(
    state: Arc<SharedState>,
    stop: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<Result<(), String>>, String> {
    spawn_refresher(
        state,
        stop,
        refresher_opencode_source(),
        refresher_pi_source(),
        refresher_claude_code_source(),
        refresher_codex_source(),
        refresher_harness_sources(),
    )
}

/// Adds the media feed, which waits for the shared attested launch identity.
pub fn start_refresher_with_media(
    state: Arc<SharedState>,
    stop: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<Result<(), String>>, String> {
    let mut sources = refresher_harness_sources();
    sources.push(Arc::new(colliculus_source::ColliculusSource::new()));
    spawn_refresher(
        state,
        stop,
        refresher_opencode_source(),
        refresher_pi_source(),
        refresher_claude_code_source(),
        refresher_codex_source(),
        sources,
    )
}

/// The production refresher reading OpenCode from `opencode_database`, so a
/// test can put a slow OpenCode stage inside the real loop and shut it down.
#[cfg(any(test, feature = "test-support"))]
pub fn start_refresher_with_opencode(
    state: Arc<SharedState>,
    stop: Arc<AtomicBool>,
    opencode_database: std::path::PathBuf,
) -> Result<std::thread::JoinHandle<Result<(), String>>, String> {
    spawn_refresher(
        state,
        stop,
        opencode_source::OpenCodeSource::at(opencode_database),
        refresher_pi_source(),
        refresher_claude_code_source(),
        refresher_codex_source(),
        refresher_harness_sources(),
    )
}

fn spawn_refresher(
    state: Arc<SharedState>,
    stop: Arc<AtomicBool>,
    opencode_source: opencode_source::OpenCodeSource,
    pi_source: pi_source::PiSource,
    claude_code_source: claude_code_source::ClaudeCodeSource,
    codex_source: codex_source::CodexSource,
    harness_sources: Vec<Arc<dyn harness_drain::HarnessSource>>,
) -> Result<std::thread::JoinHandle<Result<(), String>>, String> {
    let (ready, initialized) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name("astrocyte-refresher".into())
        .spawn(move || {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let startup = (|| {
                    let config = state.desired_config();
                    let producers = refresher_producers(&stop);
                    let opencode_source = Arc::new(opencode_source);
                    let pi_source = Arc::new(pi_source);
                    let claude_code_source = Arc::new(claude_code_source);
                    let codex_source = Arc::new(codex_source);
                    let capacity = configured_capacity(&state, &config, state.peer_access())?;
                    let (pending_catalog, plan_prices) =
                        configure_catalog(&state, state.catalog_access(), &stop)?;
                    Ok((
                        producers,
                        opencode_source,
                        pi_source,
                        claude_code_source,
                        codex_source,
                        capacity,
                        pending_catalog,
                        plan_prices,
                        config.poll_interval_secs,
                    ))
                })();
                let ready_result = startup
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error: &String| error.clone());
                let _ = ready.send(ready_result);
                let (
                    producers,
                    opencode_source,
                    pi_source,
                    claude_code_source,
                    codex_source,
                    capacity,
                    pending_catalog,
                    plan_prices,
                    poll_interval_secs,
                ) = startup?;
                // The starter has already returned readiness to the serving
                // loop. Run the first quota poll and catalog probe as soon as
                // HELLO_ACK releases the routes, not one full interval later.
                while state.quota_bus().has_staged_access() && !state.quota_bus().hello_acked() {
                    if stop.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    std::thread::sleep(Duration::from_millis(25));
                }
                // Use separate clients/runtimes for quota events and fallback snapshots.
                let bus_reader = crate::quota_events::Reader::start(
                    Arc::clone(&state),
                    state.peer_access(),
                    Arc::clone(&stop),
                );
                run_loop_full(
                    Arc::clone(&state),
                    producers,
                    FileSources {
                        opencode: Some(opencode_source),
                        pi: Some(pi_source),
                        claude_code: Some(claude_code_source),
                        codex: Some(codex_source),
                        harnesses: harness_sources,
                    },
                    capacity,
                    pending_catalog,
                    MultiplierStage::new(plan_prices),
                    poll_interval_secs,
                    Arc::clone(&stop),
                );
                if let Some(reader) = bus_reader {
                    reader.join();
                }
                Ok(())
            }));
            // The client owns a current-thread runtime. Removing the state's last
            // reference even after a panic keeps its drop on this OS thread.
            let cleanup = state.install_catalog(None);
            match outcome {
                Ok(result) => result.and(cleanup),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        })
        .map_err(|error| error.to_string())?;
    match initialized.recv() {
        Ok(Ok(())) => Ok(thread),
        Ok(Err(error)) => {
            let _ = thread.join();
            Err(error)
        }
        Err(error) => {
            let _ = thread.join();
            Err(format!("refresher initialization channel: {error}"))
        }
    }
}

/// Test fixture over the production loop: no capacity source, no catalog.
/// Production enters through `start_refresher`, which supplies all three.
#[cfg(test)]
pub fn run_loop(
    state: Arc<SharedState>,
    producers: Vec<Producer>,
    poll_secs: u64,
    stop: Arc<AtomicBool>,
) {
    run_loop_full(
        state,
        producers,
        FileSources::default(),
        None,
        None,
        MultiplierStage::new(None),
        poll_secs,
        stop,
    );
}

#[allow(clippy::too_many_arguments)]
fn run_loop_full(
    state: Arc<SharedState>,
    producers: Vec<Producer>,
    file_sources: FileSources,
    capacity: Option<Arc<dyn CapacitySource>>,
    mut pending_catalog: Option<PendingCatalog>,
    mut multiplier: MultiplierStage,
    poll_secs: u64,
    stop: Arc<AtomicBool>,
) {
    let interval = Duration::from_secs(poll_secs.max(1));
    let mut reconciler = QueueReconciler::new(QUEUE_RECONCILE_CHUNK, QUEUE_RECONCILE_TICK_BUDGET);
    let mut pricing = PricingWalk::new(PricingBudget::PER_TICK);
    let mut summaries = astrocyte_core::report::SummaryMaintenance::default();
    let mut valuations = astrocyte_core::report::valuation::ValuationMaintenance::default();
    let mut vault = crate::vault_source::Stage::new(Arc::clone(&stop));
    while !stop.load(Ordering::Relaxed) {
        vault.tick(&state, multiplier.plan_prices.as_deref(), current_time_ms());
        run_tick(
            &state,
            &producers,
            &file_sources,
            capacity.as_deref(),
            &mut pending_catalog,
            &mut multiplier,
            &mut pricing,
            &mut reconciler,
            &stop,
        );
        let mut summaries_behind = summary_tick(&state, &mut summaries, &stop);
        valuation_tick(&state, &mut valuations, &stop, Duration::from_millis(1_500));
        if !stop.load(Ordering::Relaxed) {
            multiplier.tick_days(&state, pending_catalog.is_some(), current_time_ms(), &stop);
        }
        let started = std::time::Instant::now();
        let mut next_fast = FAST_LANE_INTERVAL;
        #[cfg(test)]
        state.note_interval_wait_for_test();
        while !stop.load(Ordering::Relaxed) && started.elapsed() < interval {
            crate::quota_events::drain(&state, &stop);
            catalog_wake_tick(&state, &mut pending_catalog, &mut multiplier, &stop, false);
            attribution_source::drain_all(&state, &stop, false);
            for source in &file_sources.harnesses {
                if source.source_key() == colliculus_source::SOURCE_KEY
                    && state
                        .desired_config()
                        .harness_ingest
                        .ingests(colliculus_source::SOURCE_KEY)
                {
                    drain_media_if_due(&state, source.as_ref(), &stop, false);
                }
            }
            fast_lane_if_due(
                &state,
                &producers,
                &file_sources,
                &stop,
                started.elapsed(),
                interval,
                &mut next_fast,
            );
            // A summary format change leaves every hour stale, and reports
            // read the whole ledger until each hour is rebuilt. At one budget
            // per full tick that takes most of an hour, so while hours are
            // waiting the rebuild continues between ticks. With no backlog it
            // costs nothing.
            if summaries_behind && !stop.load(Ordering::Relaxed) {
                summaries_behind = summary_tick(&state, &mut summaries, &stop);
            }
            if multiplier.rates_behind() && !stop.load(Ordering::Relaxed) {
                multiplier.fill_rates(&state);
            }
            valuation_tick(&state, &mut valuations, &stop, Duration::from_millis(1_500));
            std::thread::sleep(
                Duration::from_millis(250).min(interval.saturating_sub(started.elapsed())),
            );
        }
    }
    vault.join();
}

const FAST_LANE_INTERVAL: Duration = Duration::from_secs(2);

/// The elapsed-time argument lets tests advance the same scheduler without wall-clock sleeps.
fn fast_lane_if_due(
    state: &SharedState,
    producers: &[Producer],
    sources: &FileSources,
    stop: &AtomicBool,
    elapsed: Duration,
    full_interval: Duration,
    next_fast: &mut Duration,
) {
    #[cfg(all(test, target_os = "linux"))]
    if !fast_lane_measure::enabled() {
        return;
    }
    if elapsed < *next_fast || elapsed >= full_interval || stop.load(Ordering::Relaxed) {
        return;
    }
    run_fast_lane(state, producers, sources, current_time_ms(), stop);
    // Do not replay missed cycles after a slow read.
    *next_fast = elapsed.saturating_add(FAST_LANE_INTERVAL);
}

fn run_fast_lane(
    state: &SharedState,
    producers: &[Producer],
    sources: &FileSources,
    at: i64,
    stop: &AtomicBool,
) {
    #[cfg(all(test, target_os = "linux"))]
    let _measurement = fast_lane_measure::cycle(true);
    let ingest = state.desired_config().harness_ingest;
    for source in &sources.harnesses {
        if source.source_key() == thalamus_payer_source::SOURCE_KEY
            && ingest.ingests(thalamus_payer_source::SOURCE_KEY)
        {
            if let Err(SourceDrainError::Failed(error)) = source.drain_fast(state, at, stop) {
                tracing::debug!(%error, "fast payer read failed");
            }
        }
    }
    let older: [Option<&dyn harness_drain::HarnessSource>; 4] = [
        sources
            .opencode
            .as_deref()
            .map(|source| source as &dyn harness_drain::HarnessSource),
        sources
            .pi
            .as_deref()
            .map(|source| source as &dyn harness_drain::HarnessSource),
        sources
            .claude_code
            .as_deref()
            .map(|source| source as &dyn harness_drain::HarnessSource),
        sources
            .codex
            .as_deref()
            .map(|source| source as &dyn harness_drain::HarnessSource),
    ];
    for source in older
        .into_iter()
        .flatten()
        .chain(sources.harnesses.iter().map(AsRef::as_ref))
    {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let ingest_key = match source.source_key() {
            opencode_v2_source::SOURCE_KEY => opencode_source::OPENCODE_SOURCE_KEY,
            key => key,
        };
        if ingest_key == thalamus_payer_source::SOURCE_KEY {
            continue;
        }
        if !ingest.ingests(ingest_key) {
            continue;
        }
        if let Err(SourceDrainError::Failed(error)) = source.drain_fast(state, at, stop) {
            // Full drains own health and retries; a failed tail must not make a source look fresh.
            tracing::debug!(source = source.source_key(), %error, "fast tail read failed");
        }
    }
    for producer in producers {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        if producer.source_key != broca_source::BROCA_SOURCE_KEY
            || !ingest.ingests(&producer.source_key)
        {
            continue;
        }
        if let Err(error) = drain_once_in_lane(
            state,
            producer.client.as_ref(),
            &producer.source_key,
            broca_source::PAGE_LIMIT,
            at,
            harness_drain::DrainMode::Fast,
        ) {
            tracing::debug!(
                source = producer.source_key,
                ?error,
                "fast producer tail read failed"
            );
        }
    }
}

/// One pass of the refresher loop, without the sleep that follows it.
#[allow(clippy::too_many_arguments)]
fn run_tick(
    state: &SharedState,
    producers: &[Producer],
    file_sources: &FileSources,
    capacity: Option<&dyn CapacitySource>,
    pending_catalog: &mut Option<PendingCatalog>,
    multiplier: &mut MultiplierStage,
    pricing: &mut PricingWalk,
    reconciler: &mut QueueReconciler,
    stop: &AtomicBool,
) {
    #[cfg(all(test, target_os = "linux"))]
    let _measurement = fast_lane_measure::cycle(false);
    catalog_wake_tick(state, pending_catalog, multiplier, stop, true);
    run_iteration_all(
        state,
        producers,
        file_sources.opencode.as_deref(),
        file_sources.pi.as_deref(),
        file_sources.claude_code.as_deref(),
        file_sources.codex.as_deref(),
        &file_sources
            .harnesses
            .iter()
            .map(|source| source.as_ref())
            .collect::<Vec<_>>(),
        capacity,
        pending_catalog.is_some(),
        pricing,
        stop,
    );
    // After the iteration, so every tick drains and prices before spending
    // time on the queue check, and the check only ever takes its budget out of
    // a tick, never a whole ledger walk. Anything it enqueues is priced on the
    // next tick.
    reconciler.tick(state, stop);
    if let Err(error) = check_broca_segments_tick(state, current_time_ms(), stop) {
        if !stop.load(Ordering::Relaxed) {
            tracing::warn!(%error, "BROCA segment coverage check failed");
        }
    }
}

fn check_broca_segments_tick(
    state: &SharedState,
    now_ms: i64,
    stop: &AtomicBool,
) -> Result<usize, String> {
    if stop.load(Ordering::Relaxed) {
        return Ok(0);
    }
    // This advances only the coverage cursor and writes diagnostic evidence;
    // no usage or pricing projections need refreshing.
    state.ordered_tail_write(|tx| {
        let checked = interrupt_on_stop(tx, stop, || {
            astrocyte_core::broca::check_segments_in_tx(tx, now_ms)
        })
        .map_err(|error| error.to_string())?;
        Ok((checked, false))
    })
}

/// Test fixture over `run_iteration_full` with capacity and catalog absent.
#[cfg(test)]
pub(crate) fn run_iteration(state: &SharedState, producers: &[Producer], stop: &AtomicBool) {
    run_iteration_full(state, producers, None, false, stop);
}

/// Test fixture over `run_iteration_full` with the catalog absent.
#[cfg(test)]
pub(crate) fn run_iteration_with_capacity(
    state: &SharedState,
    producers: &[Producer],
    capacity: Option<&dyn CapacitySource>,
    stop: &AtomicBool,
) {
    run_iteration_full(state, producers, capacity, false, stop);
}

/// `catalog_pending` is true while a configured catalog has not yet answered
/// its discovery probe. Pricing is SKIPPED for that tick rather than run
/// without a catalog: the without-catalog path records `catalog_unconfigured`,
/// which means nobody asked, and here somebody asked and was refused. An
/// event left with no priced head is examined on the first tick after the
/// catalog connects; a wrong reason would have to be repriced by hand.
#[cfg(test)]
pub(crate) fn run_iteration_full(
    state: &SharedState,
    producers: &[Producer],
    capacity: Option<&dyn CapacitySource>,
    catalog_pending: bool,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        None,
        None,
        None,
        None,
        &[],
        capacity,
        catalog_pending,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[cfg(test)]
pub(crate) fn run_iteration_with_pi(
    state: &SharedState,
    producers: &[Producer],
    pi_source: &pi_source::PiSource,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        None,
        Some(pi_source),
        None,
        None,
        &[],
        None,
        false,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[cfg(test)]
pub(crate) fn run_iteration_with_claude_code(
    state: &SharedState,
    producers: &[Producer],
    claude_code_source: &claude_code_source::ClaudeCodeSource,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        None,
        None,
        Some(claude_code_source),
        None,
        &[],
        None,
        false,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[cfg(test)]
pub(crate) fn run_iteration_with_codex(
    state: &SharedState,
    producers: &[Producer],
    codex_source: &codex_source::CodexSource,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        None,
        None,
        None,
        Some(codex_source),
        &[],
        None,
        false,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[cfg(test)]
pub(crate) fn run_iteration_with_harness(
    state: &SharedState,
    producers: &[Producer],
    harness_source: &dyn harness_drain::HarnessSource,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        None,
        None,
        None,
        None,
        &[harness_source],
        None,
        false,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[cfg(test)]
pub(crate) fn run_iteration_with_file_sources(
    state: &SharedState,
    producers: &[Producer],
    opencode_source: &opencode_source::OpenCodeSource,
    pi_source: &pi_source::PiSource,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        Some(opencode_source),
        Some(pi_source),
        None,
        None,
        &[],
        None,
        false,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[allow(clippy::too_many_arguments)]
fn run_iteration_all(
    state: &SharedState,
    producers: &[Producer],
    opencode_source: Option<&opencode_source::OpenCodeSource>,
    pi_source: Option<&pi_source::PiSource>,
    claude_code_source: Option<&claude_code_source::ClaudeCodeSource>,
    codex_source: Option<&codex_source::CodexSource>,
    harness_sources: &[&dyn harness_drain::HarnessSource],
    capacity: Option<&dyn CapacitySource>,
    catalog_pending: bool,
    pricing: &mut PricingWalk,
    stop: &AtomicBool,
) {
    crate::quota_events::drain(state, stop);
    // Read once per tick from the committed desired configuration, so the
    // producers skipped here always match the generation in force.
    let ingest = state.desired_config().harness_ingest;
    // Payer sessions and rows must be committed before a transcript decides
    // whether a call is ready or still waiting for its payer.
    for source in harness_sources {
        if source.source_key() == thalamus_payer_source::SOURCE_KEY
            && ingesting(state, &ingest, thalamus_payer_source::SOURCE_KEY)
            && !stop.load(Ordering::Relaxed)
        {
            drain_harness(state, *source, stop);
        }
    }
    let opencode_source =
        opencode_source.filter(|_| ingesting(state, &ingest, opencode_source::OPENCODE_SOURCE_KEY));
    let pi_source = pi_source.filter(|_| ingesting(state, &ingest, pi_source::PI_SOURCE_KEY));
    let claude_code_source = claude_code_source
        .filter(|_| ingesting(state, &ingest, claude_code_source::CLAUDE_CODE_SOURCE_KEY));
    let codex_source =
        codex_source.filter(|_| ingesting(state, &ingest, codex_source::CODEX_SOURCE_KEY));

    let older: [Option<&dyn harness_drain::HarnessSource>; 4] = [
        opencode_source.map(|source| source as &dyn harness_drain::HarnessSource),
        pi_source.map(|source| source as &dyn harness_drain::HarnessSource),
        claude_code_source.map(|source| source as &dyn harness_drain::HarnessSource),
        codex_source.map(|source| source as &dyn harness_drain::HarnessSource),
    ];
    for source in older.into_iter().flatten() {
        if !stop.load(Ordering::Relaxed) {
            drain_harness(state, source, stop);
        }
    }

    for harness in harness_sources {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let key = harness.source_key();
        if key == thalamus_payer_source::SOURCE_KEY {
            continue;
        }
        if !ingesting(state, &ingest, key) {
            continue;
        }
        if key == colliculus_source::SOURCE_KEY {
            drain_media_if_due(state, *harness, stop, true);
        } else {
            drain_harness(state, *harness, stop);
        }
    }

    attribution_source::drain_all(state, stop, true);

    for producer in producers {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        if !ingesting(state, &ingest, &producer.source_key) {
            continue;
        }
        match drain_once(
            state,
            producer.client.as_ref(),
            &producer.source_key,
            broca_source::PAGE_LIMIT,
            current_time_ms(),
        ) {
            Ok(outcome) if outcome.quarantined_redeliveries == 0 => {
                state.clear_producer_degraded(&producer.source_key);
            }
            // The page committed, but a stored event came back with a
            // different payload; that is surfaced like any reader's quarantine.
            Ok(outcome) => harness_drain::mark_drain_failed(
                state,
                &producer.source_key,
                format!(
                    "{} {} deliveries were quarantined",
                    outcome.quarantined_redeliveries, producer.source_key
                ),
            ),
            // Shutdown interrupted the read: nothing was committed, and a stop
            // is neither success nor failure, so health is left as it was.
            Err(PullError::Stopped) => {}
            Err(error) => {
                let reason = format!("{error:?}");
                state.mark_producer_degraded(&producer.source_key, reason.clone());
                // A lineage-regression refusal must leave the durable producer row
                // byte-for-byte unchanged. Other failures retain the cursor but
                // expose unknown pending work durably, in a row created if the
                // source had none, so a store that exists but cannot be read
                // shows as a failed source rather than an absent one.
                let degraded = if matches!(error, PullError::LineageRegressed { .. }) {
                    Ok(())
                } else {
                    state.ordered_write(|tx| {
                        harness_drain::ensure_runtime_row_in_tx(tx, &producer.source_key)
                            .map_err(|error| error.to_string())?;
                        tx.execute(
                            "UPDATE source_runtime SET pending_unknown=1 WHERE source_key=?1",
                            [&producer.source_key],
                        )
                        .map_err(|error| error.to_string())?;
                        Ok(((), true))
                    })
                };
                log_producer_drain_failed(&producer.source_key, &reason, &degraded);
            }
        }
    }
    if !stop.load(Ordering::Relaxed) {
        if let Some((capacity, ticket)) = capacity.and_then(|capacity| {
            state
                .capacity_poll_ticket(current_time_ms())
                .map(|ticket| (capacity, ticket))
        }) {
            let result = capacity.poll();
            let now_ms = current_time_ms();
            let persisted = match &result {
                Ok(snapshot) => state.ordered_write(|tx| {
                    apply_snapshot_in_tx(tx, snapshot, now_ms)
                        .map_err(|error| format!("apply capacity snapshot: {error}"))?;
                    Ok(((), true))
                }),
                Err(error) => state.ordered_write(|tx| {
                    record_failed_attempt(tx, now_ms, &format!("{error:?}"))
                        .map_err(|failure| format!("record failed capacity poll: {failure}"))?;
                    Ok(((), true))
                }),
            };
            state
                .quota_bus()
                .finish_poll(ticket, persisted.as_ref().ok().and(result.as_ref().ok()));
            if let Err(error) = persisted {
                log_capacity_failed(&error);
            }
        }
    }
    if catalog_pending || stop.load(Ordering::Relaxed) {
        return;
    }
    #[cfg(any(test, feature = "test-support"))]
    if state.pricing_pass_held() {
        return;
    }
    if let Err(error) = run_pricing_walk(state, current_time_ms(), stop, pricing) {
        log_pricing_failed(&error);
    }
}

// The backfill and timestamp fixtures in these tests stay within i64.
#[allow(clippy::cast_possible_wrap)]
#[cfg(test)]
mod tests {
    include!("refresher/stop_catalog_tests.rs");

    use super::*;
    use crate::state::test_state;
    use astrocyte_core::catalog_client::CatalogCallError;
    use fusiform_protocol::{CatalogGetRequest, CatalogGetResponse};
    use std::sync::atomic::AtomicUsize;

    /// A producer that is always caught up and counts how often it is pulled,
    /// so a test can see whether a tick drained.
    pub(super) struct CountingSource {
        pub(super) pulls: Arc<AtomicUsize>,
    }

    impl SourceClient for CountingSource {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            self.pulls.fetch_add(1, Ordering::SeqCst);
            let cursor = SourceCursor {
                lineage: "counting:1".into(),
                ordinal: 1,
            };
            Ok(ExportPage {
                broca_facts: Vec::new(),
                set_aside: Vec::new(),
                cursor: cursor.clone(),
                high_water: cursor,
                events: Vec::new(),
                pending_known: 0,
                pending_unknown: false,
            })
        }
    }

    pub(super) static NEVER_STOP: AtomicBool = AtomicBool::new(false);

    /// A catalog peer that is not registered for the first `refusals` calls
    /// (the daemon-restart shape: every module booting at once, fusiform not
    /// yet live) and answers the discovery probe from then on.
    pub(super) struct LateCatalog {
        refusals: usize,
        pub(super) calls: AtomicUsize,
        /// Code served while refusing. `unavailable` is the warming peer;
        /// anything non-retryable models a producer this binary cannot talk to.
        refusal_code: &'static str,
    }

    impl LateCatalog {
        pub(super) fn warming(refusals: usize) -> Self {
            Self {
                refusals,
                calls: AtomicUsize::new(0),
                refusal_code: "unavailable",
            }
        }
    }

    impl CatalogConnection for LateCatalog {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call < self.refusals {
                return Err(CatalogCallError {
                    code: self.refusal_code.into(),
                    message: "route.open error: unknown_module 'fusiform' is not registered".into(),
                });
            }
            if request.at_ms == Some(0) {
                return Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "no record at 0: fusiform's history begins at 1786529249396. An empty answer would claim the catalog was empty; it was unobserved.".into(),
                });
            }
            let rate = serde_json::json!({"state":"priced","units":3_000,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
            Ok(serde_json::from_value(serde_json::json!({
                "source": "late-fake",
                "resolved_at_ms": request.at_ms,
                "catalog_version": 1,
                "models": {"anthropic/claude": {"rate.input": rate,
                    "rate.output": {"state":"priced","units":6000,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}},
                "withheld": [], "uncertain": [], "overridden": []
            }))
            .unwrap())
        }
    }

    pub(super) fn seed_unpriced_event(state: &SharedState) {
        state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT INTO usage_event (event_id,producer,producer_ref,recorded_at_ms,occurred_at_ms,provider,model,charge_basis)
                     VALUES ('ev-late','broca','ev-late',1,1_800_000_000_000,'anthropic','claude','metered_api')",
                    [],
                )
                .map_err(|e| e.to_string())?;
                tx.execute(
                    "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                     VALUES ((SELECT event_key FROM usage_event WHERE event_id='ev-late'),'input','token','billed','producer',10,0)",
                    [],
                )
                .map_err(|e| e.to_string())?;
                // This fixture stands in for recording, which queues the
                // event in the recording transaction.
                astrocyte_core::pricing::enqueue_if_unpriced_in_tx(tx, "ev-late")
                    .map_err(|e| e.to_string())?;
                Ok(((), true))
            })
            .unwrap();
    }

    pub(super) fn priced_charge_rows(state: &SharedState) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM priced_charge", [], |r| r.get(0))
                    .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    /// The pricing-pass hold must scope to the state it was taken on. Tests
    /// run as threads inside one process, so a hold that reached every state
    /// would let one test's held refresher silently skip the pricing pass of
    /// any test running beside it and fail that test's priced-row
    /// assertions. Here the held state's own iteration prices nothing while a
    /// second state's iteration prices its queued event.
    #[test]
    fn pricing_hold_on_one_state_does_not_hold_pricing_on_another() {
        let held = test_state();
        let _hold = hold_pricing_pass_for_test(&held);
        seed_unpriced_event(&held);
        held.install_catalog(Some(
            CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap(),
        ))
        .unwrap();
        run_iteration(&held, &[], &NEVER_STOP);
        assert_eq!(priced_charge_rows(&held), 0);

        let other = test_state();
        seed_unpriced_event(&other);
        other
            .install_catalog(Some(
                CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap(),
            ))
            .unwrap();
        run_iteration(&other, &[], &NEVER_STOP);
        assert_eq!(priced_charge_rows(&other), 1);
    }

    /// The production failure: a configured catalog that refuses its discovery
    /// probe at boot must not stop the daemon, must not price under a wrong
    /// reason while it waits, and must price on the first tick after the peer
    /// appears. Asserted by rows, not by which branch ran.
    #[test]
    fn catalog_unreachable_at_boot_defers_pricing_and_recovers_when_the_peer_appears() {
        let state = test_state();
        seed_unpriced_event(&state);
        let stop = AtomicBool::new(false);
        let late: Arc<dyn CatalogConnection> = Arc::new(LateCatalog::warming(2));
        let pending = PendingCatalog { connection: late };

        // Tick 1 and 2: peer refuses. Pricing must be SKIPPED, leaving no head
        // at all -- a `catalog_unconfigured` row here would be a lie that only a
        // manual reprice could correct.
        for _ in 0..2 {
            assert!(!pending.try_connect(&state, &NEVER_STOP).unwrap());
            assert!(state.catalog_client().is_none());
            run_iteration_full(&state, &[], None, true, &stop);
            assert_eq!(
                priced_charge_rows(&state),
                0,
                "no head while the catalog is pending"
            );
        }

        // Tick 3: peer is live. Discovery installs the catalog and the same
        // tick prices the waiting event.
        assert!(pending.try_connect(&state, &NEVER_STOP).unwrap());
        assert_eq!(
            state.catalog_client().unwrap().history_start_ms(),
            1_786_529_249_396
        );
        run_iteration_full(&state, &[], None, false, &stop);
        assert!(
            priced_charge_rows(&state) > 0,
            "first tick after the peer appears prices"
        );
        let outcomes: Vec<(String, Option<String>)> = state
            .with_conn(|conn| {
                let mut s = conn
                    .prepare("SELECT outcome,unpriced_reason FROM priced_charge")
                    .map_err(|e| e.to_string())?;
                let rows = s
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                    .map_err(|e| e.to_string())?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| e.to_string())?;
                Ok(rows)
            })
            .unwrap();
        assert!(
            outcomes.iter().any(|(o, _)| o == "priced"),
            "the waiting event resolves against the late catalog: {outcomes:?}"
        );
        assert!(
            !outcomes
                .iter()
                .any(|(_, r)| r.as_deref() == Some("catalog_unconfigured")),
            "a configured-but-late catalog must never be recorded as unconfigured: {outcomes:?}"
        );
    }

    #[test]
    fn history_status_transient_failure_defers_pricing_and_retries_each_tick() {
        use crate::fusiform_client::fixtures::{CatalogPeer, StatusMode};

        let peer = CatalogPeer::start_with_status(
            1_786_529_249_396,
            9,
            66_666,
            StatusMode::Transient { failures: 2 },
        );
        let state = test_state();
        seed_unpriced_event(&state);
        let access = Some(crate::peer_exchange::PeerAccess::Fixture(peer.path.clone()));
        let (mut pending, _) = configure_catalog(&state, access, &NEVER_STOP).unwrap();
        assert!(
            pending.is_some(),
            "boot must stay pending after status fails"
        );
        assert!(state.catalog_client().is_none());
        let mut pricing = PricingWalk::new(PricingBudget::WHOLE_QUEUE);
        let mut reconciler =
            QueueReconciler::new(QUEUE_RECONCILE_CHUNK, QUEUE_RECONCILE_TICK_BUDGET);
        run_tick(
            &state,
            &[],
            &FileSources::default(),
            None,
            &mut pending,
            &mut MultiplierStage::new(None),
            &mut pricing,
            &mut reconciler,
            &NEVER_STOP,
        );
        assert!(pending.is_some(), "the first tick's status still fails");
        assert!(state.catalog_client().is_none());
        assert_eq!(
            priced_charge_rows(&state),
            0,
            "pending is not catalog_unconfigured"
        );
        run_tick(
            &state,
            &[],
            &FileSources::default(),
            None,
            &mut pending,
            &mut MultiplierStage::new(None),
            &mut pricing,
            &mut reconciler,
            &NEVER_STOP,
        );
        assert!(pending.is_none(), "the second tick must rediscover status");
        assert_eq!(
            state.catalog_client().unwrap().history_start_ms(),
            1_786_529_249_396
        );
        assert!(
            priced_charge_rows(&state) > 0,
            "the recovered tick must price"
        );
        let unconfigured: i64 = state.with_conn(|conn| {
            conn.query_row("SELECT COUNT(*) FROM priced_charge WHERE unpriced_reason='catalog_unconfigured'",
                [], |row| row.get(0)).map_err(|error| error.to_string())
        }).unwrap();
        assert_eq!(unconfigured, 0);
        let requests = peer.finish();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["name"] == "catalog.status")
                .count(),
            4
        );
        assert!(
            !requests
                .iter()
                .any(|request| request["name"] == "catalog.get"
                    && request["arguments"]["at_ms"] == 0),
            "transient status errors must not trigger the text fallback: {requests:?}"
        );
    }

    #[test]
    fn history_status_error_stays_pending_without_a_text_probe() {
        use crate::fusiform_client::fixtures::{CatalogPeer, StatusMode};

        let peer =
            CatalogPeer::start_with_status(34_567, 9, 66_666, StatusMode::Refused("bad_request"));
        let state = test_state();
        seed_unpriced_event(&state);
        let access = Some(crate::peer_exchange::PeerAccess::Fixture(peer.path.clone()));
        let (mut pending, _) = configure_catalog(&state, access, &NEVER_STOP).unwrap();
        assert!(pending.is_some());
        assert!(state.catalog_client().is_none());
        run_tick(
            &state,
            &[],
            &FileSources::default(),
            None,
            &mut pending,
            &mut MultiplierStage::new(None),
            &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
            &mut QueueReconciler::new(QUEUE_RECONCILE_CHUNK, QUEUE_RECONCILE_TICK_BUDGET),
            &NEVER_STOP,
        );
        assert!(pending.is_some());
        assert!(state.catalog_client().is_none());
        assert_eq!(priced_charge_rows(&state), 0);
        let requests = peer.finish();
        assert_eq!(
            requests.len(),
            2,
            "startup and the next tick must try status"
        );
        assert!(
            requests
                .iter()
                .all(|request| request["name"] == "catalog.status"),
            "a status refusal is not evidence that the text fallback is safe: {requests:?}"
        );
    }

    /// The production crash loop of 2026-09-06: a producer one protocol version
    /// behind refused discovery with a code the classifier called non-retryable,
    /// try_connect returned Err, the refresher thread died, and the crash
    /// budget was spent in seconds. Pricing may wait; the thread may not die.
    /// Asserted on the effect: try_connect is Ok(false), nothing installed,
    /// and the next tick still runs ingest (the iteration returns normally).
    #[test]
    fn non_retryable_discovery_refusal_defers_pricing_and_does_not_kill_the_loop() {
        let state = test_state();
        let stop = AtomicBool::new(false);
        let peer: Arc<dyn CatalogConnection> = Arc::new(LateCatalog {
            refusals: usize::MAX,
            calls: AtomicUsize::new(0),
            refusal_code: "future_code",
        });
        let pending = PendingCatalog { connection: peer };
        assert_eq!(
            pending.try_connect(&state, &NEVER_STOP),
            Ok(false),
            "a non-retryable refusal defers; it is never an Err from try_connect"
        );
        assert!(state.catalog_client().is_none());
        run_iteration_full(&state, &[], None, true, &stop);
        assert_eq!(priced_charge_rows(&state), 0);
    }

    /// Startup with a configured catalog that is not yet registered returns a
    /// pending handle and no error: the daemon starts and the loop retries.
    #[test]
    fn configure_catalog_with_unreachable_peer_is_pending_not_fatal() {
        let state = test_state();
        // A connection file that no daemon serves: transport builds, discovery refuses.
        let dir =
            std::env::temp_dir().join(format!("astrocyte-late-catalog-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("subc-connection.json");
        let access = Some(crate::peer_exchange::PeerAccess::Fixture(path.clone()));
        subc_transport::connection_file::write_atomic(
            &path,
            &subc_transport::connection_file::ConnectionInfo {
                schema: subc_transport::connection_file::SCHEMA_VERSION,
                wire_version: None,
                endpoints: vec![subc_transport::connection_file::Endpoint {
                    host: "127.0.0.1".into(),
                    port: 1,
                }],
                key: vec![7; subc_transport::KEY_LEN],
                daemon_id: [9; 16],
                pid: std::process::id(),
                daemon_ver: "test".into(),
            },
        )
        .unwrap();
        let (pending, plan_prices) = configure_catalog(&state, access, &NEVER_STOP)
            .expect("unreachable peer is not a startup error");
        assert!(pending.is_some(), "discovery deferred, handle returned");
        assert!(
            plan_prices.is_some(),
            "the same fusiform client serves plan prices"
        );
        assert!(state.catalog_client().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod loop_tests {
    use super::*;
    use crate::state::test_state;

    struct ReadySource;
    impl SourceClient for ReadySource {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            Ok(ExportPage {
                broca_facts: Vec::new(),
                set_aside: Vec::new(),
                cursor: SourceCursor {
                    lineage: "live:1".into(),
                    ordinal: 9,
                },
                high_water: SourceCursor {
                    lineage: "live:1".into(),
                    ordinal: 9,
                },
                events: Vec::new(),
                pending_known: 0,
                pending_unknown: false,
            })
        }
    }

    struct PricingSource;

    #[test]
    fn hourly_summary_tick_publishes_real_drain_facts_without_changing_the_served_report() {
        let state = test_state();
        run_iteration_with_capacity(
            &state,
            &[Producer {
                source_key: "broca".into(),
                client: Arc::new(PricingSource),
            }],
            None,
            &AtomicBool::new(false),
        );
        let before =
            crate::ops::dispatch(&state, "spend.workclass.report", serde_json::json!({})).unwrap();
        assert_eq!(before["rows"][0]["events_total"], 1);
        let mut summaries = astrocyte_core::report::SummaryMaintenance::default();
        let stop = AtomicBool::new(true);
        summary_tick(&state, &mut summaries, &stop);
        assert_eq!(
            state
                .with_conn(|conn| conn
                    .query_row("SELECT count(*) FROM summary_cell", [], |row| row
                        .get::<_, i64>(0))
                    .map_err(|error| error.to_string()))
                .unwrap(),
            0
        );
        stop.store(false, Ordering::Relaxed);
        summary_tick(&state, &mut summaries, &stop);
        assert_eq!(
            crate::ops::dispatch(&state, "spend.workclass.report", serde_json::json!({})).unwrap(),
            before
        );
        let (facts,cells,dirty):(i64,i64,i64)=state.with_conn(|conn|conn.query_row("SELECT (SELECT count(*) FROM usage_event),(SELECT count(*) FROM summary_cell),(SELECT count(*) FROM summary_dirty_hour)",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).map_err(|error|error.to_string())).unwrap();
        assert_eq!(facts, 1);
        assert!(cells > 0);
        assert_eq!(dirty, 0);
    }

    /// After a summary format change every hour is stale. The tick reports
    /// that backlog when its budget runs out, so the loop keeps rebuilding
    /// between full ticks, and stops reporting it once every hour is current.
    #[test]
    fn a_stale_summary_backlog_is_reported_until_every_hour_is_rebuilt() {
        let directory = tempfile::tempdir().unwrap();
        let state = crate::state::leased_test_state(&directory);
        for hour in 0..6_i64 {
            crate::ops::dispatch(
                &state,
                "spend.event.ingest",
                serde_json::json!({
                "producer":"fixture","producer_ref":format!("backlog-{hour}"),"provider":"provider",
                "model":"model","charge_basis":"metered_api","occurred_at_ms":hour*3_600_000+1,
                "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture",
                    "quantity_units":12,"quantity_exponent":0}]}),
            )
            .unwrap();
        }
        let stop = AtomicBool::new(false);
        let mut summaries = astrocyte_core::report::SummaryMaintenance::default();
        while summary_tick_within(&state, &mut summaries, &stop, Duration::ZERO) {}
        let query = |sql: &str| -> i64 {
            state
                .with_conn(|conn| {
                    conn.query_row(sql, [], |row| row.get(0))
                        .map_err(|error| error.to_string())
                })
                .unwrap()
        };
        // Every hour was just built, so its format is the current one.
        let current = query("SELECT min(format_version) FROM summary_hour");
        assert_eq!(
            current,
            query("SELECT max(format_version) FROM summary_hour")
        );
        let stale = || {
            query(&format!(
                "SELECT count(*) FROM summary_hour
                 WHERE built_revision IS NOT revision OR format_version IS NOT {current}"
            ))
        };
        assert_eq!(stale(), 0);
        state
            .ordered_write(|tx| {
                tx.execute(
                    "UPDATE summary_hour SET format_version=format_version-1",
                    [],
                )
                .map_err(|error| error.to_string())?;
                Ok(((), false))
            })
            .unwrap();
        assert_eq!(stale(), 6);
        let mut behind_ticks = 0;
        while summary_tick_within(&state, &mut summaries, &stop, Duration::ZERO) {
            behind_ticks += 1;
            assert!(behind_ticks <= 30, "the rebuild must finish");
        }
        assert!(
            behind_ticks >= 5,
            "a zero budget stops after each hour: {behind_ticks}"
        );
        assert_eq!(stale(), 0);
        // With nothing left to rebuild, a tick reports no backlog, even while
        // the audit of a fresh store is still due.
        assert!(!summary_tick_within(
            &state,
            &mut summaries,
            &stop,
            Duration::ZERO
        ));
    }

    #[test]
    fn hourly_report_snapshot_keeps_old_summary_money_across_a_write_and_publication() {
        use astrocyte_core::{ingest, verdict};
        let directory = tempfile::tempdir().unwrap();
        let state = crate::state::leased_test_state(&directory);
        let result=crate::ops::dispatch(&state,"spend.event.ingest",serde_json::json!({"producer":"fixture","producer_ref":"snapshot-one","provider":"provider","model":"model","charge_basis":"metered_api","occurred_at_ms":1000,
            "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":12,"quantity_exponent":0}]})).unwrap();
        let event_id = result["event_id"].as_str().unwrap().to_string();
        // Hand-valued setup isolates snapshot behavior from catalog/network availability.
        state.ordered_write(|tx| {
            tx.execute("UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_id=?1",[&event_id]).map_err(|error|error.to_string())?;
            tx.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms)
                VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),'input','token',1,'priced','metered_api',0,'USD',100,1,9,'none','fixture',1,'{}','pricing_pass',1)",[&event_id]).map_err(|error|error.to_string())?;
            Ok(((),true))
        }).unwrap();
        let mut summaries = astrocyte_core::report::SummaryMaintenance::default();
        let stop = AtomicBool::new(false);
        summary_tick(&state, &mut summaries, &stop);
        let reader = rusqlite::Connection::open_with_flags(
            directory.path().join("ledger.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        reader.busy_timeout(Duration::from_secs(5)).unwrap();
        reader.pragma_update(None, "foreign_keys", true).unwrap();
        let snapshot = reader.unchecked_transaction().unwrap();
        let request = verdict::VerdictRequest {
            project_root: None,
            provider: None,
            model: None,
            est_cost_nanos: None,
        };
        let before = verdict::evaluate(&snapshot, &request).unwrap();
        assert_eq!(before.consumed_nanos["USD"], 100);
        state
            .ordered_write(|tx| {
                ingest::record_adjustment_in_tx(
                    tx,
                    &ingest::AdjustmentRecordRequest {
                        event_id: event_id.clone(),
                        currency: "USD".into(),
                        amount_nanos: -25,
                        reason: ingest::AdjustmentReason::Refund,
                        occurred_at_ms: 2,
                        note: None,
                    },
                    &ingest::AdjustmentWriteContext::correction_ingest("snapshot-refund".into()),
                    3,
                )
                .map_err(|error| format!("{error:?}"))?;
                Ok(((), true))
            })
            .unwrap();
        summary_tick(&state, &mut summaries, &stop);
        assert_eq!(
            serde_json::to_vec(&verdict::evaluate(&snapshot, &request).unwrap()).unwrap(),
            serde_json::to_vec(&before).unwrap()
        );
        snapshot.commit().unwrap();
        let after = crate::ops::dispatch(&state, "budget.verdict", serde_json::json!({})).unwrap();
        assert_eq!(after["consumed_nanos"]["USD"], 75);
        let (facts,adjustments,dirty):(i64,i64,i64)=state.with_conn(|conn|conn.query_row("SELECT (SELECT count(*) FROM usage_event),(SELECT count(*) FROM adjustment),(SELECT count(*) FROM summary_dirty_hour)",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).map_err(|error|error.to_string())).unwrap();
        assert_eq!((facts, adjustments, dirty), (1, 1, 0));
    }

    impl SourceClient for PricingSource {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            let event = crate::wire_map::map_fact(
                serde_json::from_value(crate::wire_map::wire_fixture()).unwrap(),
            )
            .unwrap();
            Ok(ExportPage {
                broca_facts: Vec::new(),
                set_aside: Vec::new(),
                cursor: SourceCursor {
                    lineage: "pricing:1".into(),
                    ordinal: 7,
                },
                high_water: SourceCursor {
                    lineage: "pricing:1".into(),
                    ordinal: 7,
                },
                events: vec![event],
                pending_known: 0,
                pending_unknown: false,
            })
        }
    }

    struct FailedCapacity;

    impl CapacitySource for FailedCapacity {
        fn poll(&self) -> Result<CapacitySnapshot, PullError> {
            Err(PullError::Unavailable("qta unavailable".into()))
        }
    }

    struct ReadyCapacity;

    impl CapacitySource for ReadyCapacity {
        fn poll(&self) -> Result<CapacitySnapshot, PullError> {
            Ok(CapacitySnapshot {
                windows: vec![astrocyte_core::capacity::CapacityWindow {
                    provider: "codex".into(),
                    api_provider: Some("openai".into()),
                    account_ref: "account-a".into(),
                    pool: "primary".into(),
                    pool_key: "primary".into(),
                    slot_kind: "primary".into(),
                    external_id: None,
                    title: None,
                    extra_ordinal: None,
                    legacy_pool_key: None,
                    used_percent: 25.0,
                    raw_used_percent: None,
                    resets_at: None,
                    window_minutes: Some(300),
                    used_count: None,
                    total_count: None,
                    source: Some("oauth".into()),
                    fetched_at: Some("2026-09-05T08:00:00Z".into()),
                    stale_since: None,
                    stale_class: None,
                    breakdown: None,
                }],
                observations: Vec::new(),
                provider_readings: Vec::new(),
                complete_providers: None,
                degraded_entries: 0,
            })
        }
    }

    /// Waits until `key`'s cursor has advanced to the source's page. Bounded
    /// only so that a producer that never advances fails by name instead of
    /// hanging; a healthy loop gets there on its first tick.
    fn wait_for_success(state: &SharedState, key: &str) {
        crate::test_support::wait_until(&format!("{key} advancing its cursor"), || {
            state.with_conn(|conn| conn.query_row("SELECT COUNT(*) FROM source_runtime WHERE source_key=?1 AND consumed_ordinal=9 AND last_success_ms>0", [key], |r| r.get::<_,i64>(0)).map_err(|e|e.to_string())).unwrap() == 1
        });
    }

    /// Runs `run_loop` over `producers` on its own thread, returning a guard
    /// that stops and joins the loop when the test ends, also when an
    /// assertion fails before the test stops it.
    fn spawn_run_loop(
        state: &Arc<SharedState>,
        producers: Vec<Producer>,
        stop: &Arc<AtomicBool>,
        finished: std::sync::mpsc::Sender<()>,
    ) -> crate::test_support::Background<()> {
        let state = Arc::clone(state);
        let loop_stop = Arc::clone(stop);
        let thread = std::thread::spawn(move || {
            run_loop(state, producers, 60, loop_stop);
            let _ = finished.send(());
        });
        let stop = Arc::clone(stop);
        crate::test_support::Background::new("the refresher loop", thread, move || {
            stop.store(true, Ordering::Relaxed)
        })
    }

    #[test]
    fn failing_capacity_poll_does_not_stop_spend_cursor_advancing() {
        let state = test_state();
        state.configure_capacity_runtime(true, 5_000, 10).unwrap();
        let producer = Producer {
            source_key: "broca".into(),
            client: Arc::new(ReadySource),
        };

        run_iteration_with_capacity(
            &state,
            &[producer],
            Some(&FailedCapacity),
            &AtomicBool::new(false),
        );

        let (cursor, last_attempt, runtime): (i64, Option<i64>, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT source_runtime.consumed_ordinal,
                            capacity_runtime.last_attempt_ms,
                            capacity_runtime.runtime_variant
                     FROM source_runtime CROSS JOIN capacity_runtime
                     WHERE source_runtime.source_key='broca'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(cursor, 9);
        assert!(
            last_attempt.is_some(),
            "the failed capacity attempt must be durable"
        );
        assert_eq!(runtime, "configured_unavailable");
    }

    #[test]
    fn failing_capacity_poll_does_not_stop_spend_pricing_pass() {
        let state = test_state();
        state.configure_capacity_runtime(true, 5_000, 10).unwrap();
        let producer = Producer {
            source_key: "broca".into(),
            client: Arc::new(PricingSource),
        };

        run_iteration_with_capacity(
            &state,
            &[producer],
            Some(&FailedCapacity),
            &AtomicBool::new(false),
        );

        let priced_rows: i64 = state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM priced_charge", [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert!(
            priced_rows > 0,
            "a failed capacity poll must not skip the spend pricing pass"
        );
    }

    #[test]
    fn failing_spend_drain_does_not_stop_capacity_applying() {
        let state = test_state();
        state.configure_capacity_runtime(true, 7_000, 10).unwrap();
        let producer = Producer {
            source_key: "broca".into(),
            client: Arc::new(UnavailableSource),
        };

        run_iteration_with_capacity(
            &state,
            &[producer],
            Some(&ReadyCapacity),
            &AtomicBool::new(false),
        );

        let (windows, pending_unknown, runtime): (i64, i64, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT (SELECT COUNT(*) FROM capacity_window),
                            source_runtime.pending_unknown,
                            capacity_runtime.runtime_variant
                     FROM source_runtime CROSS JOIN capacity_runtime
                     WHERE source_runtime.source_key='broca'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(windows, 1);
        assert_eq!(pending_unknown, 1);
        assert_eq!(runtime, "active");
    }

    #[test]
    fn unconfigured_capacity_stays_disabled_and_fresh() {
        let state = test_state();
        let config = config::AstrocyteConfig {
            poll_interval_secs: 11,
            reasoning_billed_as_output: Vec::new(),
            subscription_multiplier: config::intentional_empty().config.subscription_multiplier,
            harness_ingest: config::HarnessIngest::all_on(),
        };

        let capacity = configured_capacity(&state, &config, None).unwrap();
        run_iteration_with_capacity(&state, &[], capacity.as_deref(), &AtomicBool::new(false));

        assert!(capacity.is_none());

        let freshness = state
            .with_conn(|conn| {
                astrocyte_core::capacity::freshness(conn).map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(freshness.runtime_variant, "disabled");
        assert_eq!(freshness.effective_cadence_ms, 11_000);
        let runtime = state.public_projection().health(i64::MAX).capacity_runtime;
        assert_eq!(runtime["runtimeVariant"], "disabled");
        assert_eq!(runtime["effectiveHorizonMs"], 44_000);
        assert_eq!(runtime["stale"], false);
        assert!(!astrocyte_core::capacity::is_stale(
            &freshness,
            i64::MAX,
            freshness.effective_cadence_ms.saturating_mul(4),
        ));
    }

    fn add_second_producer_runtime(state: &SharedState) {
        state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT INTO source_runtime (source_key,source_module,desired_generation,lineage,consumed_ordinal,high_water_ordinal,last_success_ms,pending_known,pending_unknown) VALUES ('second-producer','second-producer',(SELECT generation FROM desired_config WHERE id=1),NULL,NULL,NULL,NULL,0,1)",
                    [],
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
    }

    #[test]
    fn two_producer_failures_are_isolated_in_both_directions() {
        let stop = AtomicBool::new(false);
        let state = test_state();
        add_second_producer_runtime(&state);
        run_iteration(
            &state,
            &[
                Producer {
                    source_key: "second-producer".into(),
                    client: Arc::new(UnavailableSource),
                },
                Producer {
                    source_key: "broca".into(),
                    client: Arc::new(ReadySource),
                },
            ],
            &stop,
        );
        let cursors: (Option<i64>, Option<i64>) = state
            .with_conn(|conn| {
                Ok((
                    conn.query_row(
                        "SELECT consumed_ordinal FROM source_runtime WHERE source_key='broca'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())?,
                    conn.query_row(
                        "SELECT consumed_ordinal FROM source_runtime WHERE source_key='second-producer'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())?,
                ))
            })
            .unwrap();
        assert_eq!(cursors, (Some(9), None));
        assert!(state.producer_degraded_reason("second-producer").is_some());
        assert!(state.producer_degraded_reason("broca").is_none());

        let state = test_state();
        add_second_producer_runtime(&state);
        run_iteration(
            &state,
            &[
                Producer {
                    source_key: "broca".into(),
                    client: Arc::new(UnavailableSource),
                },
                Producer {
                    source_key: "second-producer".into(),
                    client: Arc::new(ReadySource),
                },
            ],
            &stop,
        );
        let second: Option<i64> = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT consumed_ordinal FROM source_runtime WHERE source_key='second-producer'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(second, Some(9));
        assert!(state.producer_degraded_reason("broca").is_some());
        assert!(state.producer_degraded_reason("second-producer").is_none());
    }

    /// The sources a production tick drains are fixed: the only producer
    /// drained through `drain_once` is BROCA's store reader, and Pi and
    /// OpenCode are read from disk. Asserted on the lists themselves -- the
    /// producer keys the refresher builds and the cursor rows a publish plus
    /// one tick leave behind -- because a retired producer that is still
    /// drained would show up there even if its drain happened to succeed.
    #[test]
    fn a_fully_configured_tick_drains_exactly_broca_pi_and_opencode() {
        let temp = tempfile::TempDir::new().unwrap();
        let desired = config::canonicalize(config::AstrocyteConfig {
            poll_interval_secs: 60,
            reasoning_billed_as_output: Vec::new(),
            subscription_multiplier: config::intentional_empty().config.subscription_multiplier,
            harness_ingest: config::HarnessIngest::all_on(),
        })
        .unwrap();

        let producers = refresher_producers(&Arc::new(AtomicBool::new(false)));
        let producer_keys: Vec<&str> = producers
            .iter()
            .map(|producer| producer.source_key.as_str())
            .collect();
        assert_eq!(producer_keys, ["broca"]);

        let state = test_state();
        state
            .ordered_write(|tx| publish_desired_transaction(tx, &desired, 1))
            .unwrap();
        // OpenCode is pointed at a directory where its database should be, so
        // its attempt is also visible as a degradation; Pi reads one real
        // session. Both leave a cursor row.
        let opencode_database = temp.path().join("opencode-database-is-a-directory");
        std::fs::create_dir(&opencode_database).unwrap();
        let opencode_source = opencode_source::OpenCodeSource::at(opencode_database);
        let pi_session = temp.path().join("pi").join("encoded");
        std::fs::create_dir_all(&pi_session).unwrap();
        std::fs::write(
            pi_session.join("session.jsonl"),
            concat!(
                r#"{"type":"session","id":"pi-session","timestamp":"2026-09-01T00:00:00.000Z","cwd":"/pi/project"}"#,
                "\n",
                r#"{"type":"message","id":"entry-1","timestamp":"2026-09-01T00:00:01.000Z","message":{"role":"assistant","provider":"anthropic","model":"claude-sonnet-4","responseId":"pi-response","usage":{"input":11,"output":7,"cacheRead":13,"cacheWrite":17,"reasoning":2,"totalTokens":50}}}"#,
                "\n"
            ),
        )
        .unwrap();
        let pi_source = pi_source::PiSource::at(temp.path().join("pi"));
        // The refresher's BROCA reader points at a store that does not exist
        // under test; the tick is driven with the same source keys over an
        // in-memory client instead, so each leaves a cursor row.
        let ticked: Vec<Producer> = producer_keys
            .iter()
            .map(|source_key| Producer {
                source_key: (*source_key).into(),
                client: Arc::new(ReadySource),
            })
            .collect();
        run_iteration_with_file_sources(
            &state,
            &ticked,
            &opencode_source,
            &pi_source,
            &AtomicBool::new(false),
        );

        let runtime_keys: Vec<String> = state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare("SELECT source_key FROM source_runtime ORDER BY source_key")
                    .map_err(|error| error.to_string())?;
                let keys = statement
                    .query_map([], |row| row.get(0))
                    .map_err(|error| error.to_string())?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(|error| error.to_string());
                keys
            })
            .unwrap();
        assert_eq!(runtime_keys, ["broca", "opencode", "pi"]);
        assert!(state.producer_degraded_reason("opencode").is_some());
        assert!(state.producer_degraded_reason("broca").is_none());
        assert!(state.producer_degraded_reason("pi").is_none());
    }

    #[test]
    fn failed_producer_degrades_only_itself_and_second_producer_advances() {
        let state = Arc::new(test_state());
        state.ordered_write(|tx| {
            tx.execute("UPDATE source_runtime SET lineage='old:1',consumed_ordinal=1,high_water_ordinal=1,last_success_ms=1,pending_unknown=0 WHERE source_key='broca'", []).unwrap();
            Ok(((),true))
        }).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let (finished, _) = std::sync::mpsc::channel();
        let refresher = spawn_run_loop(
            &state,
            vec![
                Producer {
                    source_key: "broca".into(),
                    client: Arc::new(UnavailableSource),
                },
                Producer {
                    source_key: "second-producer".into(),
                    client: Arc::new(ReadySource),
                },
            ],
            &stop,
            finished,
        );
        wait_for_success(&state, "second-producer");
        refresher.stop_and_join();
        let stored: (i64,i64,i64) = state.with_conn(|conn| conn.query_row("SELECT consumed_ordinal,last_success_ms,pending_unknown FROM source_runtime WHERE source_key='broca'", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|e|e.to_string())).unwrap();
        assert_eq!(stored, (1, 1, 1));
    }

    #[test]
    fn shutdown_mid_interval_does_not_wait_sixty_seconds() {
        let state = Arc::new(test_state());
        let stop = Arc::new(AtomicBool::new(false));
        let (finished, done) = std::sync::mpsc::channel();
        let mut refresher = spawn_run_loop(
            &state,
            vec![Producer {
                source_key: "test".into(),
                client: Arc::new(ReadySource),
            }],
            &stop,
            finished,
        );
        wait_for_success(&state, "test");
        // Stop only once the loop has begun the wait after its first tick.
        // A stop that landed earlier, while the tick was still running, would
        // end the loop at its next check without the wait ever seeing it.
        crate::test_support::wait_until("the loop beginning its 60 s interval", || {
            state.interval_waits_for_test() >= 1
        });
        refresher.signal_stop();
        // The interval is 60 s, so a loop that slept through the stop could
        // not finish within half of it. Nothing healthy comes near this bound:
        // the wait checks the stop at least every 250 ms.
        assert!(
            done.recv_timeout(Duration::from_secs(30)).is_ok(),
            "stop must interrupt the poll interval"
        );
        refresher.stop_and_join();
    }
}

// cortexkit-log prefixes component targets with the supervised module id.
// Keep these emission boundaries callable so tests exercise the actual events.
pub fn log_config_retained(failure: &str) {
    tracing::warn!(target: "config", %failure, "desired configuration unavailable; retaining committed generation");
}

/// `degraded` is whether the failure was also recorded durably in the
/// producer's runtime row.
pub fn log_producer_drain_failed(producer: &str, reason: &str, degraded: &Result<(), String>) {
    tracing::warn!(target: "drain", %producer, %reason, ?degraded, "producer drain failed");
}

pub fn log_capacity_failed(error: &str) {
    tracing::error!(target: "capacity", %error, "capacity poll persistence failed");
}

pub fn log_pricing_failed(error: &str) {
    tracing::error!(target: "pricing", %error, "pricing pass failed");
}

#[cfg(test)]
mod discovery_logging_tests {
    use std::process::Command;

    const CHILD_TEST_ENV: &str = "ASTROCYTE_DISCOVERY_LOG_TEST";
    const TEST_NAME: &str = "store_presence_logs_only_when_it_changes";

    #[test]
    fn set_aside_evidence_is_atomic_and_warns_once() {
        const NAME: &str = "set_aside_evidence_is_atomic_and_warns_once";
        if std::env::var(CHILD_TEST_ENV).as_deref() == Ok(NAME) {
            let handle = cortexkit_log::init_from_env().unwrap();
            let state = crate::state::test_state();
            let evidence = [super::SetAsideRecord {
                locator: "fixture.jsonl:line:2".into(),
                reason: "missing identity".into(),
            }];
            let rollback: Result<(), String> = state.ordered_write(|tx| {
                super::harness_drain::record_set_aside_in_tx(tx, "pi", &evidence, 50)?;
                Err("forced rollback".into())
            });
            assert!(rollback.is_err());
            for now in [100, 200] {
                let newly_seen = state
                    .ordered_write(|tx| {
                        let newly_seen =
                            super::harness_drain::record_set_aside_in_tx(tx, "pi", &evidence, now)?;
                        Ok((newly_seen, true))
                    })
                    .unwrap();
                assert_eq!(newly_seen.len(), usize::from(now == 100));
                super::harness_drain::warn_set_aside("pi", &newly_seen);
            }
            super::harness_drain::assert_set_aside(&state, "pi", 1);
            let times: (i64, i64) = state.with_conn(|conn| conn.query_row("SELECT first_seen_ms,last_seen_ms FROM set_aside_record WHERE source_key='pi'", [], |r| Ok((r.get(0)?,r.get(1)?))).map_err(|e| e.to_string())).unwrap();
            assert_eq!(times, (100, 200));
            let contents = std::fs::read_to_string(handle.path()).unwrap();
            let lines: Vec<_> = contents.lines().collect();
            assert_eq!(lines.len(), 1, "{contents}");
            assert!(matches!(
                cortexkit_log::parse_line(lines[0]).unwrap().level,
                cortexkit_log::ParsedLevel::Warn
            ));
            for field in ["pi", "fixture.jsonl:line:2", "missing identity"] {
                assert!(lines[0].contains(field), "{}", lines[0]);
            }
            return;
        }
        let home = tempfile::tempdir().unwrap();
        let output = crate::test_support::output_within(
            Command::new(std::env::current_exe().unwrap()).args(["--exact", "refresher::discovery_logging_tests::set_aside_evidence_is_atomic_and_warns_once", "--nocapture"])
                .env(CHILD_TEST_ENV, NAME).env("SUBC_MODULE_ID", "astrocyte").env("CK_LOG", "trace").env("XDG_DATA_HOME", home.path()),
            "set-aside warning test child",
        );
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn store_presence_logs_only_when_it_changes() {
        if std::env::var(CHILD_TEST_ENV).as_deref() == Ok(TEST_NAME) {
            let handle = cortexkit_log::init_from_env().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().join("pi-sessions");
            let source = super::pi_source::PiSource::at(root.clone());
            let state = crate::state::test_state();
            let drain = || {
                source
                    .drain(&state, 100, &std::sync::atomic::AtomicBool::new(false))
                    .unwrap();
            };

            drain();
            drain();
            std::fs::create_dir_all(&root).unwrap();
            drain();
            drain();
            std::fs::remove_dir_all(&root).unwrap();
            drain();
            drain();

            let contents = std::fs::read_to_string(handle.path()).unwrap();
            let lines: Vec<_> = contents.lines().collect();
            assert_eq!(lines.len(), 3, "{contents}");
            let expected = ["absent", "present", "absent"];
            for (line, outcome) in lines.iter().zip(expected) {
                let parsed = cortexkit_log::parse_line(line).unwrap();
                assert_eq!(parsed.logger, "astrocyte.drain", "{line}");
                assert!(
                    matches!(parsed.level, cortexkit_log::ParsedLevel::Info),
                    "{line}"
                );
                assert!(
                    line.contains("source=pi") || line.contains("source=\"pi\""),
                    "{line}"
                );
                let path = root.display().to_string();
                assert!(
                    line.contains(&format!("path={path}"))
                        || line.contains(&format!("path=\"{path}\"")),
                    "{line}"
                );
                assert!(
                    line.contains(&format!("outcome={outcome}"))
                        || line.contains(&format!("outcome=\"{outcome}\"")),
                    "{line}"
                );
            }
            return;
        }

        // Logger configuration is process-global, so each capture runs alone.
        let data_home = std::env::temp_dir().join(format!(
            "astrocyte-discovery-log-{}-{TEST_NAME}",
            std::process::id()
        ));
        std::fs::create_dir_all(&data_home).unwrap();
        let output = crate::test_support::output_within(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "refresher::discovery_logging_tests::store_presence_logs_only_when_it_changes",
                    "--nocapture",
                ])
                .env(CHILD_TEST_ENV, TEST_NAME)
                .env("SUBC_MODULE_ID", "astrocyte")
                .env("CK_LOG", "trace")
                .env("XDG_DATA_HOME", &data_home),
            "store discovery log test child",
        );
        std::fs::remove_dir_all(data_home).unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success()
                && stdout.contains("running 1 test")
                && stdout.contains(
                    "test refresher::discovery_logging_tests::store_presence_logs_only_when_it_changes ... ok"
                ),
            "{stdout}\n{stderr}"
        );
    }
}

#[cfg(test)]
mod multiplier_stage_tests {
    use super::*;

    /// Records today's observation for every quota provider in the production
    /// config into a COPY of the live store (`MULTIPLIER_REHEARSAL_DB`, already
    /// migrated), through the real fusiform client on the daemon connection
    /// file `MULTIPLIER_REHEARSAL_SUBC`: the same read, resolve and append steps
    /// the daily stage runs. Both fusiform calls are reads. Ignored by default:
    /// it needs the copy and a live daemon.
    #[test]
    #[ignore]
    fn live_rehearsal_on_a_store_copy() {
        let database = std::env::var("MULTIPLIER_REHEARSAL_DB").expect("MULTIPLIER_REHEARSAL_DB");
        let subc = std::env::var("MULTIPLIER_REHEARSAL_SUBC").expect("MULTIPLIER_REHEARSAL_SUBC");
        let desired =
            config::decode(include_str!("../../../docs/prod-config/astrocyte-v2.jsonc")).unwrap();
        let config::MultiplierSetting::Recorded(config) = desired.config.subscription_multiplier
        else {
            unreachable!("a decoded document records the member");
        };
        let client = Arc::new(crate::fusiform_client::FusiformClient::for_test(subc).unwrap());
        let connection: Arc<dyn CatalogConnection> = client.clone();
        let catalog = CatalogClient::connect(connection).unwrap();
        let mut conn = rusqlite::Connection::open(database).unwrap();
        let now = current_time_ms();
        let end = now - now.rem_euclid(DAY_MS);
        for quota_provider in config.quota_providers.keys() {
            let plan = multiplier::plan_observation(
                &conn,
                &config,
                quota_provider,
                end - MULTIPLIER_INTERVAL_MS,
                end,
            )
            .unwrap();
            let rates = multiplier::resolve_rates(
                &catalog,
                &multiplier::catalog_models(&config, &plan),
                now,
            )
            .unwrap();
            let plans =
                multiplier::resolve_plans(Some(client.as_ref()), &config, &plan.provider, now)
                    .unwrap();
            let observation = multiplier::compute_observation(
                &conn,
                &config,
                &plan,
                &rates,
                &plans,
                now,
                &["openai".into()],
            )
            .unwrap();
            let tx = conn.transaction().unwrap();
            multiplier::insert_observation_in_tx(&tx, &observation).unwrap();
            tx.commit().unwrap();
            println!(
                "{quota_provider} {} delta% {} subscription ${:.2} fee ${:.2} api ${:.2} multiplier {:?} fee_multiplier {:?} excluded {} unpriced {} outside {} untested {} low_res {}",
                observation.observation_day,
                observation.delta_percent,
                observation.subscription_nanos as f64 / 1e9,
                observation.fee_paid_nanos as f64 / 1e9,
                observation.api_nanos as f64 / 1e9,
                observation.multiplier,
                observation.fee_multiplier,
                serde_json::to_string(&observation.accounts_excluded).unwrap(),
                serde_json::to_string(&observation.unpriced_tokens).unwrap(),
                observation.tokens_outside_quota_coverage,
                observation.base_rate_tier_untested_tokens,
                observation.low_resolution,
            );
            for price in &observation.plan_prices {
                println!(
                    "  plan {} {} {:?} {:?} overdue {}",
                    price.account_ref,
                    price.plan,
                    price.source,
                    price.monthly_nanos,
                    price.plan_price_overdue
                );
            }
        }
    }
}
