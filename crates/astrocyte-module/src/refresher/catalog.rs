//! Catalog discovery and wake handling are separate from scheduling so peer retries stay together.

use super::*;

/// Connect a test catalog without a shutdown flag, using the same discovery
/// probe as the refresher. No local catalog file is read.
#[cfg(test)]
pub fn connect_catalog(
    connection: Arc<dyn CatalogConnection>,
) -> Result<CatalogClient, astrocyte_core::catalog_client::CatalogClientError> {
    CatalogClient::connect(connection)
}

pub fn catalog_history_boundary(client: &CatalogClient) -> i64 {
    client.history_start_ms()
}

/// A configured catalog whose history boundary has not yet been discovered.
/// Built at startup without network; the loop retries discovery each tick.
///
/// Discovery is the one catalog call that cannot be deferred to a per-event
/// pricing failure, because every later call needs the boundary. It used to
/// run at startup and a refusal exited the process. On a daemon restart every
/// module boots at once, fusiform is not yet registered, and three refusals
/// inside half a second spent the crash budget: the module was `failed` for
/// a peer that was seconds from live. The dependency contract is register
/// first and fail at USE, never block boot on a peer.
pub struct PendingCatalog {
    pub(super) connection: Arc<dyn CatalogConnection>,
}

impl PendingCatalog {
    /// One discovery attempt. `Ok(true)` installed the catalog; `Ok(false)`
    /// means it did not and the caller tries again next tick; `Err` is a
    /// local (store) failure.
    ///
    /// NO REFUSAL FROM THE PEER IS FATAL HERE. This once returned Err on a
    /// non-retryable refusal, which killed the refresher thread at startup
    /// and spent the crash budget in seconds -- against a healthy producer
    /// one protocol version behind, whose boundary refusal carried a code the
    /// classifier did not expect. A catalog that cannot be discovered costs
    /// pricing; it must not cost ingest or capacity, which do not depend on
    /// it. The distinction the kind still buys is the log line: a retryable
    /// refusal is the peer warming, anything else is a contract question and
    /// is named as one on every tick until someone reads it.
    pub fn try_connect(&self, state: &SharedState, stop: &AtomicBool) -> Result<bool, String> {
        match CatalogClient::connect_until_stopped(Arc::clone(&self.connection), stop) {
            Ok(Some(catalog)) => {
                if stop.load(Ordering::Relaxed) {
                    return Ok(false);
                }
                let boundary = catalog_history_boundary(&catalog);
                state.install_catalog(Some(catalog))?;
                tracing::info!(target: "pricing", boundary, "fusiform history boundary discovered");
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(error) if error.is_retryable() => {
                tracing::warn!(target: "pricing", ?error, "catalog not yet reachable, pricing deferred");
                Ok(false)
            }
            Err(error) => {
                tracing::error!(target: "pricing", ?error,
                    "catalog discovery refused (not a warming peer; check the producer's protocol version), pricing deferred"
                );
                Ok(false)
            }
        }
    }
}

/// The pending catalog (if discovery has not yet succeeded) and the plan-price
/// source, both on one fusiform client so they share its connection.
pub(super) type ConfiguredFusiform = (Option<PendingCatalog>, Option<Arc<dyn PlanPriceSource>>);

pub(super) fn configure_catalog(
    state: &SharedState,
    config: &config::AstrocyteConfig,
    stop: &AtomicBool,
) -> Result<ConfiguredFusiform, String> {
    let Some(path) = &config.fusiform_connection_file else {
        state.install_catalog(None)?;
        return Ok((None, None));
    };
    // Constructing the transport reads no socket; only discovery does.
    let client = Arc::new(crate::fusiform_client::FusiformClient::new(path)?);
    let plan_prices: Arc<dyn PlanPriceSource> = client.clone();
    let connection: Arc<dyn CatalogConnection> = client;
    let pending = PendingCatalog { connection };
    // First attempt now so a healthy fleet prices from the first tick; a
    // refusal here is the retry case, not a startup failure.
    if pending.try_connect(state, stop)? {
        return Ok((None, Some(plan_prices)));
    }
    Ok((Some(pending), Some(plan_prices)))
}

pub(super) fn catalog_wake_tick(
    state: &SharedState,
    pending: &mut Option<PendingCatalog>,
    multiplier: &mut MultiplierStage,
    stop: &AtomicBool,
    full_tick: bool,
) {
    if stop.load(Ordering::Relaxed) {
        return;
    }
    let Some(ticket) = state.event_wakes().begin(
        crate::event_wakes::Source::Catalog,
        full_tick,
        current_time_ms(),
    ) else {
        return;
    };
    if let Some(discovery) = pending.as_ref() {
        // Discovery reads the history boundary; rate-era refresh below reads
        // the version. Keep both authoritative reads in this one coalesced
        // cycle when discovery succeeds, as on the original full-tick path.
        match discovery.try_connect(state, stop) {
            Ok(true) => *pending = None,
            Ok(false) => {
                ticket.finish(false);
                return;
            }
            Err(error) => {
                tracing::error!(target: "pricing", %error, "install catalog failed");
                ticket.finish(false);
                return;
            }
        }
    }
    if state.catalog_client().is_none() || stop.load(Ordering::Relaxed) {
        ticket.finish(false);
        return;
    }
    match rate_era_tick(state, &mut multiplier.rate_backoff) {
        Ok(pending) => {
            multiplier.rates_pending = pending;
            ticket.finish(true);
        }
        Err(error) => {
            tracing::warn!(target: "valuation", %error, "rate timeline refresh deferred");
            ticket.finish(false);
        }
    }
}
