#![forbid(unsafe_code)]

//! Shared metering integrations for the daemon and management consumers.
//! Keeping these modules here gives their unit tests one owning test target.

mod absent_backoff;
mod bus_publications;
#[cfg(test)]
mod cache_steps_vectors;
mod capacity_map;
pub mod config;
mod event_wakes;
mod fusiform_client;
mod harness_paths;
#[cfg(test)]
mod multiplier_report_vectors_tests;
pub mod ops;
#[doc(hidden)]
pub mod peer_exchange;
mod qta_client;
mod quota_bus_client;
mod quota_events;
mod quota_history;
mod recent_steps;
mod refresher;
#[cfg(test)]
mod rewrite_causes_vectors;
#[doc(hidden)]
pub mod route_reply;
mod runner_steps;
pub mod runs_feed;
#[cfg(test)]
mod runs_vectors;
pub mod state;
pub mod step_feed;
mod wire_map;

pub use refresher::{hydrate_startup, start_refresher, start_refresher_with_media};

#[cfg(test)]
mod route_death_peer;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub(crate) mod vault_source;
