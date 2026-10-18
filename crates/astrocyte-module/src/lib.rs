#![forbid(unsafe_code)]

//! Shared metering integrations for the daemon and management consumers.
//! Keeping these modules here gives their unit tests one owning test target.

mod capacity_map;
pub mod config;
mod fusiform_client;
mod harness_paths;
pub mod ops;
#[doc(hidden)]
pub mod peer_exchange;
mod qta_client;
mod quota_bus_client;
mod quota_events;
mod refresher;
pub mod state;
mod wire_map;

pub use refresher::{hydrate_startup, start_refresher};

#[cfg(test)]
mod route_death_peer;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
