#![deny(dead_code, unused_imports)]
//! Astrocyte v2 core identity and storage foundation.
//!
//! Settlement, pricing, statements, subscriptions, reconciliation, reads, and
//! serving are built on top of these producer-neutral records.

pub mod cache_diagnostics;
pub mod canon;
pub mod capacity;
pub mod catalog_client;
pub mod fact;
pub mod ingest;
pub mod money;
pub mod multiplier;
pub mod pricing;
pub mod reconcile;
pub mod report;
pub mod statement;
pub mod statement_import;
pub mod step_feed;
pub mod store;
pub mod subscription;
pub mod verdict;

#[cfg(test)]
mod test_support;

pub use fact::{event_id, Measurement, UsageEvent};
