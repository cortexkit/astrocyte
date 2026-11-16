//! Durable events schedule authoritative reads; their payloads are never evidence.

use std::{collections::BTreeMap, sync::Mutex, time::Duration};

use async_nats::jetstream::{self, consumer::PullConsumer};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_nats::NatsConnection;
use futures_util::{stream::FuturesUnordered, StreamExt};
use tokio::sync::oneshot;

use crate::state::SharedState;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Source {
    Media,
    Catalog,
    Attribution(&'static str),
}

impl Source {
    fn backstop(self) -> i64 {
        match self {
            Self::Media | Self::Catalog => 600_000,
            Self::Attribution(_) => 3_600_000,
        }
    }
}

#[derive(Default)]
struct Lane {
    attached: bool,
    last_started: Option<i64>,
    waiting: Vec<oneshot::Sender<bool>>,
}

#[derive(Default)]
pub(crate) struct Control(Mutex<BTreeMap<Source, Lane>>);

pub(crate) struct Ticket(Vec<oneshot::Sender<bool>>);

impl Ticket {
    pub(crate) fn has_notice(&self) -> bool {
        !self.0.is_empty()
    }

    pub(crate) fn finish(self, committed: bool) {
        for waiting in self.0 {
            let _ = waiting.send(committed);
        }
    }
}

impl Control {
    pub(crate) fn attached(&self, sources: &[Source], attached: bool) {
        let mut lanes = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for source in sources {
            let lane = lanes.entry(*source).or_default();
            lane.attached = attached;
            if !attached {
                lane.waiting.clear();
            }
        }
    }

    pub(crate) fn wake(&self, source: Source) -> oneshot::Receiver<bool> {
        let (send, receive) = oneshot::channel();
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(source)
            .or_default()
            .waiting
            .push(send);
        receive
    }

    /// Detach the waiting batch before opening a route. A message arriving during
    /// that read must wait for another read, even if the first one commits.
    pub(crate) fn begin(&self, source: Source, full_tick: bool, now: i64) -> Option<Ticket> {
        let mut lanes = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let lane = lanes.entry(source).or_default();
        let due = full_tick
            && (!lane.attached
                || lane
                    .last_started
                    .is_none_or(|last| now.saturating_sub(last) >= source.backstop()));
        if lane.waiting.is_empty() && !due {
            return None;
        }
        lane.last_started = Some(now);
        Some(Ticket(std::mem::take(&mut lane.waiting)))
    }
}

const PUBLISHERS: [astrocyte_core::attribution::Publisher; 4] =
    astrocyte_core::attribution::Publisher::ALL;

pub(crate) async fn follow(state: &SharedState, connection: &NatsConnection, names: &AccountNames) {
    let media = [Source::Media];
    let catalog = [Source::Catalog];
    let attribution = PUBLISHERS.map(|publisher| Source::Attribution(publisher.module()));
    tokio::join!(
        follow_one(state, connection, names, "m_astrocyte_media_usage", &media),
        follow_one(state, connection, names, "m_astrocyte_catalog", &catalog),
        follow_one(
            state,
            connection,
            names,
            "m_astrocyte_attribution",
            &attribution
        ),
    );
}

struct Attachment<'a> {
    control: &'a Control,
    sources: &'a [Source],
}
impl Drop for Attachment<'_> {
    fn drop(&mut self) {
        self.control.attached(self.sources, false);
    }
}

async fn follow_one(
    state: &SharedState,
    connection: &NatsConnection,
    names: &AccountNames,
    durable: &str,
    sources: &[Source],
) {
    let control = state.event_wakes();
    loop {
        let bound: Result<PullConsumer, _> = jetstream::new(connection.client())
            .get_consumer_from_stream(durable, &names.streams().event)
            .await;
        if let Ok(consumer) = bound {
            let _attachment = Attachment { control, sources };
            let _ = pull_session(control, &consumer, names, sources).await;
        }
        // Each durable retries independently; a refusal cannot tear down the
        // shared connection or switch another reader back to full-tick polling.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn subject_source(names: &AccountNames, subject: &str, sources: &[Source]) -> Option<Source> {
    sources.iter().copied().find(|source| {
        let (publisher, event) = match source {
            Source::Media => ("colliculus", "usage_recorded"),
            Source::Catalog => ("fusiform", "catalog_changed"),
            Source::Attribution(publisher) => (
                *publisher,
                if *publisher == "entorhinal" {
                    "placement_fact"
                } else {
                    "attribution_fact"
                },
            ),
        };
        names
            .event_subject(publisher, event, 1)
            .is_ok_and(|expected| expected == subject)
    })
}

async fn pull_session(
    control: &Control,
    consumer: &PullConsumer,
    names: &AccountNames,
    sources: &[Source],
) -> Result<(), ()> {
    let mut messages = consumer.messages().await.map_err(|_| ())?;
    control.attached(sources, true);
    let mut acknowledgements = FuturesUnordered::new();
    loop {
        tokio::select! {
            // Keep pulling while reads are pending. A burst crossing the pull
            // client's batch boundary still shares one coalesced read, rather
            // than forcing a read for every transport batch. Bound retained
            // acknowledgement futures if an excluded source never drains.
            message = messages.next(), if acknowledgements.len() < 4096 => {
                let message = message.ok_or(())?.map_err(|_| ())?;
                if let Some(source) = subject_source(names, message.subject.as_str(), sources) {
                    let committed = control.wake(source);
                    acknowledgements.push(async move {
                        if committed.await.unwrap_or(false) {
                            message.double_ack().await.map_err(|_| ())?;
                        }
                        Ok::<(), ()>(())
                    });
                } else {
                    // A subject this reader doesn't know (a newer event version,
                    // say) wakes nothing. Acknowledge it, or it would be
                    // redelivered forever and count against the durable's
                    // unacknowledged limit.
                    message.ack().await.map_err(|_| ())?;
                }
            }
            Some(result) = acknowledgements.next(), if !acknowledgements.is_empty() => result?,
        }
    }
}

#[cfg(test)]
#[path = "event_wakes_tests.rs"]
mod tests;
