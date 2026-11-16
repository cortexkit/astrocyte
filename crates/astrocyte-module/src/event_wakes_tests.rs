use super::*;
use cortexkit_bus_trait::PermissionViolationSource;

#[test]
fn event_wakes_coalesce_and_only_ack_reads_started_after_arrival() {
    for source in [Source::Media, Source::Catalog, Source::Attribution("basal")] {
        let control = Control::default();
        control.attached(&[source], true);
        control.begin(source, true, 0).unwrap().finish(true);
        let mut first = control.wake(source);
        let mut second = control.wake(source);
        let ticket = control
            .begin(source, false, 1)
            .expect("wake must start a read");
        let mut during_read = control.wake(source);
        assert!(first.try_recv().is_err());
        ticket.finish(true);
        assert_eq!(first.try_recv(), Ok(true));
        assert_eq!(second.try_recv(), Ok(true));
        assert!(
            during_read.try_recv().is_err(),
            "an older read cannot acknowledge a new arrival"
        );
        control.begin(source, false, 2).unwrap().finish(false);
        assert_eq!(during_read.try_recv(), Ok(false));
        assert!(control.begin(source, false, 3).is_none());
    }
}

#[test]
fn event_wakes_backstop_and_fallback_are_independent() {
    let control = Control::default();
    let sources = [
        Source::Media,
        Source::Catalog,
        Source::Attribution("prefrontal-core"),
        Source::Attribution("magic-context"),
        Source::Attribution("basal"),
        Source::Attribution("entorhinal"),
    ];
    control.attached(&sources, true);
    for source in sources {
        let mut reads = 0;
        for now in [0, 30_000, 60_000, 599_999, 600_000, 3_599_999, 3_600_000] {
            if let Some(ticket) = control.begin(source, true, now) {
                reads += 1;
                ticket.finish(true);
            }
        }
        assert_eq!(
            reads,
            if matches!(source, Source::Attribution(_)) {
                2
            } else {
                3
            }
        );
    }
    control.attached(&[Source::Media], false);
    for now in [3_600_001, 3_600_002] {
        assert!(control.begin(Source::Media, true, now).is_some());
        assert!(control.begin(Source::Catalog, true, now).is_none());
        assert!(control
            .begin(Source::Attribution("basal"), true, now)
            .is_none());
    }
    let control = Control::default();
    for source in sources {
        for now in 0..3 {
            assert!(
                control.begin(source, true, now).is_some(),
                "no bus must retain per-tick reads"
            );
        }
    }
}

#[tokio::test]
async fn event_wakes_real_nats_denied_catalog_preserves_other_consumers_and_redelivery() {
    use crate::quota_events::tests::NatsServer;
    let names = AccountNames::derive("wake_isolation").unwrap();
    let denied = format!(
        "$JS.API.CONSUMER.INFO.{}.m_astrocyte_catalog",
        names.streams().event
    );
    let config = format!(
        r#"accounts {{ ledger {{
        jetstream: enabled
        users: [
        {{user: admin, password: fixture}},
        {{user: reader, password: fixture, permissions: {{
            publish: {{allow: [">"], deny: ["{denied}"]}}
            subscribe: {{allow: [">"]}}
        }}}}
    ] }} }}"#
    );
    let Some(server) = NatsServer::start_with_config(&config) else {
        return;
    };
    let admin = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(client) = async_nats::ConnectOptions::new()
                .user_and_password("admin".into(), "fixture".into())
                .connect(&server.url)
                .await
            {
                break client;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let stream = jetstream::new(admin.clone())
        .create_stream(jetstream::stream::Config {
            name: names.streams().event.clone(),
            subjects: vec![format!("ck.{}.event.>", "wake_isolation")],
            ..Default::default()
        })
        .await
        .unwrap();
    for (durable, filters) in [
        (
            "m_astrocyte_media_usage",
            vec![names
                .event_subject("colliculus", "usage_recorded", 1)
                .unwrap()],
        ),
        (
            "m_astrocyte_catalog",
            vec![names
                .event_subject("fusiform", "catalog_changed", 1)
                .unwrap()],
        ),
        (
            "m_astrocyte_attribution",
            PUBLISHERS
                .iter()
                .map(|publisher| {
                    names
                        .event_subject(
                            publisher.module(),
                            if publisher.module() == "entorhinal" {
                                "placement_fact"
                            } else {
                                "attribution_fact"
                            },
                            1,
                        )
                        .unwrap()
                })
                .collect(),
        ),
    ] {
        stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some(durable.into()),
                filter_subjects: filters,
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(2),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    let connection = NatsConnection::connect(
        &server.url,
        async_nats::ConnectOptions::new().user_and_password("reader".into(), "fixture".into()),
        cortexkit_bus_nats::ConnectConfig::new("wake_isolation").unwrap(),
    )
    .await
    .unwrap();
    let state = crate::state::test_state();
    let mut violations = connection.permission_violations();
    let runner = crate::runner_steps::follow(&state, &connection, &names);
    tokio::pin!(runner);
    assert!(futures_util::poll!(runner.as_mut()).is_pending());
    let watches = follow(&state, &connection, &names);
    tokio::pin!(watches);
    let publications = state.bus_publications().follow(&connection, &names);
    tokio::pin!(publications);
    let committed_subject = names
        .live_subject("astrocyte", "steps_committed", 1)
        .unwrap();
    let mut published = admin.subscribe(committed_subject).await.unwrap();
    admin.flush().await.unwrap();
    let stage = std::cell::Cell::new("permission refusal");
    let check = async {
        let refusal = violations.next_violation().await.unwrap().unwrap();
        assert_eq!(refusal.subject, denied);
        stage.set("step publication");
        let (subject, body) = crate::runner_steps::test_event(&names, "minimal", "broca", |_| {});
        admin.publish(subject, body.into()).await.unwrap();
        state.bus_publications().steps("after-refusal".into());
        admin.flush().await.unwrap();
        assert!(published.next().await.is_some());
        stage.set("runner delivery");
        while state.step_feed().runner_counts().appended == 0 {
            tokio::task::yield_now().await;
        }
        let jetstream = jetstream::new(admin.clone());
        stage.set("wake publication");
        for source in [
            Source::Media,
            Source::Attribution("basal"),
            Source::Attribution("entorhinal"),
        ] {
            let (module, event) = match source {
                Source::Media => ("colliculus", "usage_recorded"),
                Source::Attribution(module) => (
                    module,
                    if module == "entorhinal" {
                        "placement_fact"
                    } else {
                        "attribution_fact"
                    },
                ),
                Source::Catalog => unreachable!(),
            };
            let count = if source == Source::Media { 321 } else { 3 };
            for _ in 0..count {
                jetstream
                    .publish(
                        names.event_subject(module, event, 1).unwrap(),
                        vec![0xff, 0x00].into(),
                    )
                    .await
                    .unwrap()
                    .await
                    .unwrap();
            }
        }
        let control = state.event_wakes();
        let wait = |source| async move {
            loop {
                if let Some(ticket) = control.begin(source, false, 0) {
                    break ticket;
                }
                tokio::task::yield_now().await;
            }
        };
        stage.set("basal wake");
        wait(Source::Attribution("basal")).await.finish(true);
        stage.set("entorhinal wake");
        wait(Source::Attribution("entorhinal")).await.finish(true);
        stage.set("media first wake");
        loop {
            if control
                .0
                .lock()
                .unwrap()
                .get(&Source::Media)
                .is_some_and(|lane| lane.waiting.len() >= 321)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        wait(Source::Media).await.finish(false);
        let mut media: PullConsumer = stream
            .get_consumer("m_astrocyte_media_usage")
            .await
            .unwrap();
        assert!(
            media.info().await.unwrap().num_ack_pending > 0,
            "failed read must not ack"
        );
        stage.set("media redelivery");
        while media.info().await.unwrap().num_redelivered == 0 {
            tokio::task::yield_now().await;
        }
        wait(Source::Media).await.finish(true);
        stage.set("media acknowledgement");
        let mut attribution: PullConsumer = stream
            .get_consumer("m_astrocyte_attribution")
            .await
            .unwrap();
        loop {
            for source in [
                Source::Media,
                Source::Attribution("basal"),
                Source::Attribution("entorhinal"),
            ] {
                if let Some(ticket) = control.begin(source, false, 0) {
                    ticket.finish(true);
                }
            }
            if media.info().await.unwrap().num_ack_pending == 0
                && attribution.info().await.unwrap().num_ack_pending == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(state.step_feed().runner_counts().appended, 1);
        assert_eq!(
            connection.client().connection_state(),
            async_nats::connection::State::Connected
        );
        assert!(
            !control
                .0
                .lock()
                .unwrap()
                .entry(Source::Catalog)
                .or_default()
                .attached
        );
    };
    tokio::time::timeout(Duration::from_secs(15), async {
        tokio::select! {
            () = &mut watches => panic!("wake followers ended"),
            () = &mut runner => panic!("runner follower ended"),
            () = &mut publications => panic!("publishers ended"),
            () = check => {},
        }
    })
    .await
    .unwrap_or_else(|_| panic!("isolated consumers did not finish {}", stage.get()));
}
