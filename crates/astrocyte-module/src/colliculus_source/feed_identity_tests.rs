use super::*;

struct FeedFixture {
    id: Option<&'static str>,
    entries: Vec<Value>,
    fail_ack_once: bool,
}

impl FeedFixture {
    fn new(id: Option<&'static str>, entries: Vec<Value>) -> Self {
        Self {
            id,
            entries,
            fail_ack_once: false,
        }
    }

    fn serve(self) -> (Peer, Arc<Mutex<Self>>) {
        let feed = Arc::new(Mutex::new(self));
        let producer = feed.clone();
        let peer = Peer::start(Arc::new(move |request| {
            if request["op"] == "route.open" {
                return opened();
            }
            let mut producer = producer.lock().unwrap();
            match request["method"].as_str().unwrap() {
                "usage.read" => {
                    let after = request["params"]["after_seq"].as_u64().unwrap();
                    let mut answer = page(
                        producer
                            .entries
                            .iter()
                            .filter(|entry| entry["seq"].as_u64().unwrap() > after)
                            .cloned()
                            .collect(),
                    );
                    if let Some(id) = producer.id {
                        let head = producer
                            .entries
                            .last()
                            .map_or(0, |entry| entry["seq"].as_u64().unwrap());
                        answer["feed"] = json!({"id":id,"head_seq":head});
                    }
                    result(answer)
                }
                "usage.ack" => {
                    if std::mem::take(&mut producer.fail_ack_once) {
                        return (FrameType::Error, json!({"code":"temporary_vendor_error"}));
                    }
                    result(json!({"acked_through_seq":request["params"]["through_seq"]}))
                }
                other => panic!("unexpected method {other}"),
            }
        }));
        (peer, feed)
    }
}

fn named_entry(seq: u64, id: &str) -> Value {
    let mut entry = vendor_entry(seq);
    entry["record"]["record_id"] = json!(id);
    entry
}

fn named_entries(ids: &[&str]) -> Vec<Value> {
    (1..)
        .zip(ids)
        .map(|(seq, id)| named_entry(seq, id))
        .collect()
}

fn rejected_entry(seq: u64, id: &str) -> Value {
    let mut entry = named_entry(seq, id);
    entry["record"]["measurements"][0]["quantity"] = json!("unbillable");
    entry
}

fn feed_cursor(state: &SharedState) -> Option<(String, Option<String>)> {
    state
        .with_conn(|conn| {
            conn.query_row(
                "SELECT through_seq,feed_id FROM media_feed_cursor",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| error.to_string())
        })
        .unwrap()
}

fn assert_feed_cursor(state: &SharedState, sequence: u64, id: Option<&str>) {
    assert_eq!(
        feed_cursor(state),
        Some((sequence.to_string(), id.map(str::to_owned)))
    );
}

fn feed_calls(peer: &Peer) -> Vec<(String, u64)> {
    peer.requests
        .lock()
        .unwrap()
        .iter()
        .filter_map(|request| {
            let method = request["method"].as_str()?;
            let field = match method {
                "usage.read" => "after_seq",
                "usage.ack" => "through_seq",
                _ => return None,
            };
            Some((method.into(), request["params"][field].as_u64().unwrap()))
        })
        .collect()
}

fn assert_feed_calls(peer: &Peer, expected: &[(&str, u64)]) {
    let calls = feed_calls(peer);
    assert_eq!(
        calls
            .iter()
            .map(|(method, seq)| (method.as_str(), *seq))
            .collect::<Vec<_>>(),
        expected
    );
}

#[test]
fn media_feed_identity_resets_after_renumbering() {
    // The published media conformance kit's reader scenarios cover replacement
    // at the old head and a lower head without a change of id.
    for (id, entries, new_count, through) in [
        ("replacement", &["shared", "new", "newer"][..], 2, 3),
        ("original", &["shared", "new"][..], 1, 2),
    ] {
        let state = test_state();
        let (peer, feed) =
            FeedFixture::new(Some("original"), named_entries(&["shared", "old", "older"])).serve();
        let source = peer.source();
        tick(&state, &source);
        assert_feed_cursor(&state, 3, Some("original"));
        *feed.lock().unwrap() = FeedFixture::new(Some(id), named_entries(entries));
        tick(&state, &source);
        assert_feed_cursor(&state, through, Some(id));
        assert_feed_calls(
            &peer,
            &[
                ("usage.read", 0),
                ("usage.ack", 3),
                ("usage.read", 3),
                ("usage.read", 3),
                ("usage.read", 0),
                ("usage.ack", through),
                ("usage.read", through),
            ],
        );
        assert_eq!(count(&state, "usage_event"), 3 + new_count);
        assert_eq!(count(&state, "rejected_delivery"), 0);
    }
}

#[test]
fn media_feed_identity_retains_one_evidence_row_per_redelivered_record() {
    let state = test_state();
    let raw = json!({"raw":{"bytes_b64":"/w==","reason":"undecodable"}});
    let mut first_raw = raw.clone();
    first_raw["seq"] = json!(3);
    let (peer, feed) = FeedFixture::new(
        Some("original"),
        vec![
            named_entry(1, "shared"),
            rejected_entry(2, "bad"),
            first_raw,
        ],
    )
    .serve();
    let source = peer.source();
    tick(&state, &source);
    assert_eq!(count(&state, "usage_event"), 1);
    assert_eq!(count(&state, "set_aside_record"), 2);

    let mut replayed_raw = raw;
    replayed_raw["seq"] = json!(1);
    let mut replayed_bad = rejected_entry(4, "bad");
    replayed_bad["record"]["measurements"][0]["quantity"] = json!("still unbillable");
    *feed.lock().unwrap() = FeedFixture::new(
        Some("replacement"),
        vec![replayed_raw.clone(), named_entry(2, "shared"), replayed_bad],
    );
    tick(&state, &source);
    assert_feed_cursor(&state, 4, Some("replacement"));
    assert_eq!(count(&state, "usage_event"), 1);
    assert_eq!(count(&state, "rejected_delivery"), 0);
    assert_eq!(count(&state, "set_aside_record"), 2);

    *feed.lock().unwrap() = FeedFixture::new(
        Some("another-replacement"),
        vec![
            replayed_raw,
            rejected_entry(2, "different-bad"),
            rejected_entry(3, "bad"),
            named_entry(4, "shared"),
        ],
    );
    tick(&state, &source);
    assert_feed_cursor(&state, 4, Some("another-replacement"));
    assert_eq!(count(&state, "usage_event"), 1);
    assert_eq!(count(&state, "rejected_delivery"), 0);
    assert_eq!(count(&state, "set_aside_record"), 3);
    assert_feed_calls(
        &peer,
        &[
            ("usage.read", 0),
            ("usage.ack", 3),
            ("usage.read", 3),
            ("usage.read", 3),
            ("usage.read", 0),
            ("usage.ack", 4),
            ("usage.read", 4),
            ("usage.read", 4),
            ("usage.read", 0),
            ("usage.ack", 4),
            ("usage.read", 4),
        ],
    );
}

#[test]
fn media_feed_identity_commits_id_with_cursor_or_neither() {
    let state = test_state();
    state.with_conn(|conn| {
        conn.execute_batch(
            "CREATE TABLE fail_media_commit (event_key INTEGER REFERENCES usage_event(event_key) DEFERRABLE INITIALLY DEFERRED);
             CREATE TRIGGER fail_cursor_commit AFTER INSERT ON media_feed_cursor
             BEGIN INSERT INTO fail_media_commit VALUES(-1); END;",
        )
        .map_err(|error| error.to_string())
    }).unwrap();
    let (peer, _) = FeedFixture::new(
        Some("original"),
        vec![named_entry(1, "valid"), rejected_entry(2, "bad")],
    )
    .serve();
    let source = peer.source();
    tick(&state, &source);
    assert_eq!(feed_cursor(&state), None);
    assert_eq!(count(&state, "usage_event"), 0);
    assert_eq!(count(&state, "set_aside_record"), 0);
    assert!(state
        .producer_degraded_reason(SOURCE_KEY)
        .unwrap()
        .contains("FOREIGN KEY"));
    assert_feed_calls(&peer, &[("usage.read", 0)]);

    state
        .with_conn(|conn| {
            conn.execute_batch("DROP TRIGGER fail_cursor_commit; DROP TABLE fail_media_commit;")
                .map_err(|error| error.to_string())
        })
        .unwrap();
    tick(&state, &source);
    assert_feed_cursor(&state, 2, Some("original"));
    assert_eq!(count(&state, "usage_event"), 1);
    assert_eq!(count(&state, "set_aside_record"), 1);
    assert_feed_calls(
        &peer,
        &[
            ("usage.read", 0),
            ("usage.read", 0),
            ("usage.ack", 2),
            ("usage.read", 2),
        ],
    );
}

#[test]
fn media_feed_identity_drops_pending_ack_for_a_replaced_feed() {
    let state = test_state();
    let mut original = FeedFixture::new(Some("original"), named_entries(&["shared"]));
    original.fail_ack_once = true;
    let (peer, feed) = original.serve();
    let source = peer.source();
    tick(&state, &source);
    assert_feed_cursor(&state, 1, Some("original"));
    assert!(state.producer_degraded_reason(SOURCE_KEY).is_some());

    *feed.lock().unwrap() =
        FeedFixture::new(Some("replacement"), named_entries(&["shared", "new"]));
    tick(&state, &source);
    assert_feed_cursor(&state, 2, Some("replacement"));
    assert_eq!(count(&state, "usage_event"), 2);
    assert_feed_calls(
        &peer,
        &[
            ("usage.read", 0),
            ("usage.ack", 1),
            ("usage.read", 1),
            ("usage.read", 0),
            ("usage.ack", 2),
            ("usage.read", 2),
        ],
    );
}

#[test]
fn media_feed_identity_retries_only_after_confirming_the_pending_ack_feed() {
    let state = test_state();
    let mut original = FeedFixture::new(Some("original"), named_entries(&["shared"]));
    original.fail_ack_once = true;
    let (peer, _) = original.serve();
    let source = peer.source();
    tick(&state, &source);
    assert_feed_cursor(&state, 1, Some("original"));
    assert!(state.producer_degraded_reason(SOURCE_KEY).is_some());
    tick(&state, &source);
    assert_eq!(state.producer_degraded_reason(SOURCE_KEY), None);
    assert_feed_calls(
        &peer,
        &[
            ("usage.read", 0),
            ("usage.ack", 1),
            ("usage.read", 1),
            ("usage.ack", 1),
        ],
    );
}

#[test]
fn media_feed_identity_adopts_first_id_without_reset_or_retrying_legacy_ack() {
    let state = test_state();
    let mut legacy = FeedFixture::new(None, named_entries(&["legacy"]));
    legacy.fail_ack_once = true;
    let (peer, feed) = legacy.serve();
    let source = peer.source();
    tick(&state, &source);
    assert_feed_cursor(&state, 1, None);
    assert!(state.producer_degraded_reason(SOURCE_KEY).is_some());

    feed.lock().unwrap().id = Some("original");
    tick(&state, &source);
    assert_feed_cursor(&state, 1, Some("original"));
    assert_eq!(state.producer_degraded_reason(SOURCE_KEY), None);
    assert_eq!(count(&state, "usage_event"), 1);
    assert_feed_calls(
        &peer,
        &[("usage.read", 0), ("usage.ack", 1), ("usage.read", 1)],
    );
}

#[test]
fn media_feed_identity_untagged_reply_keeps_sequence_behavior() {
    let state = test_state();
    let (peer, feed) =
        FeedFixture::new(Some("original"), named_entries(&["shared", "older"])).serve();
    let source = peer.source();
    tick(&state, &source);
    *feed.lock().unwrap() = FeedFixture::new(None, vec![]);
    tick(&state, &source);
    assert_feed_cursor(&state, 2, Some("original"));
    assert_eq!(count(&state, "usage_event"), 2);
    assert_feed_calls(
        &peer,
        &[
            ("usage.read", 0),
            ("usage.ack", 2),
            ("usage.read", 2),
            ("usage.read", 2),
        ],
    );
}

#[test]
fn media_feed_identity_survives_reopening_the_store() {
    use crate::state::leased_test_state;

    let directory = tempfile::tempdir().unwrap();
    let (original, _) = FeedFixture::new(Some("original"), named_entries(&["old"])).serve();
    {
        let state = leased_test_state(&directory);
        tick(&state, &original.source());
        assert_feed_cursor(&state, 1, Some("original"));
    }
    let state = leased_test_state(&directory);
    let (replacement, _) = FeedFixture::new(Some("replacement"), named_entries(&["new"])).serve();
    tick(&state, &replacement.source());
    assert_feed_cursor(&state, 1, Some("replacement"));
    assert_eq!(count(&state, "usage_event"), 2);
    assert_feed_calls(
        &replacement,
        &[
            ("usage.read", 1),
            ("usage.read", 0),
            ("usage.ack", 1),
            ("usage.read", 1),
        ],
    );
}

#[test]
fn media_feed_identity_rejects_an_invalid_id_or_head_before_ingest() {
    for (id, head) in [("", 1), ("contains space", 1), ("original", 0)] {
        let state = test_state();
        let peer = Peer::start(Arc::new(move |request| {
            if request["op"] == "route.open" {
                return opened();
            }
            assert_eq!(request["method"], "usage.read");
            let mut answer = page(vec![vendor_entry(1)]);
            answer["feed"] = json!({"id":id,"head_seq":head});
            result(answer)
        }));
        tick(&state, &peer.source());
        assert_eq!(feed_cursor(&state), None);
        assert_eq!(count(&state, "usage_event"), 0);
        assert!(state
            .producer_degraded_reason(SOURCE_KEY)
            .unwrap()
            .contains("invalid media feed identity"));
        assert_feed_calls(&peer, &[("usage.read", 0)]);
    }
}
