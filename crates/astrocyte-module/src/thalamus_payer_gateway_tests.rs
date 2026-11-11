//! Claude Code calls sent through a company's own Anthropic gateway, joined to
//! THALAMUS's payer rows for them.
//!
//! The payer store is the stand-in set under
//! `tests/vectors/gateway-payer-standin/`: rows THALAMUS's own code wrote for a
//! short Claude Code session whose requests went out through a stand-in
//! company gateway. Claude Code's transcript for that session is not part of
//! the set, so each test writes transcript entries for the same calls, with
//! the counts the stand-in gateway answered with.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use astrocyte_core::catalog_client::{CatalogCallError, CatalogClient, CatalogConnection};
use fusiform_protocol::{CatalogGetRequest, CatalogGetResponse, StatusRequest, StatusResponse};
use rusqlite::Connection;
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use super::super::claude_thalamus_source::ClaudeThalamusSource;
use super::*;
use crate::state::test_state;

/// Claude Code's own session id, as the stand-in rows name it.
const SESSION: &str = "c8fc54c0-929c-46ed-a78a-d56360936a61";
const MODEL: &str = "claude-haiku-5-5";
const AUTH: &str = "oauth:anthropic:two";
const ACCOUNT: &str = "acct-2";
/// The `failed_before_row` row: the request the gateway answered with 401.
const FAILED_ATTEMPT: &str = "94f56122-c915-4ef4-a6d0-bcf112658d72";
/// The third `main` row, which the message-id test withholds and adds late.
const THIRD_ATTEMPT: &str = "dd748f77-6bb6-4b0a-845f-266d1f4d375b";
/// When the tests read the stores: ten seconds after the session's last row.
const AT: i64 = 1_794_417_460_000;

/// The three answered calls: request id, message id, stop reason and the
/// transcript timestamp of the reply.
const CALLS: [(&str, &str, &str, &str); 3] = [
    (
        "req_standin_gw_1",
        "msg_standin_gw_1",
        "end_turn",
        "2026-11-11T17:17:30.420Z",
    ),
    (
        "req_standin_gw_2",
        "msg_standin_gw_2",
        "tool_use",
        "2026-11-11T17:17:30.680Z",
    ),
    (
        "req_standin_gw_3",
        "msg_standin_gw_3",
        "end_turn",
        "2026-11-11T17:17:30.727Z",
    ),
];

/// One call's charge at [`AnthropicCatalog`]'s rates for the counts the
/// stand-in gateway reported (1,200 input, 42 output, 900 cache-read and 100
/// five-minute cache-write tokens), in nanodollars:
/// 1,200 x 1,000 + 42 x 5,000 + 900 x 100 + 100 x 1,250.
const CALL_CHARGE_NANOS: i64 = 1_625_000;

/// Anthropic's published Claude Haiku 4.5 rates, per million tokens: $1 input,
/// $5 output, $0.10 cache read, $1.25 five-minute and $2 one-hour cache write.
/// The stand-in model name is not a real model, so the rates are stated here.
struct AnthropicCatalog;

impl CatalogConnection for AnthropicCatalog {
    fn catalog_status(&self, _: StatusRequest) -> Result<StatusResponse, CatalogCallError> {
        Ok(
            serde_json::from_value(json!({"source":"fixture","catalog_version":1,
            "model_count":1,"era_count":1,"recent_polls":[]}))
            .unwrap(),
        )
    }

    fn catalog_get(
        &self,
        request: CatalogGetRequest,
    ) -> Result<CatalogGetResponse, CatalogCallError> {
        if request.at_ms == Some(0) {
            return Err(CatalogCallError {
                code: "no_coverage".into(),
                message: "fusiform's history begins at 1".into(),
            });
        }
        let rate = |units: i64| {
            json!({"state":"priced","units":units,"exponent":9,
            "currency":"USD","unit_provenance":{"kind":"stated"}})
        };
        Ok(serde_json::from_value(json!({"source":"fixture",
            "resolved_at_ms":request.at_ms,"catalog_version":1,
            "models":{format!("anthropic/{MODEL}"):{
                "rate.input":rate(1_000_000_000),
                "rate.output":rate(5_000_000_000),
                "rate.cache_read":rate(100_000_000),
                "rate.cache_write.ttl.5m":rate(1_250_000_000),
                "rate.cache_write.ttl.1h":rate(2_000_000_000)}},
            "withheld":[],"uncertain":[],"overridden":[]}))
        .unwrap())
    }
}

fn vectors() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vectors/gateway-payer-standin")
}

/// A data home holding a copy of the stand-in payer store and an empty
/// THALAMUS Claude Code session folder, with the two sources reading it.
struct Gateway {
    _temp: TempDir,
    folder: PathBuf,
    payer: ThalamusPayerSource,
    sessions: ClaudeThalamusSource,
    state: SharedState,
}

impl Gateway {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let home = fs::canonicalize(temp.path()).unwrap();
        let payer = ThalamusPayerSource::from_data_home(home.clone());
        fs::create_dir_all(payer.path.parent().unwrap()).unwrap();
        fs::copy(vectors().join("payer.db"), &payer.path).unwrap();
        let folder = home.join("cortexkit/thalamus-harness/claude-code");
        fs::create_dir_all(&folder).unwrap();
        let state = test_state();
        state
            .install_catalog(Some(
                CatalogClient::connect(Arc::new(AnthropicCatalog)).unwrap(),
            ))
            .unwrap();
        Self {
            _temp: temp,
            folder,
            payer,
            sessions: ClaudeThalamusSource::from_data_home(home),
            state,
        }
    }

    /// Opens the copied payer store to change it the way THALAMUS would.
    fn payer_store(&self) -> Connection {
        Connection::open(&self.payer.path).unwrap()
    }

    /// Writes Claude Code's transcript of the three calls and declares the
    /// session to the Claude Code THALAMUS source. With `request_ids` false,
    /// every entry lacks `requestId`, as when a gateway dropped the header.
    fn transcript(&self, request_ids: bool) {
        let config = self.folder.join("sessions").join(SESSION).join("config");
        let projects = config.join("projects/-work-gateway");
        fs::create_dir_all(&projects).unwrap();
        let mut lines = String::new();
        for (request, message, stop, timestamp) in CALLS {
            let mut entry = json!({"type":"assistant","timestamp":timestamp,
                "sessionId":SESSION,"cwd":"/work/gateway",
                "message":{"id":message,"model":MODEL,"stop_reason":stop,
                    "usage":{"input_tokens":1200,"output_tokens":42,
                        "cache_read_input_tokens":900,"cache_creation_input_tokens":100,
                        "cache_creation":{"ephemeral_5m_input_tokens":100,
                            "ephemeral_1h_input_tokens":0}}}});
            if request_ids {
                entry["requestId"] = json!(request);
            }
            lines.push_str(&format!("{entry}\n"));
        }
        fs::write(projects.join(format!("{SESSION}.jsonl")), lines).unwrap();
        fs::write(
            self.folder.join("roots.json"),
            json!({"version":1,"roots":[{"session":SESSION,"session_ref":"runner/head",
                "path":config,"owner_principal":"prefrontal-core","state":"done",
                "done_at_ms":AT}]})
            .to_string(),
        )
        .unwrap();
    }

    fn drain_payer(&self, at: i64) {
        let problem = self
            .payer
            .drain_for_health(&self.state, at, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(problem, None);
    }

    fn drain_sessions(&self, at: i64) {
        let problem = self
            .sessions
            .drain_for_health(&self.state, at, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(problem, None);
    }

    fn price(&self, at: i64) {
        super::super::price_unpriced_events(&self.state, at, &AtomicBool::new(false)).unwrap();
    }

    /// `(producer_ref, charge_basis, auth_selection, account_ref,
    /// vendor_request_id)` of every Claude Code transcript call.
    fn transcript_calls(&self) -> Vec<CallRow> {
        self.state.with_conn(|conn| {
            Ok(conn.prepare("SELECT producer_ref,charge_basis,auth_selection,account_ref,vendor_request_id FROM usage_event WHERE producer='claude-code' ORDER BY producer_ref").unwrap()
                .query_map([], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))).unwrap()
                .collect::<rusqlite::Result<Vec<_>>>().unwrap())
        }).unwrap()
    }

    /// `(producer_ref, charge rows, priced rows, total charge)` of every event
    /// of `producer` that has a charge row.
    fn charges(&self, producer: &str) -> Vec<(String, i64, i64, i64)> {
        self.state.with_conn(|conn| {
            Ok(conn.prepare("SELECT e.producer_ref,COUNT(*),SUM(c.outcome='priced'),COALESCE(SUM(c.charge_nanos),0) FROM usage_event e JOIN priced_charge c USING(event_key) WHERE e.producer=?1 GROUP BY e.event_key ORDER BY e.producer_ref").unwrap()
                .query_map([producer], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).unwrap()
                .collect::<rusqlite::Result<Vec<_>>>().unwrap())
        }).unwrap()
    }

    fn coverage(&self, kind: &str) -> Vec<String> {
        self.state.with_conn(|conn| {
            Ok(conn.prepare("SELECT identity FROM thalamus_payer_coverage WHERE kind=?1 ORDER BY identity").unwrap()
                .query_map([kind], |row| row.get(0)).unwrap()
                .collect::<rusqlite::Result<Vec<_>>>().unwrap())
        }).unwrap()
    }
}

type CallRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn joined(reference: &str, request: Option<&str>) -> CallRow {
    (
        reference.into(),
        "metered_api".into(),
        Some(AUTH.into()),
        Some(ACCOUNT.into()),
        request.map(Into::into),
    )
}

fn priced(reference: &str) -> (String, i64, i64, i64) {
    // Five charge rows: input, output, cache read and the two cache-write
    // lifetimes (the one-hour count is zero, so its charge is zero).
    (reference.into(), 5, 5, CALL_CHARGE_NANOS)
}

#[test]
fn gateway_standin_vectors_are_the_pinned_stand_in_bytes() {
    let sums = fs::read_to_string(vectors().join("SHA256SUMS")).unwrap();
    let mut checked = 0;
    for line in sums.lines() {
        let (expected, name) = line.split_once("  ").unwrap();
        let actual = format!(
            "{:x}",
            Sha256::digest(fs::read(vectors().join(name)).unwrap())
        );
        assert_eq!(actual, expected, "{name}");
        checked += 1;
    }
    assert_eq!(checked, 3);
    let readme = fs::read_to_string(vectors().join("README.md")).unwrap();
    assert!(
        readme.lines().next().unwrap().starts_with("STAND-IN:"),
        "the README's first line must say these rows are stand-ins"
    );
}

#[test]
fn gateway_standin_main_rows_join_their_calls_and_the_failed_row_has_unknown_usage() {
    let gateway = Gateway::new();
    gateway.drain_payer(AT);
    gateway.transcript(true);
    gateway.drain_sessions(AT);
    gateway.price(AT + 1);

    assert_eq!(
        gateway.transcript_calls(),
        CALLS
            .map(|(request, ..)| joined(request, Some(request)))
            .to_vec()
    );
    assert_eq!(
        gateway.charges("claude-code"),
        CALLS.map(|(request, ..)| priced(request)).to_vec()
    );

    // The 401: a row-less call recorded from the payer row alone. Its usage
    // is unknown, so it carries no measurement and no charge row, never a
    // charge of zero.
    gateway
        .state
        .with_conn(|conn| {
            let row = conn
                .query_row(
                    "SELECT producer,producer_lineage,terminal_status,vendor_request_id,provider,model,charge_basis,auth_selection,account_ref,work_class,
                            (SELECT COUNT(*) FROM measurement m WHERE m.event_key=e.event_key)
                     FROM usage_event e WHERE producer_ref=?1",
                    [FAILED_ATTEMPT],
                    |row| {
                        Ok((
                            (
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, String>(4)?,
                                row.get::<_, String>(5)?,
                            ),
                            (
                                row.get::<_, String>(6)?,
                                row.get::<_, String>(7)?,
                                row.get::<_, String>(8)?,
                                row.get::<_, String>(9)?,
                                row.get::<_, i64>(10)?,
                            ),
                        ))
                    },
                )
                .unwrap();
            assert_eq!(
                row,
                (
                    (
                        "thalamus-harness".into(),
                        SESSION.into(),
                        "failed".into(),
                        "req_standin_gw_4".into(),
                        "anthropic".into(),
                        MODEL.into(),
                    ),
                    (
                        "metered_api".into(),
                        AUTH.into(),
                        ACCOUNT.into(),
                        "prefrontal-core".into(),
                        0,
                    ),
                )
            );
            assert_eq!(
                conn.query_row("SELECT COUNT(*) FROM usage_event", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                4
            );
            Ok(())
        })
        .unwrap();
    assert!(gateway.charges("thalamus-harness").is_empty());
    assert!(gateway.coverage("without_payer").is_empty());
}

#[test]
fn gateway_without_request_id_keys_the_call_by_message_id_and_joins_it() {
    let gateway = Gateway::new();
    // What THALAMUS writes when the gateway passed no `request-id`: no
    // provider request id, and the message id as the main row's step_ref.
    // The third call's row is held back so it arrives after its call.
    let store = gateway.payer_store();
    store
        .execute_batch(
            "UPDATE payer_record SET step_ref=provider_response_id, provider_request_id=NULL
               WHERE request_kind='main';
             DELETE FROM payer_record WHERE attempt_id='dd748f77-6bb6-4b0a-845f-266d1f4d375b';",
        )
        .unwrap();
    drop(store);
    gateway.drain_payer(AT);
    gateway.transcript(false);
    gateway.drain_sessions(AT);

    let [(_, first, ..), (_, second, ..), (_, third, ..)] = CALLS;
    assert_eq!(
        gateway.transcript_calls(),
        vec![joined(first, None), joined(second, None)]
    );
    // With no payer row the third call waits, then is recorded without one.
    gateway.drain_sessions(AT + PAYER_WAIT);
    let mut expected = vec![joined(first, None), joined(second, None)];
    expected.push((third.into(), "unknown".into(), None, None, None));
    assert_eq!(gateway.transcript_calls(), expected);
    assert_eq!(gateway.coverage("without_payer").len(), 1);

    // Its payer row then arrives. The late check derives the transcript
    // call's identity from the message id and finds the recorded call.
    gateway.payer_store().execute(
        "INSERT INTO payer_record(attempt_id,harness,session,request_kind,step_ref,recorded_at_ms,provider,model,charge_basis,auth_selection,account_ref,outcome,provider_request_id,provider_response_id,usage_json)
         VALUES (?1,'claude-code',?2,'main',?3,1794417450727,'anthropic',?4,'metered_api',?5,?6,'completed',NULL,?3,NULL)",
        rusqlite::params![THIRD_ATTEMPT, SESSION, third, MODEL, AUTH, ACCOUNT],
    )
    .unwrap();
    gateway.drain_payer(AT + PAYER_WAIT + 1);
    assert_eq!(gateway.coverage("late"), [THIRD_ATTEMPT]);
    assert_eq!(gateway.transcript_calls(), expected);

    gateway.price(AT + PAYER_WAIT + 2);
    // The third call was recorded before its payer row said who paid, so it
    // keeps an unknown charge basis and none of its charge rows is priced.
    assert_eq!(
        gateway.charges("claude-code"),
        vec![priced(first), priced(second), (third.into(), 5, 0, 0)]
    );
}

/// How long a THALAMUS call waits for its payer row.
const PAYER_WAIT: i64 = super::super::thalamus_payer_join::PAYER_WAIT_MS;
