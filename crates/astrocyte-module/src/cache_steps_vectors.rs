//! Published `usage.cache.steps` reply vectors, regenerated from the real
//! handler and compared byte for byte with the files the phone app decodes.
//!
//! Every reply and refusal is answered by `crate::ops::dispatch`, the
//! operation's real dispatch, over a store this module builds from invented
//! rows: no real session, model or count is published. A reply keeps its bytes
//! while its members, their names and their omission rules stay put, so
//! changing any of them fails here, and the published files move only on
//! purpose. One published file is a request rather than an answer: it is
//! serialised from its params, because it pins the request member that no
//! reply's request shows.

use serde_json::json;
use sha2::{Digest, Sha256};

use crate::state::SharedState;

#[path = "cache_steps_vectors/store.rs"]
mod store;

use store::{CODEX, DAY_MS, OMP};

const OP: &str = crate::ops::OP_CACHE_STEPS;

/// The published directory, by absolute path.
const DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/test-vectors/usage-cache-steps-v1"
);

/// The whole frame body the serving handler sends for the request `params`:
/// for a reply, the `{"result": result}` the route builds; for a refused
/// request, the `{"error": {"code": code, "message": message}}` frame the daemon
/// sends instead (the route reply and the refusal are built in `main.rs`).
///
/// The params go through the operation's real dispatch, including request
/// validation and the choice between the recent-steps memory and the store.
fn served(state: &SharedState, params: serde_json::Value) -> Vec<u8> {
    match crate::ops::dispatch(state, OP, params) {
        Ok(result) => serde_json::to_vec(&json!({"result": result})).unwrap(),
        Err((code, message)) => {
            serde_json::to_vec(&json!({"error": {"code": code, "message": message}})).unwrap()
        }
    }
}

/// The session whose three steps all fit the default limit of 200.
const STREAMING: &str = "ses_ck_phone_stream";
/// The session whose two steps have no time.
const UNDATED: &str = "ses_ck_phone_undated";
/// The OMP session whose steps carry every member an OMP row can have.
const EVERY_MEMBER: &str = "ses_ck_omp_diag";
/// The Codex session: the steps that carry a context window.
const CONTEXT_WINDOW: &str = "ses_ck_codex_window";
/// The session whose only dated step carries nothing beyond the required.
const REQUIRED_ONLY: &str = "ses_ck_required_only";

/// A request frame published without a reply of its own.
const AFTER_CURSOR: &str = "request-after-cursor.json";

/// The request member the published `after_cursor` frame carries.
const AFTER_CURSOR_MEMBER: &str = "after_cursor";
/// One harness step with no member set, which a case extends.
fn harness_step(
    harness: &'static str,
    session: &'static str,
    step_ref: &'static str,
) -> store::Step<'static> {
    store::Step {
        harness,
        session,
        step_ref,
        at: None,
        input: None,
        cache_read: None,
        cache_write: None,
        output: None,
        reasoning: None,
        provider: None,
        model: None,
        finish: None,
        terminal_status: None,
        context_limit: None,
    }
}

/// Step second `second` of the day the invented steps are timed on.
fn at(second: i64) -> Option<i64> {
    Some(DAY_MS + second * 1_000)
}

/// One published frame: the file it is committed to, and the request params
/// the handler serves it from, or the params the frame itself carries.
struct Vector {
    file: &'static str,
    params: serde_json::Value,
}

/// Every published reply and refusal, served from one store holding all the
/// fixtures, beside the one published request frame and its params.
fn vectors() -> Vec<Vector> {
    let vector = |file: &'static str, params: serde_json::Value| Vector { file, params };
    vec![
        vector(
            "reply-streaming.json",
            json!({"harness": OMP, "session": STREAMING}),
        ),
        vector(
            "reply-earlier-steps.json",
            json!({"harness": OMP, "session": STREAMING, "limit": 1}),
        ),
        vector(
            "reply-undated-steps.json",
            json!({"harness": OMP, "session": UNDATED}),
        ),
        vector(
            "reply-row-every-member.json",
            json!({"harness": OMP, "session": EVERY_MEMBER}),
        ),
        vector(
            "reply-context-window.json",
            json!({"harness": CODEX, "session": CONTEXT_WINDOW}),
        ),
        vector(
            "reply-row-required-only.json",
            json!({"harness": OMP, "session": REQUIRED_ONLY}),
        ),
        // The step after the streaming session's first: its window stays the
        // same, and only the rows past that cursor are served.
        vector(
            AFTER_CURSOR,
            json!({"harness": OMP, "session": STREAMING, "limit": 2, "after_cursor": 1}),
        ),
        // A limit is a whole number from 1, so zero is refused.
        vector(
            "rejected/reply-zero-limit.json",
            json!({"harness": OMP, "session": STREAMING, "limit": 0}),
        ),
        // A harness this operation cannot serve has no steps to answer with.
        vector(
            "rejected/reply-unknown-harness.json",
            json!({"harness": "cursor", "session": STREAMING}),
        ),
        // The request is a closed object: a member it does not define is not
        // quietly ignored, because a caller would never learn it was unused.
        vector(
            "rejected/reply-unknown-member.json",
            json!({"harness": OMP, "session": STREAMING, "page": 2}),
        ),
    ]
}

/// The streaming session: three steps whose verdicts walk three bands. The
/// first has no earlier step and reads no cache, the second is stable against
/// what the first wrote, and the third reads 12,000 of the 16,000 of a prefix
/// that should still be cached, so it is `bust` at a retention of 0.376.
fn streaming_steps() -> Vec<store::Step<'static>> {
    let series = |step_ref: &'static str,
                  second: i64,
                  read: u64,
                  write: u64,
                  output: u64,
                  reasoning: Option<u64>,
                  finish: &'static str| store::Step {
        input: Some(300),
        cache_read: Some(read),
        cache_write: Some(write),
        output: Some(output),
        reasoning,
        provider: Some("openai-codex"),
        model: Some("gpt-5.6-luna"),
        finish: None,
        terminal_status: Some(finish),
        context_limit: None,
        at: at(second),
        ..harness_step(OMP, STREAMING, step_ref)
    };
    vec![
        series("stream-0", 1, 0, 16_000, 800, None, "toolUse"),
        series("stream-1", 2, 15_900, 16_000, 1_200, Some(500), "toolUse"),
        series("stream-2", 3, 12_000, 16_000, 1_000, Some(400), "stop"),
    ]
}

/// The undated session: two steps without a time, and one dated step.
fn undated_steps() -> Vec<store::Step<'static>> {
    let first = store::Step {
        input: Some(30),
        model: Some("gpt-5.6-luna"),
        ..harness_step(OMP, UNDATED, "entry-undated-0")
    };
    let dated = store::Step {
        input: Some(40),
        cache_read: Some(960),
        output: Some(120),
        provider: Some("openai-codex"),
        model: Some("gpt-5.6-luna"),
        at: at(1),
        ..harness_step(OMP, UNDATED, "entry-undated-1")
    };
    let second = store::Step {
        output: Some(90),
        model: Some("gpt-5.6-luna"),
        ..harness_step(OMP, UNDATED, "entry-undated-2")
    };
    vec![first, dated, second]
}

/// The required-only session: one dated step with no members beyond the
/// required, and one step without a time that the reply only counts.
fn required_only_steps() -> Vec<store::Step<'static>> {
    vec![
        store::Step {
            at: at(1),
            ..harness_step(OMP, REQUIRED_ONLY, "required-step-1")
        },
        harness_step(OMP, REQUIRED_ONLY, "required-step-2"),
    ]
}

/// The OMP session whose steps carry every member an OMP row can have: the
/// five counts (the reasoning apart from the visible output), the provider,
/// model and stop reason from the reader, and a context window OMP does not
/// report, so its steps have no `context_limit`.
fn every_member_steps() -> Vec<store::Step<'static>> {
    let step_of = |entry: &'static str,
                   second: i64,
                   input: u64,
                   read: u64,
                   visible: Option<u64>,
                   reasoning: Option<u64>,
                   stop: Option<&'static str>| store::Step {
        // The harness is set by `harness_step`; the provider, model and stop
        // reason are what OMP's reader records for one assistant message.
        input: Some(input),
        cache_read: Some(read),
        cache_write: Some(0),
        output: visible,
        reasoning,
        provider: Some("openai-codex"),
        model: Some("gpt-5.6-luna"),
        finish: None,
        terminal_status: stop,
        context_limit: None,
        at: at(second),
        ..harness_step(OMP, EVERY_MEMBER, entry)
    };
    vec![
        step_of(
            "every-0",
            4,
            858,
            28_416,
            Some(68),
            Some(30),
            Some("toolUse"),
        ),
        step_of("every-1", 5, 902, 28_400, Some(26), None, Some("stop")),
        step_of("every-2", 6, 1_204, 0, Some(40), Some(12), Some("aborted")),
    ]
}

/// The Codex session whose steps carry a context window: its reader keeps
/// the model's window in `step_detail` and stores its implicit cache read.
fn context_window_steps() -> Vec<store::Step<'static>> {
    let step_of = |entry: &'static str, second: i64, input: u64, read: u64| store::Step {
        input: Some(input),
        cache_read: Some(read),
        output: Some(120),
        provider: Some("openai"),
        model: Some("gpt-5.6"),
        context_limit: Some(258_400),
        at: at(second),
        ..harness_step(CODEX, CONTEXT_WINDOW, entry)
    };
    vec![
        step_of("token-count-1", 7, 400, 0),
        step_of("token-count-2", 8, 50, 300),
    ]
}

/// A store holding every fixture, and the recent-steps memory loaded from it,
/// so the request path answers from memory first and the store second.
fn frozen_state() -> SharedState {
    let directory = tempfile::TempDir::new().unwrap();
    let state = crate::state::leased_test_state(&directory);
    state.set_recent_steps_now_for_test(DAY_MS + 3_600_000);
    store::record(&streaming_steps(), &state);
    store::record(&undated_steps(), &state);
    store::record(&required_only_steps(), &state);
    store::record(&every_member_steps(), &state);
    store::record(&context_window_steps(), &state);

    state.rebuild_recent_steps();
    state
}

/// Every published frame's body: the file name, and the bytes it holds.
///
/// A reply is served from its params. A published request frame is the one
/// exception, because the `after_cursor` request has no reply of its own: its
/// bytes are the request a client sends, and it is committed that way.
fn received() -> Vec<(&'static str, Vec<u8>)> {
    let state = frozen_state();
    vectors()
        .into_iter()
        .map(|Vector { file, params }| {
            if file == AFTER_CURSOR {
                return (
                    file,
                    serde_json::to_vec(&json!({"method": OP, "params": params})).unwrap(),
                );
            }
            (file, served(&state, params))
        })
        .collect()
}

/// A published request frame carries the one request member that no reply's
/// params show. Serving its params returns the streaming session's second and
/// third steps, row for row as `reply-streaming.json` holds them.
#[test]
fn the_after_cursor_request_serves_the_steps_past_its_cursor() {
    let state = frozen_state();
    let frames = received()
        .into_iter()
        .map(|(file, body)| {
            (
                file,
                serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        frames[AFTER_CURSOR]["method"], OP,
        "{AFTER_CURSOR} is not a request frame"
    );
    let params = frames[AFTER_CURSOR]["params"].clone();
    let cursor = params[AFTER_CURSOR_MEMBER]
        .as_i64()
        .expect("the published after_cursor request must carry an after_cursor");
    let streaming = frames["reply-streaming.json"]["result"]["steps"].clone();
    let streamed = streaming.as_array().unwrap();
    assert_eq!(
        streamed[0]["cursor"], cursor,
        "the cursor the request reads after must be a step's own"
    );
    let window = serde_json::from_slice::<serde_json::Value>(&served(&state, params)).unwrap();
    assert_eq!(window["result"]["earlier_steps"], true);
    assert_eq!(window["result"]["steps"], serde_json::json!(streamed[1..]));
}

/// Every published file is regenerated and must match the committed bytes
/// exactly: a reply through the real handler, the one request frame from its
/// params. A member that is renamed, reordered, dropped or newly omitted
/// changes those bytes and fails here.
#[test]
fn published_replies_are_the_served_bytes() {
    for (file, body) in received() {
        let expected = std::fs::read(format!("{DIR}/{file}")).unwrap_or_else(|error| {
            panic!("read test-vectors/usage-cache-steps-v1/{file}: {error}")
        });
        assert_eq!(
            body, expected,
            "{file} no longer matches the bytes the handler serves"
        );
    }
}

/// `SHA256SUMS` lists exactly the published files, each with its own hash.
/// Renaming a file, editing one or leaving one out fails here.
#[test]
fn published_sha256sums_cover_every_file() {
    let manifest = std::fs::read_to_string(format!("{DIR}/SHA256SUMS")).unwrap();
    let mut listed = std::collections::BTreeMap::new();
    for line in manifest.lines().filter(|line| !line.is_empty()) {
        let (hash, path) = line
            .split_once("  ")
            .unwrap_or_else(|| panic!("SHA256SUMS line is not '<hash>  <path>': {line}"));
        listed.insert(path.to_string(), hash.to_string());
    }
    let mut published = Vec::new();
    let mut directories = vec![std::path::PathBuf::from(DIR)];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                directories.push(path);
                continue;
            }
            if !path.is_file() {
                continue;
            }
            let name = path
                .strip_prefix(DIR)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if name != "SHA256SUMS" && name != "README.md" {
                published.push(name);
            }
        }
    }
    published.sort();
    let listed_names = listed.keys().cloned().collect::<Vec<_>>();
    assert_eq!(listed_names, published, "SHA256SUMS lists the wrong files");
    for path in published {
        let bytes = std::fs::read(format!("{DIR}/{path}")).unwrap();
        let hash = format!("{:x}", Sha256::digest(&bytes));
        assert_eq!(listed[&path], hash, "{path} does not match SHA256SUMS");
    }
}

/// What each published reply or refusal asks of a decoder, read from the
/// frame the handler serves rather than trusted to match the file's name. The
/// request frame is checked by `the_after_cursor_request_serves_the_steps_past_its_cursor`.
#[test]
fn every_frame_pins_the_case_its_name_names() {
    use serde_json::Value;

    let frames = received()
        .into_iter()
        .map(|(file, body)| (file, serde_json::from_slice::<Value>(&body).unwrap()))
        .collect::<std::collections::BTreeMap<_, _>>();
    // Only a reply frame carries a result; the request frames are checked by
    // `the_after_cursor_request_serves_the_steps_past_its_cursor`.
    let result = |file: &str| -> Value {
        frames[file]
            .get("result")
            .unwrap_or_else(|| panic!("{file} is not a reply frame: {}", frames[file]))
            .clone()
    };
    let step_refs = |reply: &Value| {
        reply["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|step| step["step_ref"].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    };

    let streaming = result("reply-streaming.json");
    assert_eq!(streaming["harness"], OMP);
    assert_eq!(streaming["session"], STREAMING);
    assert_eq!(streaming["earlier_steps"], false);
    assert_eq!(streaming["undated_steps"], 0);
    let verdicts = streaming["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|step| step["verdict"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(verdicts, ["cold_start", "stable", "bust"]);
    let count = |reply: &Value| reply["steps"].as_array().unwrap().len();
    assert_eq!(count(&streaming), 3);

    // A window of one step: the session has older steps than the window.
    let window = result("reply-earlier-steps.json");
    assert_eq!(window["earlier_steps"], true);
    assert_eq!(count(&window), 1);
    assert_eq!(step_refs(&window), [step_refs(&streaming)[2].clone()]);

    let undated = result("reply-undated-steps.json");
    assert_eq!(undated["undated_steps"], 2);
    assert_eq!(undated["earlier_steps"], false);
    assert_eq!(count(&undated), 1);

    // The OMP session's steps carry every member an OMP row has: the five
    // counts, with `output` the visible output plus reasoning, the provider,
    // model and stop reason, and no context window, which OMP does not report.
    let every = result("reply-row-every-member.json");
    assert_eq!(every["harness"], OMP);
    assert_eq!(count(&every), 3);
    let steps = every["steps"].as_array().unwrap().clone();
    assert_eq!(steps[0]["input"], 858);
    assert_eq!(steps[0]["cache_read"], 28_416);
    assert_eq!(steps[0]["cache_write"], 0);
    assert_eq!(steps[0]["output"], 98, "68 visible plus 30 reasoning");
    assert_eq!(steps[0]["reasoning"], 30);
    assert_eq!(steps[0]["provider"], "openai-codex");
    assert_eq!(steps[0]["model"], "gpt-5.6-luna");
    assert_eq!(steps[0]["finish"], "toolUse");
    assert!(steps[0].get("context_limit").is_none(), "{}", steps[0]);
    assert_eq!(steps[1]["verdict"], "stable");
    assert_eq!(
        steps[1]["retention"], 0.970_144_155_223_064_9,
        "{}",
        steps[1]
    );
    assert_eq!(steps[2]["verdict"], "full_bust");
    assert_eq!(steps[2]["retention"], 0.0, "{}", steps[2]);
    // No harness reader reports a request body length, so no harness step
    // carries `request_body_len` or `request_size`.
    for step in &steps {
        assert!(step.get("request_body_len").is_none(), "{step}");
        assert!(step.get("request_size").is_none(), "{step}");
    }
    // The third step's prompt is 28,098 tokens smaller, which is a drop. It
    // carries no request size, because no harness reader reports a body length.
    assert_eq!(steps[2]["prompt_dropped"], true, "{}", steps[2]);

    // Codex is the one harness whose reader keeps a context window.
    let window = result("reply-context-window.json");
    assert_eq!(window["harness"], CODEX);
    assert_eq!(count(&window), 2);
    for step in window["steps"].as_array().unwrap() {
        assert_eq!(step["context_limit"], 258_400, "{step}");
        assert_eq!(step["provider"], "openai", "{step}");
        assert_eq!(step["model"], "gpt-5.6", "{step}");
        assert!(step.get("finish").is_none(), "Codex stores no stop reason");
        assert!(step.get("cache_write").is_none(), "Codex writes no cache");
    }

    let required = result("reply-row-required-only.json");
    assert_eq!(count(&required), 1);
    assert_eq!(required["undated_steps"], 1);
    // Only the members every step carries: no absent member appears, and none
    // is served as a zero. They are compared as a set, because a decoder reads
    // a step by member name and their order on the wire is not the contract.
    let members = required["steps"][0]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        members,
        [
            "cursor",
            "step_ref",
            "occurred_at_ms",
            "prompt",
            "verdict",
            "prompt_dropped"
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    );

    for file in [
        "rejected/reply-zero-limit.json",
        "rejected/reply-unknown-harness.json",
        "rejected/reply-unknown-member.json",
    ] {
        assert!(frames[file].get("result").is_none(), "{file} was served");
        assert_eq!(frames[file]["error"]["code"], error_code(file));
    }
}

/// The refusal code each rejected request is published for.
fn error_code(file: &str) -> &'static str {
    match file {
        "rejected/reply-zero-limit.json" => "invalid_request",
        "rejected/reply-unknown-harness.json" => "unknown_harness",
        "rejected/reply-unknown-member.json" => "malformed",
        other => panic!("no refusal code for {other}"),
    }
}
