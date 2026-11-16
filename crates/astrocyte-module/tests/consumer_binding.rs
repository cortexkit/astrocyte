//! Guard against legacy per-call identities on claim-bound SDK connections.

fn forbidden_identity(source: &str) -> bool {
    // Older daemons can still refuse with the exact code `bad_consumer_identity`,
    // so the source may name that code. Strip it before searching, so only a
    // request that sends its own `consumer_identity` member is caught.
    source
        .replace("\"bad_consumer_identity\"", "")
        .contains("consumer_identity")
}

#[test]
fn consumer_binding_guard_rejects_request_members_but_allows_exact_refusal() {
    assert!(forbidden_identity(
        r#"json!({"consumer_identity": identity})"#
    ));
    assert!(forbidden_identity("options.consumer_identity = None;"));
    assert!(forbidden_identity(r#""prefix_bad_consumer_identity""#));
    assert!(!forbidden_identity(r#"Some("bad_consumer_identity")"#));
}

#[test]
fn shipped_source_has_no_consumer_identity() {
    fn check(directory: &std::path::Path, violations: &mut Vec<String>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                check(&path, violations);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let name = path.file_name().unwrap().to_str().unwrap();
                if name == "tests.rs" || name.ends_with("_tests.rs") {
                    continue;
                }
                for (line, text) in std::fs::read_to_string(&path).unwrap().lines().enumerate() {
                    if forbidden_identity(text) {
                        violations.push(format!("{}:{}", path.display(), line + 1));
                    }
                }
            }
        }
    }
    let mut violations = Vec::new();
    check(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut violations,
    );
    assert!(
        violations.is_empty(),
        "claim-bound connections must not name a legacy per-call identity: {violations:?}"
    );
}
