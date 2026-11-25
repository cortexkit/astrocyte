use rusqlite::Connection;

use crate::ingest::MeasurementInput;
use crate::store::{initialize_connection, MIGRATIONS};

pub(crate) fn conn() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    initialize_connection(&conn).unwrap();
    for migration in MIGRATIONS {
        conn.execute_batch(migration.statements).unwrap();
    }
    conn
}

pub(crate) fn measurement(role: &str, units: Option<i64>) -> MeasurementInput {
    MeasurementInput {
        role: role.into(),
        unit: "token".into(),
        kind: "billed".into(),
        source: format!("usage.{role}"),
        quantity_units: units,
        quantity_exponent: units.map(|_| 0),
    }
}

pub(crate) fn reasoning_vector<T: serde::de::DeserializeOwned>(name: &str) -> T {
    use sha2::{Digest, Sha256};

    macro_rules! sample {
        ($name:literal) => {
            include_bytes!(concat!(
                "../test-vectors/fusiform-reasoning-billed-as/",
                $name
            ))
        };
    }
    const MANIFEST: &[u8] = sample!("SHA256SUMS");
    const FILES: &[(&str, &[u8])] = &[
        ("anthropic-current.json", sample!("anthropic-current.json")),
        ("anthropic-dated.json", sample!("anthropic-dated.json")),
        (
            "kimi-code-plan-global.json",
            sample!("kimi-code-plan-global.json"),
        ),
        ("openai-current.json", sample!("openai-current.json")),
        ("openai-dated.json", sample!("openai-dated.json")),
        ("openai-history.json", sample!("openai-history.json")),
        ("opencode-go.json", sample!("opencode-go.json")),
        ("README.md", sample!("README.md")),
    ];
    let manifest = std::str::from_utf8(MANIFEST).unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(MANIFEST)),
        "02822546cd4362058b019c73bec51fa3f534e86074efaa2d8167658498044538"
    );
    assert_eq!(manifest.lines().count(), FILES.len());
    for (name, bytes) in FILES {
        let digest = hex::encode(Sha256::digest(bytes));
        assert!(manifest
            .lines()
            .any(|line| line == format!("{digest}  {name}")));
    }
    let (_, bytes) = FILES.iter().find(|(file, _)| *file == name).unwrap();
    serde_json::from_slice(bytes).unwrap()
}
