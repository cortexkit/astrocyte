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
