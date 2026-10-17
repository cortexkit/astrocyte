#![forbid(unsafe_code)]

//! Drives a producer's real exported facts through the v2 ingest path.
//!
//! # Why this exists
//!
//! v2's ingest and measurement extraction are covered by unit tests written
//! alongside the code they test, which means they share the author's model of
//! what a fact looks like. This harness runs the same path against bytes a
//! producer actually emitted, where the shapes were chosen by a vendor rather
//! than by us.
//!
//! # What a green run does and does not establish
//!
//! It establishes that the corpus's shapes ingest. It establishes nothing about
//! dimensions the corpus holds only one value of — and every such dimension is
//! reported beside the result, because a corpus with one value in a dimension
//! does not test that dimension, it fails to notice it.
//!
//! The run refuses to report at all unless it can name the corpus it read: the
//! manifest's `sha256` says WHICH artifact, and `capture_files_read` says
//! whether two artifacts had the same subject. A result carrying only the first
//! identifies something nobody can compare; only the second describes the input,
//! which is the thing that moves when a producer prunes.

use std::collections::BTreeMap;
use std::path::PathBuf;

use astrocyte_core::ingest::{record_event, EventRecordRequest};
use astrocyte_core::store::{initialize_connection, MIGRATIONS};
use rusqlite::Connection;
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Deserialize)]
struct Manifest {
    event_count: usize,
    sha256: String,
    capture_files_read: usize,
    generator_commit: String,
    /// True when the generator ran from a dirty tree, which makes
    /// `generator_commit` an assertion about code nobody can check.
    generator_modified: bool,
}

fn main() {
    let corpus = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            eprintln!("usage: corpus_drive <path-to.jsonl>  (manifest must sit beside it)");
            std::process::exit(2);
        });
    let manifest_path = corpus.with_extension("jsonl.manifest.json");

    // IDENTITY BEFORE MEASUREMENT. Without a manifest the run could still
    // execute and its result would name no subject, which is a count with no
    // denominator one level up.
    let manifest: Manifest = match std::fs::read(&manifest_path) {
        Ok(bytes) => serde_json::from_slice(&bytes).expect("manifest is not valid JSON"),
        Err(error) => {
            eprintln!("no manifest at {}: {error}", manifest_path.display());
            eprintln!("refusing to run: a result whose corpus identity is unrecorded is a claim about an artifact nobody can reproduce");
            std::process::exit(2);
        }
    };

    let bytes = std::fs::read(&corpus).expect("corpus unreadable");
    let digest = format!("{:x}", <Sha256 as Digest>::digest(&bytes));
    if digest != manifest.sha256 {
        eprintln!(
            "corpus sha {digest} does not match manifest {}",
            manifest.sha256
        );
        eprintln!(
            "refusing to run: the manifest describes a different artifact than the one on disk"
        );
        std::process::exit(2);
    }
    let corpus_text = String::from_utf8(bytes).expect("utf8");
    if corpus_text.trim().is_empty() {
        eprintln!("refusing to run: empty corpus has no input to exercise");
        std::process::exit(2);
    }

    // A fresh in-memory store per run: this harness measures whether real
    // shapes ingest, not whether they accumulate.
    let mut conn = Connection::open_in_memory().expect("store");
    initialize_connection(&conn).expect("connection prerequisites");
    for migration in MIGRATIONS {
        conn.execute_batch(migration.statements).expect("migration");
    }
    let mut ingested = 0usize;
    let mut refused: BTreeMap<String, usize> = BTreeMap::new();
    let mut measurements = 0usize;
    // Dimensions we report the spread of, because a monovalent one is a
    // dimension the run did not test rather than one it passed.
    let mut basis: BTreeMap<String, usize> = BTreeMap::new();
    let mut models: BTreeMap<String, usize> = BTreeMap::new();
    let mut role_unit: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut zero_quantities = 0usize;
    // Counted, not inferred: a claim that absences do not occur requires
    // observing absences, and "present, not omitted" welded to a zero count
    // would be a measured clause vouching for an unmeasured one.
    let mut absent_quantities = 0usize;

    for (line_no, line) in corpus_text.lines().enumerate() {
        let request: EventRecordRequest = match serde_json::from_str(line) {
            Ok(request) => request,
            Err(error) => {
                *refused.entry(decode_reason(&error)).or_default() += 1;
                continue;
            }
        };
        *basis
            .entry(format!("{:?}", request.charge_basis))
            .or_default() += 1;
        if let Some(model) = &request.model {
            *models.entry(model.clone()).or_default() += 1;
        }
        for measurement in &request.measurements {
            *role_unit
                .entry((measurement.role.clone(), measurement.unit.clone()))
                .or_default() += 1;
            match measurement.quantity_units {
                Some(0) => zero_quantities += 1,
                None => absent_quantities += 1,
                Some(_) => {}
            }
            measurements += 1;
        }
        // A line index cannot exceed the addressable corpus string length.
        let line_no = i64::try_from(line_no).expect("corpus line count fits i64");
        match record_event(&mut conn, &request, 1_700_000_000_000 + line_no) {
            Ok(_) => ingested += 1,
            Err(error) => *refused.entry(format!("{error:?}")).or_default() += 1,
        }
    }

    println!("CORPUS");
    println!("  path                {}", corpus.display());
    println!("  sha256              {}", manifest.sha256);
    println!("  capture_files_read  {}", manifest.capture_files_read);
    println!(
        "  generator           {} (modified: {})",
        manifest.generator_commit, manifest.generator_modified
    );
    if manifest.generator_modified {
        println!("  WARNING: generator_commit describes code that was not what ran");
    }
    println!("  manifest events     {}", manifest.event_count);

    println!("\nINGEST");
    println!("  ingested            {ingested}");
    println!("  measurements        {measurements}");
    println!("  zero quantities     {zero_quantities}");
    println!("  absent quantities   {absent_quantities}");
    if refused.is_empty() {
        println!("  refused             0");
    } else {
        for (reason, count) in &refused {
            println!("  REFUSED {count:>6}      {reason}");
        }
    }

    println!("\nWHAT THIS RUN COULD NOT TEST");
    report_spread("charge_basis", &basis);
    report_spread("model", &models);
    println!("\nROLE / UNIT COVERAGE");
    for ((role, unit), count) in &role_unit {
        println!("  {role:<12} {unit:<18} {count}");
    }
    if !refused.is_empty() {
        std::process::exit(1);
    }
}

fn decode_reason(error: &serde_json::Error) -> String {
    let message = error.to_string();
    // Position identifies a record, not a refusal category. Each JSONL line is
    // decoded separately, so including its column would split the same defect.
    let reason = message
        .rsplit_once(" at line ")
        .map_or(message.as_str(), |(reason, _)| reason);
    format!("decode: {reason}")
}

/// A dimension with one value did not pass, it was never exercised — and a
/// dimension whose mass sits on one value tested that value and noticed the
/// others.
fn report_spread(name: &str, counts: &BTreeMap<String, usize>) {
    let total: usize = counts.values().sum();
    if counts.is_empty() {
        println!("  {name}: NO COVERAGE — no values were observed");
        return;
    }
    if counts.len() == 1 {
        let (value, count) = counts.iter().next().expect("non-empty");
        println!(
            "  {name}: MONOVALENT at {value} ({count} of {total}) — this dimension is untested"
        );
        return;
    }
    let mut sorted: Vec<_> = counts.iter().collect();
    sorted.sort_by_key(|(_, count)| std::cmp::Reverse(**count));
    let (top_value, top_count) = sorted[0];
    println!(
        "  {name}: {} values, {top_count} of {total} on {top_value}",
        counts.len()
    );
    for (value, count) in sorted.iter().skip(1) {
        if **count * 100 < total {
            println!("      {value}: {count} — under 1%, an anecdote rather than coverage");
        }
    }
}
