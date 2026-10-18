//! Declared headless Claude Code roots and post-commit read acknowledgements.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::harness_paths::ClaudeWorkerPaths;
use crate::refresher::claude_code_source::{self, ClaudeCodeCache, ClaudeCodeDrainOutcome};
use crate::refresher::harness_drain::{self, HarnessSource};
use crate::refresher::SourceDrainError;
use crate::state::SharedState;

const SOURCE_KEY: &str = "claude-code-workers";

#[derive(Clone, Deserialize)]
struct Root {
    worker_id: String,
    path: PathBuf,
    work_class: String,
    state: WorkerState,
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum WorkerState {
    Active,
    Done,
}

#[derive(Deserialize)]
struct Registry {
    roots: Vec<Root>,
}

#[derive(Default, PartialEq, Deserialize, Serialize)]
struct Ack {
    version: u64,
    read_through: Vec<ReadThrough>,
}

#[derive(PartialEq, Deserialize, Serialize)]
struct ReadThrough {
    worker_id: String,
    at_ms: i64,
}

#[derive(Default)]
struct Cache {
    roots: Vec<Root>,
    resumes: HashMap<PathBuf, ClaudeCodeCache>,
    // A resumed read emits a refused call only once. Remember that it was not
    // recorded, so a later empty read cannot authorize deleting its transcript.
    blocked: HashSet<String>,
    ack: Option<Ack>,
    warned: HashSet<String>,
}

pub(crate) struct ClaudeWorkersSource {
    paths: Result<ClaudeWorkerPaths, String>,
    cache: Mutex<Cache>,
}

impl ClaudeWorkersSource {
    pub(crate) fn from_data_home(home: PathBuf) -> Self {
        Self {
            paths: crate::harness_paths::claude_workers(home),
            cache: Mutex::new(Cache::default()),
        }
    }

    fn drain(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let paths = self
            .paths
            .as_ref()
            .map_err(|e| SourceDrainError::Failed(e.clone()))?;
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "Claude workers cache lock poisoned".to_string())?;
        if cache.ack.is_none() {
            cache.ack = Some(read_ack(&paths.ack)?);
        }
        let mut degraded = None;
        match fs::read(&paths.roots) {
            Ok(bytes) => {
                let value: Result<serde_json::Value, _> = serde_json::from_slice(&bytes);
                match value {
                    Ok(value)
                        if value
                            .get("version")
                            .and_then(serde_json::Value::as_u64)
                            .is_some_and(|v| v != 1) =>
                    {
                        return Err("unknown Claude worker roots version".to_string().into());
                    }
                    Ok(value) => match decode_registry(value) {
                        Ok(roots) => cache.roots = roots,
                        Err(reason) => degraded = Some(reason),
                    },
                    Err(error) => degraded = Some(format!("decode Claude worker roots: {error}")),
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                harness_drain::remove_runtime_row(state, SOURCE_KEY)?;
                cache.roots.clear();
                cache.resumes.clear();
                cache.blocked.clear();
                if !cache
                    .ack
                    .as_ref()
                    .expect("ack loaded")
                    .read_through
                    .is_empty()
                {
                    update_ack(
                        paths,
                        &mut cache,
                        Ack {
                            version: 1,
                            read_through: vec![],
                        },
                    )?;
                }
                return Ok(None);
            }
            Err(error) => degraded = Some(format!("read Claude worker roots: {error}")),
        }
        let registry_valid = degraded.is_none();
        harness_drain::ensure_runtime_row(state, SOURCE_KEY)?;
        let folder = fs::canonicalize(paths.roots.parent().expect("roots parent"))
            .map_err(|e| format!("resolve Claude worker folder: {e}"))?;
        let roots = cache.roots.clone();
        let mut next_resumes = HashMap::new();
        let mut ready = HashSet::new();
        let mut refs_by_worker = HashMap::new();
        let mut blocked = cache.blocked.clone();
        let mut requests = Vec::new();
        let mut set_aside = Vec::new();
        let mut outcome = ClaudeCodeDrainOutcome::default();
        for root in &roots {
            if stop.load(Ordering::Relaxed) {
                return Err(SourceDrainError::Stopped);
            }
            if !root.path.is_absolute() {
                warn_once(
                    &mut cache,
                    format!("refuse relative worker root {}", root.path.display()),
                );
                continue;
            }
            let canonical = match fs::canonicalize(&root.path) {
                Ok(path) => path,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    warn_once(
                        &mut cache,
                        format!("skip deleted worker root {}", root.path.display()),
                    );
                    continue;
                }
                Err(error) => {
                    return Err(
                        format!("resolve worker root {}: {error}", root.path.display()).into(),
                    )
                }
            };
            if canonical == folder || !canonical.starts_with(&folder) {
                warn_once(
                    &mut cache,
                    format!(
                        "refuse worker root outside declared folder: {}",
                        root.path.display()
                    ),
                );
                continue;
            }
            let projects = canonical.join("projects");
            let previous = cache.resumes.get(&canonical).cloned().unwrap_or_default();
            let read = match fs::metadata(&projects) {
                Ok(_) => claude_code_source::read_projects(
                    &projects,
                    &previous,
                    at,
                    astrocyte_readers::sessions::claudecode::CLAUDE_CODE_QUIET_WINDOW_MS,
                    Some(&root.work_class),
                    stop,
                )?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    // A worker may exit without ever creating a transcript.
                    claude_code_source::ClaudeCodeRead {
                        outcome: ClaudeCodeDrainOutcome::default(),
                        requests: vec![],
                        set_aside: vec![],
                        cache: ClaudeCodeCache::default(),
                    }
                }
                Err(error) => {
                    return Err(
                        format!("inspect worker projects {}: {error}", projects.display()).into(),
                    )
                }
            };
            if read.outcome.counts.calls_refused > 0 || !read.set_aside.is_empty() {
                blocked.insert(root.worker_id.clone());
            }
            if root.state == WorkerState::Done
                && !blocked.contains(&root.worker_id)
                && read.outcome.calls_pending == 0
                && read.outcome.counts.calls_refused == 0
                && read.set_aside.is_empty()
            {
                ready.insert(root.worker_id.clone());
            }
            outcome.counts.calls_refused += read.outcome.counts.calls_refused;
            refs_by_worker.insert(
                root.worker_id.clone(),
                read.requests
                    .iter()
                    .map(|r| r.producer_ref.clone())
                    .collect::<HashSet<_>>(),
            );
            requests.extend(read.requests);
            set_aside.extend(read.set_aside);
            next_resumes.insert(canonical, read.cache);
        }
        let commit = claude_code_source::record_requests(
            state,
            SOURCE_KEY,
            &requests,
            &set_aside,
            at,
            &mut outcome,
            &|| stop.load(Ordering::Relaxed),
        );
        commit?;
        cache.resumes = next_resumes;
        for (id, refs) in refs_by_worker {
            if outcome.quarantined_refs.iter().any(|r| refs.contains(r)) {
                blocked.insert(id);
            }
        }
        blocked.retain(|id| roots.iter().any(|r| &r.worker_id == id));
        cache.blocked = blocked;
        if outcome.quarantined_redeliveries > 0 || outcome.counts.calls_refused > 0 {
            degraded = Some("Claude worker calls were quarantined or refused".to_string());
        }
        // A malformed or unreadable roots file cannot authorize deletion. Publish
        // acknowledgements only after a valid roots file was read and the ledger
        // transaction recording its calls and last-success time has committed.
        if registry_valid {
            ready.retain(|id| !cache.blocked.contains(id));
            let declared: HashSet<_> = roots.iter().map(|r| r.worker_id.as_str()).collect();
            let mut entries: BTreeMap<String, i64> = cache
                .ack
                .as_ref()
                .expect("ack loaded")
                .read_through
                .iter()
                .filter(|r| declared.contains(r.worker_id.as_str()))
                .map(|r| (r.worker_id.clone(), r.at_ms))
                .collect();
            for worker_id in ready {
                entries.entry(worker_id).or_insert(at);
            }
            let ack = Ack {
                version: 1,
                read_through: entries
                    .into_iter()
                    .map(|(worker_id, at_ms)| ReadThrough { worker_id, at_ms })
                    .collect(),
            };
            update_ack(paths, &mut cache, ack)?;
        }
        Ok(degraded)
    }
}

impl HarnessSource for ClaudeWorkersSource {
    fn source_key(&self) -> &'static str {
        SOURCE_KEY
    }

    fn drain_for_health(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        self.drain(state, at, stop)
    }
}

fn decode_registry(value: serde_json::Value) -> Result<Vec<Root>, String> {
    if value.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
        return Err("missing or invalid Claude worker roots version".into());
    }
    let registry: Registry =
        serde_json::from_value(value).map_err(|e| format!("decode Claude worker roots: {e}"))?;
    let mut ids = HashSet::new();
    for root in &registry.roots {
        let suffix = root.worker_id.strip_prefix("w-").unwrap_or("");
        if suffix.len() != 32
            || !suffix
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || !ids.insert(&root.worker_id)
        {
            return Err("invalid or duplicate Claude worker id".into());
        }
    }
    Ok(registry.roots)
}

fn warn_once(cache: &mut Cache, reason: String) {
    if cache.warned.insert(reason.clone()) {
        tracing::warn!(target: "drain", producer = SOURCE_KEY, %reason, "skip declared Claude worker root");
    }
}

fn read_ack(path: &Path) -> Result<Ack, SourceDrainError> {
    match fs::read(path) {
        Ok(bytes) => {
            let ack: Ack = serde_json::from_slice(&bytes)
                .map_err(|e| format!("decode Claude worker ack: {e}"))?;
            if ack.version != 1 {
                return Err("unknown Claude worker ack version".to_string().into());
            }
            Ok(ack)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Ack {
            version: 1,
            read_through: vec![],
        }),
        Err(error) => Err(format!("read Claude worker ack: {error}").into()),
    }
}

fn update_ack(
    paths: &ClaudeWorkerPaths,
    cache: &mut Cache,
    ack: Ack,
) -> Result<(), SourceDrainError> {
    if cache.ack.as_ref() == Some(&ack) && paths.ack.exists() {
        return Ok(());
    }
    write_ack(&paths.ack, &ack).map_err(|e| format!("write Claude worker ack: {e}"))?;
    cache.ack = Some(ack);
    Ok(())
}

fn write_ack(path: &Path, ack: &Ack) -> std::io::Result<()> {
    let folder = path.parent().expect("ack parent");
    fs::create_dir_all(folder)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(folder, fs::Permissions::from_mode(0o700))?;
    }
    // Exclusive creation in the destination directory prevents partial reads.
    static TEMP_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let (temporary, mut file) = loop {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = folder.join(format!(
            ".claude-workers-{}-{sequence}.tmp",
            std::process::id()
        ));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&temporary) {
            Ok(file) => break (temporary, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    let result = (|| {
        serde_json::to_writer(&mut file, ack)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::{
        claude_code_source::ClaudeCodeSource, run_iteration_with_claude_code,
        run_iteration_with_harness,
    };
    use crate::state::test_state;
    use serde_json::{json, Value};
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    const WORKER: &str = "w-0123456789abcdef0123456789abcdef";
    const OTHER: &str = "w-1123456789abcdef0123456789abcdef";

    struct Fixture {
        temp: TempDir,
        folder: PathBuf,
        roots: PathBuf,
        ack: PathBuf,
        source: ClaudeWorkersSource,
        state: Arc<SharedState>,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_temp(TempDir::new().unwrap())
        }

        fn new_in(directory: &Path) -> Self {
            Self::with_temp(tempfile::tempdir_in(directory).unwrap())
        }

        fn with_temp(temp: TempDir) -> Self {
            let home = fs::canonicalize(temp.path()).unwrap();
            let folder = home.join("cortexkit/thalamus-harness/claude-workers");
            fs::create_dir_all(&folder).unwrap();
            Self {
                roots: folder.join("roots.json"),
                ack: home.join("cortexkit/astrocyte/acks/claude-workers.json"),
                source: ClaudeWorkersSource::from_data_home(home),
                temp,
                folder,
                state: Arc::new(test_state()),
            }
        }

        fn root(&self, id: &str, done: bool) -> Value {
            json!({"worker_id":id,"path":self.folder.join(id),"work_class":"claude-code-worker",
                "state":if done {"done"} else {"active"},"done_at_ms":if done {Some(50)} else {None},
                "account_ref":{"ignored":"not interpreted"},"future":true})
        }

        fn declare(&self, roots: Vec<Value>) {
            fs::write(
                &self.roots,
                json!({"version":1,"roots":roots,"future":true}).to_string(),
            )
            .unwrap();
        }

        fn call(&self, id: &str, request: &str, complete: bool) {
            write_call(&self.folder.join(id), request, complete);
        }

        fn tick(&self, at: i64) {
            struct Clocked<'a>(&'a ClaudeWorkersSource, i64);
            impl HarnessSource for Clocked<'_> {
                fn source_key(&self) -> &'static str {
                    SOURCE_KEY
                }
                fn drain_for_health(
                    &self,
                    state: &SharedState,
                    _: i64,
                    stop: &AtomicBool,
                ) -> Result<Option<String>, SourceDrainError> {
                    self.0.drain_for_health(state, self.1, stop)
                }
            }
            run_iteration_with_harness(
                &self.state,
                &[],
                &Clocked(&self.source, at),
                &AtomicBool::new(false),
            );
        }

        fn runtime(&self) -> Option<(Option<i64>, bool)> {
            use rusqlite::OptionalExtension;
            self.state.with_conn(|conn| conn.query_row("SELECT last_success_ms,pending_unknown FROM source_runtime WHERE source_key=?1", [SOURCE_KEY], |row| Ok((row.get(0)?,row.get(1)?))).optional().map_err(|e| e.to_string())).unwrap()
        }

        fn events(&self) -> Vec<(String, String, Option<String>)> {
            self.state.with_conn(|conn| {
                let mut stmt = conn.prepare("SELECT producer_ref,producer,work_class FROM usage_event ORDER BY producer_ref").unwrap();
                Ok(stmt.query_map([], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap())
            }).unwrap()
        }

        fn acks(&self) -> Value {
            serde_json::from_slice(&fs::read(&self.ack).unwrap()).unwrap()
        }
    }

    fn write_call(root: &Path, request: &str, complete: bool) {
        let folder = root.join("projects/project/session/subagents");
        fs::create_dir_all(&folder).unwrap();
        let record = json!({"type":"assistant","timestamp":"2026-09-01T00:00:01.000Z","requestId":request,
            "sessionId":"session","cwd":"/work","message":{"id":format!("msg_{request}"),"model":"claude-opus-5",
            "stop_reason":if complete {Some("end_turn")} else {None},"usage":{"input_tokens":3,"output_tokens":5}}});
        fs::write(
            folder.join(format!("{request}.jsonl")),
            format!("{record}\n"),
        )
        .unwrap();
    }

    #[test]
    fn worker_events_stamp_declared_class_without_changing_identity() {
        let f = Fixture::new();
        f.call(WORKER, "worker", true);
        f.declare(vec![f.root(WORKER, false)]);
        f.tick(100);
        assert_eq!(
            f.events(),
            vec![(
                "worker".into(),
                "claude-code".into(),
                Some("claude-code-worker".into())
            )]
        );
        let mut other = f.root(OTHER, false);
        other["work_class"] = json!("custom verbatim class");
        f.call(OTHER, "other", true);
        f.declare(vec![f.root(WORKER, false), other]);
        f.tick(200);
        assert_eq!(f.events()[0].2.as_deref(), Some("custom verbatim class"));
        assert_eq!(f.runtime(), Some((Some(200), false)));
    }

    #[test]
    fn main_claude_root_still_has_no_work_class() {
        let f = Fixture::new();
        let main = f.temp.path().join("main");
        write_call(&main, "main", true);
        run_iteration_with_claude_code(
            &f.state,
            &[],
            &ClaudeCodeSource::at(main.join("projects")),
            &AtomicBool::new(false),
        );
        assert_eq!(
            f.events(),
            vec![("main".into(), "claude-code".into(), None)]
        );
    }

    fn refuses_path(f: Fixture, path: impl FnOnce(&Fixture) -> PathBuf) {
        f.call(WORKER, "good", true);
        f.call(OTHER, "bad", true);
        let mut bad = f.root(OTHER, false);
        bad["path"] = json!(path(&f));
        f.declare(vec![bad, f.root(WORKER, false)]);
        f.tick(100);
        assert_eq!(
            f.events(),
            vec![(
                "good".into(),
                "claude-code".into(),
                Some("claude-code-worker".into())
            )]
        );
        assert_eq!(f.runtime(), Some((Some(100), false)));
    }

    #[test]
    fn relative_root_is_refused_without_blocking_other_roots() {
        // Windows may put the checkout and system temp directory on different
        // drives. A fixture under the canonical cwd has a real relative path.
        let cwd = fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
        refuses_path(Fixture::new_in(&cwd), |f| {
            let target = f.folder.join(OTHER);
            let relative = target.strip_prefix(&cwd).unwrap().to_path_buf();
            assert!(relative.is_relative());
            assert!(relative.exists());
            relative
        });
    }

    #[test]
    fn outside_root_is_refused_without_blocking_other_roots() {
        refuses_path(Fixture::new(), |f| {
            let outside = f.temp.path().join("outside");
            write_call(&outside, "bad", true);
            outside
        });
    }

    #[cfg(unix)]
    #[test]
    fn escaping_symlink_is_refused_without_blocking_other_roots() {
        refuses_path(Fixture::new(), |f| {
            let outside = f.temp.path().join("outside");
            write_call(&outside, "bad", true);
            let link = f.folder.join("escape");
            std::os::unix::fs::symlink(outside, &link).unwrap();
            link
        });
    }

    #[test]
    fn unknown_version_skips_roots_and_degrades_source() {
        let f = Fixture::new();
        f.call(WORKER, "first", true);
        f.declare(vec![f.root(WORKER, false)]);
        f.tick(100);
        f.call(WORKER, "new", true);
        fs::write(
            &f.roots,
            json!({"version":2,"roots":[f.root(WORKER,false)]}).to_string(),
        )
        .unwrap();
        f.tick(200);
        assert_eq!(f.events().len(), 1);
        assert_eq!(f.runtime(), Some((Some(100), true)));
    }

    #[test]
    fn malformed_registry_reads_last_good_roots_and_degrades() {
        let f = Fixture::new();
        f.call(WORKER, "first", true);
        f.declare(vec![f.root(WORKER, false)]);
        f.tick(100);
        f.call(WORKER, "second", true);
        fs::write(&f.roots, b"{").unwrap();
        f.tick(200);
        assert_eq!(f.events().len(), 2);
        assert_eq!(f.runtime(), Some((Some(200), true)));
    }

    #[test]
    fn unreadable_registry_reads_last_good_roots_and_degrades() {
        let f = Fixture::new();
        f.call(WORKER, "first", true);
        f.declare(vec![f.root(WORKER, false)]);
        f.tick(100);
        f.call(WORKER, "second", true);
        fs::remove_file(&f.roots).unwrap();
        fs::create_dir(&f.roots).unwrap();
        f.tick(200);
        assert_eq!(f.events().len(), 2);
        assert_eq!(f.runtime(), Some((Some(200), true)));
    }

    #[test]
    fn absent_registry_removes_runtime_and_ack_entries() {
        let f = Fixture::new();
        f.call(WORKER, "first", true);
        f.declare(vec![f.root(WORKER, true)]);
        f.tick(100);
        fs::remove_file(&f.roots).unwrap();
        f.tick(200);
        assert_eq!(f.runtime(), None);
        assert_eq!(f.acks()["read_through"], json!([]));
    }

    #[test]
    fn nothing_new_and_empty_registry_advance_last_success() {
        let f = Fixture::new();
        f.call(WORKER, "first", true);
        f.declare(vec![f.root(WORKER, false)]);
        f.tick(100);
        f.tick(200);
        assert_eq!(f.events().len(), 1);
        assert_eq!(f.runtime(), Some((Some(200), false)));
        f.declare(vec![]);
        f.tick(300);
        assert_eq!(f.runtime(), Some((Some(300), false)));
    }

    #[test]
    fn deleted_worker_is_skipped_without_degradation() {
        let f = Fixture::new();
        f.call(WORKER, "good", true);
        f.declare(vec![f.root(OTHER, true), f.root(WORKER, false)]);
        f.tick(100);
        assert_eq!(f.events().len(), 1);
        assert_eq!(f.runtime(), Some((Some(100), false)));
        assert_eq!(f.acks()["read_through"], json!([]));
    }

    #[test]
    fn ack_requires_done_and_waits_for_quiet_pending_call() {
        let f = Fixture::new();
        f.call(WORKER, "complete", true);
        f.declare(vec![f.root(WORKER, false)]);
        f.tick(100);
        assert_eq!(f.acks()["read_through"], json!([]));
        f.call(WORKER, "unfinished", false);
        f.declare(vec![f.root(WORKER, true)]);
        f.tick(200);
        f.tick(600_199);
        assert_eq!(f.events().len(), 1);
        assert_eq!(f.acks()["read_through"], json!([]));
        f.tick(600_200);
        assert_eq!(f.events().len(), 2);
        assert_eq!(
            f.acks(),
            json!({"version":1,"read_through":[{"worker_id":WORKER,"at_ms":600_200}]})
        );
    }

    #[test]
    fn ack_is_never_written_before_event_transaction_commits() {
        let f = Fixture::new();
        f.call(WORKER, "complete", true);
        f.declare(vec![f.root(WORKER, true)]);
        f.state.with_conn(|conn| conn.execute_batch("CREATE TRIGGER refuse_commit BEFORE UPDATE OF last_success_ms ON source_runtime BEGIN SELECT RAISE(ABORT,'refuse commit'); END;").map_err(|e| e.to_string())).unwrap();
        f.tick(100);
        assert!(f.events().is_empty());
        assert!(!f.ack.exists(), "ack must not precede the event commit");
        f.state
            .with_conn(|conn| {
                conn.execute_batch("DROP TRIGGER refuse_commit")
                    .map_err(|e| e.to_string())
            })
            .unwrap();
        f.tick(200);
        assert_eq!(f.events().len(), 1);
        assert_eq!(
            f.acks()["read_through"],
            json!([{"worker_id":WORKER,"at_ms":200}])
        );
    }

    #[test]
    fn ack_waits_for_unrecordable_calls_across_resumed_drains() {
        let f = Fixture::new();
        f.call(WORKER, "bad", true);
        let path = f
            .folder
            .join(WORKER)
            .join("projects/project/session/subagents/bad.jsonl");
        let mut record: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        record["message"]["usage"]["output_tokens"] = json!(1_u64 << 63);
        fs::write(&path, format!("{record}\n")).unwrap();
        f.declare(vec![f.root(WORKER, true)]);
        f.tick(100);
        f.tick(200);
        assert!(f.events().is_empty());
        assert!(f.acks()["read_through"].as_array().unwrap().is_empty());
    }

    #[test]
    fn quarantined_worker_does_not_block_other_worker_ack_or_pruning() {
        let f = Fixture::new();
        f.call(WORKER, "changed", true);
        f.declare(vec![f.root(WORKER, false)]);
        f.tick(100);
        let path = f
            .folder
            .join(WORKER)
            .join("projects/project/session/subagents/changed.jsonl");
        let mut record: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        record["message"]["usage"]["output_tokens"] = json!(9);
        fs::write(&path, format!("{record}\n")).unwrap();
        f.call(OTHER, "good", true);
        f.declare(vec![f.root(WORKER, true), f.root(OTHER, true)]);
        f.tick(200);
        assert_eq!(
            f.acks()["read_through"],
            json!([{"worker_id":OTHER,"at_ms":200}])
        );
        f.declare(vec![f.root(WORKER, true)]);
        f.tick(300);
        assert_eq!(f.acks()["read_through"], json!([]));
    }

    #[test]
    fn ack_drops_removed_workers_and_preserves_timestamp_on_restart() {
        let mut f = Fixture::new();
        f.call(WORKER, "one", true);
        f.call(OTHER, "two", true);
        f.declare(vec![f.root(WORKER, true), f.root(OTHER, true)]);
        f.tick(100);
        assert_eq!(f.acks()["read_through"].as_array().unwrap().len(), 2);
        f.source = ClaudeWorkersSource::from_data_home(fs::canonicalize(f.temp.path()).unwrap());
        f.tick(200);
        assert_eq!(f.acks()["read_through"][0]["at_ms"], 100);
        f.declare(vec![f.root(OTHER, true)]);
        f.tick(300);
        assert_eq!(
            f.acks()["read_through"],
            json!([{"worker_id":OTHER,"at_ms":100}])
        );
    }

    #[cfg(unix)]
    #[test]
    fn ack_is_private_and_unchanged_ack_is_not_rewritten() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mut f = Fixture::new();
        f.call(WORKER, "one", true);
        f.declare(vec![f.root(WORKER, true)]);
        f.tick(100);
        assert_eq!(
            fs::metadata(&f.ack).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(f.ack.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        let inode = fs::metadata(&f.ack).unwrap().ino();
        f.source = ClaudeWorkersSource::from_data_home(fs::canonicalize(f.temp.path()).unwrap());
        f.tick(200);
        assert_eq!(fs::metadata(&f.ack).unwrap().ino(), inode);
    }

    #[test]
    fn excluded_workers_are_not_read_and_runtime_is_removed() {
        let f = Fixture::new();
        f.call(WORKER, "one", true);
        f.declare(vec![f.root(WORKER, true)]);
        f.tick(100);
        let config = f.temp.path().join("astrocyte.jsonc");
        fs::write(&config,r#"{"poll_interval_secs":30,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{"api_twins":{},"plan_price_overrides":{},"quota_providers":{}},"harness_ingest":{"enabled":true,"exclude":["claude-code-workers"]}}"#).unwrap();
        crate::refresher::reload_config(&f.state, Ok(config)).unwrap();
        f.call(WORKER, "two", true);
        fs::write(&f.roots, b"{").unwrap();
        f.tick(200);
        assert_eq!(f.events().len(), 1);
        assert_eq!(f.runtime(), None);
        assert_eq!(f.acks()["read_through"][0]["at_ms"], 100);
    }
}
