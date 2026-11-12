//! THALAMUS's private Claude Code sessions and their post-commit read acknowledgements.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use super::claude_code_source::{self, ClaudeCodeCache, ClaudeCodeDrainOutcome};
use super::declared_roots::{self, Contained, RootsFile};
use super::harness_drain::{self, HarnessSource};
use super::thalamus_payer_join::{WaitQueue, WaitingCall};
use super::SourceDrainError;
use crate::harness_paths::ClaudeSessionPaths;
use crate::state::SharedState;

pub(crate) const SOURCE_KEY: &str = "claude-code-thalamus";

#[derive(Clone, Deserialize)]
struct Root {
    session: String,
    #[serde(rename = "session_ref")]
    _session_ref: String,
    path: PathBuf,
    owner_principal: String,
    #[serde(default, deserialize_with = "deserialize_owner_session")]
    owner_session: Option<String>,
    state: SessionState,
}

fn deserialize_owner_session<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let session = String::deserialize(deserializer)?;
    if session.is_empty() {
        return Err(serde::de::Error::custom(
            "owner_session must be a nonempty string",
        ));
    }
    Ok(Some(session))
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum SessionState {
    Active,
    Done,
}

#[derive(PartialEq, Deserialize, Serialize)]
struct Ack {
    version: u64,
    read_through: Vec<ReadThrough>,
}

#[derive(PartialEq, Deserialize, Serialize)]
struct ReadThrough {
    session: String,
    at_ms: i64,
}

#[derive(Default)]
struct Cache {
    roots: Vec<Root>,
    resumes: HashMap<PathBuf, ClaudeCodeCache>,
    // Resumed reads emit a refused call only once; an empty later read must
    // not authorize deletion of a transcript containing that call.
    blocked: HashSet<String>,
    ack: Option<Ack>,
    payer_wait: WaitQueue,
}

pub(crate) struct ClaudeThalamusSource {
    paths: Result<ClaudeSessionPaths, String>,
    cache: Mutex<Cache>,
}

impl ClaudeThalamusSource {
    pub(crate) fn from_data_home(home: PathBuf) -> Self {
        Self {
            paths: crate::harness_paths::claude_sessions(home),
            cache: Mutex::new(Cache::default()),
        }
    }

    fn drain(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let mut transactions = Vec::new();
        let result = self.drain_staged(state, at, stop, &mut transactions);
        for transaction in transactions.into_iter().rev() {
            transaction.rollback();
        }
        result
    }

    fn drain_staged(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
        transactions: &mut Vec<astrocyte_readers::sessions::claudecode::ReadTransaction>,
    ) -> Result<Option<String>, SourceDrainError> {
        let paths = self
            .paths
            .as_ref()
            .map_err(|e| SourceDrainError::Failed(e.clone()))?;
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "Claude Code session cache lock poisoned".to_string())?;
        let mut problems = Vec::new();
        let registry_valid =
            match declared_roots::read::<Root>(&paths.roots, "Claude Code session roots") {
                RootsFile::Absent => {
                    if !cache.payer_wait.is_empty() {
                        claude_code_source::record_requests_in_lane(
                            state,
                            SOURCE_KEY,
                            &[],
                            &[],
                            at,
                            &mut ClaudeCodeDrainOutcome::default(),
                            &|| stop.load(Ordering::Relaxed),
                            harness_drain::DrainMode::Full,
                            Some((&mut cache.payer_wait, &[])),
                        )?;
                        if !cache.payer_wait.is_empty() {
                            return Ok(None);
                        }
                    }
                    harness_drain::remove_runtime_row(state, SOURCE_KEY)?;
                    *cache = Cache::default();
                    // Prune acknowledgements for sessions the harness removed.
                    if paths.ack.exists() {
                        write_ack(
                            &paths.ack,
                            &Ack {
                                version: 1,
                                read_through: vec![],
                            },
                        )
                        .map_err(|e| format!("write Claude Code session ack: {e}"))?;
                    }
                    return Ok(None);
                }
                RootsFile::UnknownVersion => {
                    harness_drain::ensure_runtime_row(state, SOURCE_KEY)?;
                    return Err("unknown Claude Code session roots version"
                        .to_string()
                        .into());
                }
                RootsFile::Unusable(reason) => {
                    problems.push(reason);
                    false
                }
                RootsFile::Declared(roots) => {
                    cache.roots = roots;
                    true
                }
            };
        harness_drain::ensure_runtime_row(state, SOURCE_KEY)?;
        if cache.ack.is_none() {
            cache.ack = Some(match fs::read(&paths.ack) {
                Ok(bytes) => {
                    let ack: Ack = serde_json::from_slice(&bytes)
                        .map_err(|e| format!("decode Claude Code session ack: {e}"))?;
                    if ack.version != 1 {
                        return Err("unknown Claude Code session ack version".to_string().into());
                    }
                    ack
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ack {
                    version: 1,
                    read_through: vec![],
                },
                Err(e) => return Err(format!("read Claude Code session ack: {e}").into()),
            });
        }
        let folder = fs::canonicalize(paths.roots.parent().expect("roots parent"))
            .map_err(|e| format!("resolve Claude Code session folder: {e}"))?;
        let roots = cache.roots.clone();
        let mut ids = HashMap::new();
        let mut folders = HashMap::new();
        let mut resolved = Vec::new();
        for root in &roots {
            *ids.entry(root.session.as_str()).or_insert(0usize) += 1;
            // Count aliases even when one entry names the wrong session, so an
            // ambiguous owner cannot win merely by appearing first in the file.
            if let Ok(Contained::Inside(path)) =
                declared_roots::contain(Some(&folder), &root.path, "Claude Code session config")
            {
                *folders.entry(path).or_insert(0usize) += 1;
            }
            match declared_roots::contain_claude_session(&folder, &root.path, &root.session) {
                Ok(Contained::Inside(path)) => {
                    resolved.push(Some(path));
                }
                Ok(Contained::Gone) => resolved.push(None),
                Ok(Contained::Refused(reason)) | Err(reason) => {
                    problems.push(reason);
                    resolved.push(None);
                }
            }
        }
        let mut resumes = HashMap::new();
        let mut ready = HashSet::new();
        let mut refs_by_session = HashMap::new();
        let mut blocked = cache.blocked.clone();
        let mut requests = Vec::new();
        let mut set_aside = Vec::new();
        let mut outcome = ClaudeCodeDrainOutcome::default();
        let mut accepted = HashSet::new();
        for (root, canonical) in roots.iter().zip(resolved) {
            if ids[root.session.as_str()] > 1 {
                problems.push(format!(
                    "refuse duplicate Claude Code session {}",
                    root.session
                ));
                continue;
            }
            let Some(canonical) = canonical else { continue };
            if folders.get(&canonical).is_some_and(|count| *count > 1) {
                problems.push(format!(
                    "two Claude Code session entries resolve to one folder: {}",
                    canonical.display()
                ));
                continue;
            }
            if stop.load(Ordering::Relaxed) {
                return Err(SourceDrainError::Stopped);
            }
            accepted.insert(root.session.clone());
            let projects = canonical.join("projects");
            let previous = cache.resumes.get(&canonical).cloned().unwrap_or_default();
            let read = match astrocyte_readers::read_io::metadata(&projects) {
                Ok(_) => claude_code_source::read_projects(
                    &projects,
                    &previous,
                    at,
                    astrocyte_readers::sessions::claudecode::CLAUDE_CODE_QUIET_WINDOW_MS,
                    Some(&root.owner_principal),
                    stop,
                    harness_drain::DrainMode::Full,
                )?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    claude_code_source::ClaudeCodeRead {
                        outcome: ClaudeCodeDrainOutcome::default(),
                        requests: vec![],
                        completed_at_ms: HashMap::new(),
                        set_aside: vec![],
                        cache: ClaudeCodeCache::default(),
                        transactions: Vec::new(),
                    }
                }
                Err(e) => {
                    return Err(format!(
                        "inspect Claude Code session projects {}: {e}",
                        projects.display()
                    )
                    .into())
                }
            };
            transactions.extend(read.transactions);
            if read.outcome.counts.calls_refused > 0 || !read.set_aside.is_empty() {
                blocked.insert(root.session.clone());
            }
            if root.state == SessionState::Done
                && read.outcome.calls_pending == 0
                && !blocked.contains(&root.session)
            {
                ready.insert(root.session.clone());
            }
            outcome.counts.calls_refused += read.outcome.counts.calls_refused;
            refs_by_session.insert(
                root.session.clone(),
                read.requests
                    .iter()
                    .map(|r| r.producer_ref.clone())
                    .collect::<HashSet<_>>(),
            );
            requests.extend(read.requests.into_iter().map(|mut request| {
                let native_session = if let Some(owner_session) = &root.owner_session {
                    request.session = Some(owner_session.clone());
                    Some(root.session.clone())
                } else {
                    None
                };
                WaitingCall {
                    completed_at_ms: read
                        .completed_at_ms
                        .get(&request.producer_ref)
                        .copied()
                        .flatten(),
                    request,
                    native_session,
                }
            }));
            set_aside.extend(read.set_aside);
            resumes.insert(canonical, read.cache);
        }
        if !cache.payer_wait.is_empty() {
            let sessions = roots
                .iter()
                .filter(|root| accepted.contains(&root.session))
                .map(|root| {
                    (
                        root.session.as_str(),
                        root.owner_session.as_deref().unwrap_or(&root.session),
                    )
                })
                .collect();
            cache.payer_wait.update_recorded_sessions(&sessions);
        }
        claude_code_source::record_requests_in_lane(
            state,
            SOURCE_KEY,
            &[],
            &set_aside,
            at,
            &mut outcome,
            &|| stop.load(Ordering::Relaxed),
            harness_drain::DrainMode::Full,
            Some((&mut cache.payer_wait, &requests)),
        )?;
        for transaction in transactions.drain(..) {
            transaction.commit();
        }
        cache.resumes = resumes;
        for (session, refs) in refs_by_session {
            if outcome.quarantined_refs.iter().any(|r| refs.contains(r)) {
                blocked.insert(session);
            }
        }
        blocked.retain(|id| roots.iter().any(|r| &r.session == id));
        cache.blocked = blocked;
        if !cache.blocked.is_empty()
            || outcome.quarantined_redeliveries > 0
            || outcome.counts.calls_refused > 0
        {
            problems.push("Claude Code session calls were quarantined or refused".to_string());
        }
        // An acknowledgement entry tells THALAMUS it may delete that session's
        // folder. An unusable roots file can't add entries, but a session with
        // a call still waiting for its payer row must lose any entry it
        // already has, so its folder isn't deleted before the call is recorded.
        let waiting = cache.payer_wait.pending_sessions();
        if registry_valid || !waiting.is_empty() {
            let mut entries: BTreeMap<_, _> = cache
                .ack
                .as_ref()
                .expect("ack loaded")
                .read_through
                .iter()
                .filter(|r| {
                    (!registry_valid
                        || (accepted.contains(&r.session) && !cache.blocked.contains(&r.session)))
                        && !waiting.contains(&r.session)
                })
                .map(|r| (r.session.clone(), r.at_ms))
                .collect();
            for session in ready {
                if registry_valid
                    && !cache.blocked.contains(&session)
                    && !waiting.contains(&session)
                {
                    entries.entry(session).or_insert(at);
                }
            }
            let ack = Ack {
                version: 1,
                read_through: entries
                    .into_iter()
                    .map(|(session, at_ms)| ReadThrough { session, at_ms })
                    .collect(),
            };
            if cache.ack.as_ref() != Some(&ack) || !paths.ack.exists() {
                write_ack(&paths.ack, &ack)
                    .map_err(|e| format!("write Claude Code session ack: {e}"))?;
                cache.ack = Some(ack);
            }
        }
        Ok((!problems.is_empty()).then(|| problems.join("; ")))
    }
}

impl HarnessSource for ClaudeThalamusSource {
    fn drain_fast(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<(), SourceDrainError> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "Claude Code session cache lock poisoned".to_string())?;
        if cache.payer_wait.is_empty() {
            return Ok(());
        }
        // The parser doesn't revisit a call once it's complete, so queued calls
        // are rechecked here for a newly arrived payer row (who paid, which
        // login and account) without opening transcripts or the roots file.
        claude_code_source::record_requests_in_lane(
            state,
            SOURCE_KEY,
            &[],
            &[],
            at,
            &mut ClaudeCodeDrainOutcome::default(),
            &|| stop.load(Ordering::Relaxed),
            harness_drain::DrainMode::Fast,
            Some((&mut cache.payer_wait, &[])),
        )
    }

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

fn write_ack(path: &Path, ack: &impl Serialize) -> std::io::Result<()> {
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
            ".claude-code-{}-{sequence}.tmp",
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
    include!("claude_thalamus_source_tests.rs");
    use crate::refresher::{
        claude_code_source::ClaudeCodeSource, run_iteration_with_claude_code,
        run_iteration_with_harness,
    };
    use crate::state::test_state;
    use rusqlite::OptionalExtension;
    use serde_json::{json, Value};
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    const SESSION: &str = "01234567-89ab-4cde-8123-456789abcdef";
    const OTHER: &str = "11234567-89ab-4cde-8123-456789abcdef";

    struct Fixture {
        temp: TempDir,
        folder: PathBuf,
        roots: PathBuf,
        ack: PathBuf,
        source: ClaudeThalamusSource,
        state: Arc<SharedState>,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = TempDir::new().unwrap();
            let home = fs::canonicalize(temp.path()).unwrap();
            let folder = home.join("cortexkit/thalamus-harness/claude-code");
            fs::create_dir_all(&folder).unwrap();
            Self {
                roots: folder.join("roots.json"),
                ack: home.join("cortexkit/astrocyte/acks/claude-code.json"),
                source: ClaudeThalamusSource::from_data_home(home),
                temp,
                folder,
                state: Arc::new(test_state()),
            }
        }

        fn config(&self, id: &str) -> PathBuf {
            self.folder.join("sessions").join(id).join("config")
        }

        fn root(&self, id: &str, done: bool) -> Value {
            json!({"session":id,"session_ref":"runner/head","path":self.config(id),
                "owner_principal":"prefrontal-core verbatim", "state":if done {"done"} else {"active"},
                "done_at_ms":if done {Some(50)} else {None}, "future":true})
        }

        fn declare(&self, roots: Vec<Value>) {
            fs::write(
                &self.roots,
                json!({"version":1,"roots":roots,"future":true}).to_string(),
            )
            .unwrap();
        }

        fn call(&self, id: &str, request: &str, complete: bool) {
            write_call(&self.config(id), request, complete);
        }

        fn tick(&self, at: i64) {
            struct Clocked<'a>(&'a ClaudeThalamusSource, i64);
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
        let folder = root.join("projects/encoded-cwd/session/subagents");
        fs::create_dir_all(&folder).unwrap();
        let record = json!({"type":"assistant","timestamp":"2026-09-01T00:00:01.000Z","requestId":request,
            "sessionId":SESSION,"cwd":"/work","message":{"id":format!("msg_{request}"),"model":"claude-opus-5",
            "stop_reason":if complete {Some("end_turn")} else {None},"usage":{"input_tokens":3,"output_tokens":5}}});
        fs::write(
            folder.join(format!("{request}.jsonl")),
            format!("{record}\n"),
        )
        .unwrap();
    }

    #[test]
    fn valid_session_is_read_acknowledged_and_keeps_identity_and_owner() {
        let f = Fixture::new();
        f.call(SESSION, "one", true);
        f.declare(vec![f.root(SESSION, true)]);
        f.tick(100);
        assert_eq!(
            f.events(),
            vec![(
                "one".into(),
                "claude-code".into(),
                Some("prefrontal-core verbatim".into())
            )]
        );
        assert_eq!(
            f.acks(),
            json!({"version":1,"read_through":[{"session":SESSION,"at_ms":100}]})
        );
        assert_eq!(f.runtime(), Some((Some(100), false)));
        assert!(
            f.config(SESSION).exists(),
            "astrocyte never deletes harness folders"
        );
    }

    fn refuses_path(path: impl FnOnce(&Fixture) -> PathBuf) {
        let f = Fixture::new();
        let bad = path(&f);
        write_call(&bad, "refused", true);
        f.call(OTHER, "good", true);
        let mut root = f.root(SESSION, true);
        root["path"] = json!(bad);
        f.declare(vec![root, f.root(OTHER, true)]);
        f.tick(100);
        assert_eq!(
            f.events().iter().map(|e| &e.0).collect::<Vec<_>>(),
            vec!["good"]
        );
        assert_eq!(f.runtime(), Some((Some(100), true)));
        assert_eq!(
            f.acks()["read_through"],
            json!([{"session":OTHER,"at_ms":100}])
        );
    }

    #[test]
    fn refuses_relative_path() {
        let f = Fixture::new();
        f.call(SESSION, "refused", true);
        let mut root = f.root(SESSION, true);
        root["path"] = json!("sessions/session/config");
        f.declare(vec![root]);
        f.tick(100);
        assert!(f.events().is_empty());
        assert_eq!(f.runtime(), Some((Some(100), true)));
        assert_eq!(f.acks()["read_through"], json!([]));
    }

    #[test]
    fn refuses_outside_path() {
        refuses_path(|f| f.temp.path().join("outside"));
    }
    #[test]
    fn refuses_harness_folder_itself() {
        refuses_path(|f| f.folder.clone());
    }
    #[test]
    fn refuses_session_folder_without_config() {
        refuses_path(|f| f.folder.join("sessions").join(SESSION));
    }
    #[test]
    fn refuses_config_descendant() {
        refuses_path(|f| f.config(SESSION).join("nested"));
    }
    #[test]
    fn refuses_mismatched_session_component() {
        refuses_path(|f| f.config("another-session"));
    }
    #[test]
    fn refuses_arbitrary_harness_subfolder() {
        refuses_path(|f| f.folder.join("misc/config"));
    }

    #[test]
    fn refuses_invalid_session_ids_even_if_folder_is_absent() {
        for id in ["", ".", "..", "../escape", "a/b", "a\\b"] {
            let f = Fixture::new();
            f.declare(vec![f.root(id, true)]);
            f.tick(100);
            assert!(f.events().is_empty());
            assert_eq!(f.runtime(), Some((Some(100), true)), "id {id:?}");
            assert_eq!(f.acks()["read_through"], json!([]));
        }
    }

    #[test]
    fn refuses_duplicate_session_entries() {
        let f = Fixture::new();
        f.call(SESSION, "ambiguous", true);
        f.declare(vec![f.root(SESSION, true), f.root(SESSION, true)]);
        f.tick(100);
        assert!(f.events().is_empty());
        assert_eq!(f.runtime(), Some((Some(100), true)));
        assert_eq!(f.acks()["read_through"], json!([]));
    }

    #[cfg(unix)]
    #[test]
    fn refuses_escaping_symlink() {
        refuses_path(|f| {
            let outside = f.temp.path().join("outside");
            fs::create_dir_all(&outside).unwrap();
            let config = f.config(SESSION);
            fs::create_dir_all(config.parent().unwrap()).unwrap();
            std::os::unix::fs::symlink(outside, &config).unwrap();
            config
        });
    }

    #[cfg(unix)]
    #[test]
    fn refuses_aliased_session_entries() {
        let f = Fixture::new();
        f.call(SESSION, "ambiguous", true);
        let alias = f.folder.join("alias");
        std::os::unix::fs::symlink(f.config(SESSION), &alias).unwrap();
        let mut root = f.root(SESSION, true);
        root["path"] = json!(alias);
        f.declare(vec![f.root(SESSION, true), root]);
        f.tick(100);
        assert!(f.events().is_empty());
        assert_eq!(f.runtime(), Some((Some(100), true)));
        assert_eq!(f.acks()["read_through"], json!([]));
    }

    #[cfg(unix)]
    #[test]
    fn refuses_alias_with_different_session_and_owner() {
        let f = Fixture::new();
        f.call(SESSION, "ambiguous", true);
        let alias = f.config(OTHER);
        fs::create_dir_all(alias.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(f.config(SESSION), alias).unwrap();
        let mut root = f.root(OTHER, true);
        root["owner_principal"] = json!("another owner");
        f.declare(vec![f.root(SESSION, true), root]);
        f.tick(100);
        assert!(f.events().is_empty());
        assert_eq!(f.runtime(), Some((Some(100), true)));
        assert_eq!(f.acks()["read_through"], json!([]));
    }

    #[test]
    fn active_heads_are_read_each_tick_but_not_acknowledged() {
        let f = Fixture::new();
        f.call(SESSION, "one", true);
        f.declare(vec![f.root(SESSION, false)]);
        f.tick(100);
        f.call(SESSION, "two", true);
        f.tick(200);
        assert_eq!(f.events().len(), 2);
        assert_eq!(f.acks()["read_through"], json!([]));
    }

    #[test]
    fn done_session_waits_for_quiet_window() {
        let f = Fixture::new();
        f.call(SESSION, "unfinished", false);
        f.declare(vec![f.root(SESSION, true)]);
        f.tick(100);
        f.tick(600_099);
        assert!(f.events().is_empty());
        assert_eq!(f.acks()["read_through"], json!([]));
        f.tick(600_100);
        assert_eq!(f.events().len(), 1);
        assert_eq!(
            f.acks()["read_through"],
            json!([{"session":SESSION,"at_ms":600_100}])
        );
    }

    #[test]
    fn acknowledgement_follows_commit_and_failed_commit_retries_calls() {
        let f = Fixture::new();
        f.call(SESSION, "one", true);
        f.declare(vec![f.root(SESSION, true)]);
        f.state.with_conn(|conn| conn.execute_batch("CREATE TRIGGER refuse_commit BEFORE UPDATE OF last_success_ms ON source_runtime BEGIN SELECT RAISE(ABORT,'refuse commit'); END;").map_err(|e| e.to_string())).unwrap();
        f.tick(100);
        assert!(f.events().is_empty());
        assert!(!f.ack.exists());
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
            json!([{"session":SESSION,"at_ms":200}])
        );
    }

    #[test]
    fn history_scaling_combined_failure_restores_both_root_histories() {
        let f = Fixture::new();
        f.call(SESSION, "first", true);
        f.call(OTHER, "second", true);
        f.declare(vec![f.root(SESSION, false), f.root(OTHER, false)]);
        f.tick(100);
        let snapshot = || {
            let cache = f.source.cache.lock().unwrap();
            cache
                .resumes
                .iter()
                .map(|(root, cache)| (root.clone(), cache.state_snapshot()))
                .collect::<BTreeMap<_, _>>()
        };
        let before = snapshot();
        for (session, old, new) in [(SESSION, "first", "third"), (OTHER, "second", "fourth")] {
            use std::io::Write;
            let path = f.config(session).join(format!(
                "projects/encoded-cwd/session/subagents/{old}.jsonl"
            ));
            let old_line = fs::read_to_string(&path).unwrap();
            let new_line = old_line.replace(old, new);
            let mut file = fs::OpenOptions::new().append(true).open(path).unwrap();
            write!(file, "{old_line}{new_line}").unwrap();
        }
        f.state.with_conn(|conn| conn.execute_batch("CREATE TRIGGER fail_history BEFORE INSERT ON usage_event BEGIN SELECT RAISE(ABORT,'test failure'); END;").map_err(|e| e.to_string())).unwrap();
        assert!(f
            .source
            .drain(&f.state, 200, &AtomicBool::new(false))
            .is_err());
        assert_eq!(snapshot(), before);
        assert_eq!(f.events().len(), 2);
        f.state
            .with_conn(|conn| {
                conn.execute_batch("DROP TRIGGER fail_history")
                    .map_err(|e| e.to_string())
            })
            .unwrap();
        f.tick(300);
        assert_eq!(f.events().len(), 4);
        f.tick(400);
        assert_eq!(f.events().len(), 4);
    }

    #[test]
    fn moved_transcript_is_already_recorded_and_first_labels_stand() {
        let f = Fixture::new();
        let original = f.temp.path().join("original");
        write_call(&original, "moved", true);
        run_iteration_with_claude_code(
            &f.state,
            &[],
            &ClaudeCodeSource::at(original.join("projects")),
            &AtomicBool::new(false),
        );
        assert_eq!(
            f.events(),
            vec![("moved".into(), "claude-code".into(), None)]
        );
        fs::create_dir_all(f.config(SESSION).parent().unwrap()).unwrap();
        fs::rename(original, f.config(SESSION)).unwrap();
        f.call(SESSION, "new", true);
        f.declare(vec![f.root(SESSION, true)]);
        f.tick(100);
        assert_eq!(
            f.events(),
            vec![
                ("moved".into(), "claude-code".into(), None),
                (
                    "new".into(),
                    "claude-code".into(),
                    Some("prefrontal-core verbatim".into())
                )
            ]
        );
        f.state
            .with_conn(|conn| {
                let quarantined: i64 = conn
                    .query_row("SELECT COUNT(*) FROM rejected_delivery", [], |r| r.get(0))
                    .unwrap();
                assert_eq!(quarantined, 0);
                Ok(())
            })
            .unwrap();
        assert_eq!(f.runtime(), Some((Some(100), false)));
        assert_eq!(
            f.acks()["read_through"],
            json!([{"session":SESSION,"at_ms":100}])
        );
    }

    #[test]
    fn source_can_be_excluded_through_harness_ingest() {
        let f = Fixture::new();
        assert!(crate::config::INGEST_SOURCES.contains(&SOURCE_KEY));
        f.call(SESSION, "one", true);
        f.declare(vec![f.root(SESSION, false)]);
        f.tick(100);
        let config = f.temp.path().join("astrocyte.jsonc");
        fs::write(&config, json!({"poll_interval_secs":30,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{"api_twins":{},"plan_price_overrides":{},"quota_providers":{}},"harness_ingest":{"enabled":true,"exclude":[SOURCE_KEY]}}).to_string()).unwrap();
        crate::refresher::reload_config(&f.state, Ok(config)).unwrap();
        f.call(SESSION, "two", true);
        fs::write(&f.roots, b"{").unwrap();
        f.tick(200);
        assert_eq!(f.events().len(), 1);
        assert_eq!(f.runtime(), None);
        assert_eq!(f.acks()["read_through"], json!([]));
    }

    #[test]
    fn absent_roots_file_leaves_health_untouched() {
        let f = Fixture::new();
        f.tick(100);
        assert_eq!(f.runtime(), None);
        assert!(!f.ack.exists());
        assert!(f.events().is_empty());
    }

    #[test]
    fn malformed_registry_keeps_last_roots_but_cannot_acknowledge() {
        let f = Fixture::new();
        f.call(SESSION, "one", true);
        f.declare(vec![f.root(SESSION, true)]);
        f.tick(100);
        f.call(SESSION, "two", true);
        fs::remove_file(&f.ack).unwrap();
        fs::write(&f.roots, b"{").unwrap();
        f.tick(200);
        assert_eq!(f.events().len(), 2);
        assert!(!f.ack.exists());
        assert_eq!(f.runtime(), Some((Some(200), true)));
    }

    #[test]
    fn unknown_version_degrades_without_reading_roots() {
        let f = Fixture::new();
        f.call(SESSION, "one", true);
        fs::write(
            &f.roots,
            json!({"version":2,"roots":[f.root(SESSION, true)]}).to_string(),
        )
        .unwrap();
        f.tick(100);
        assert!(f.events().is_empty());
        assert!(!f.ack.exists());
        assert_eq!(f.runtime(), Some((None, true)));
    }

    #[test]
    fn blocked_calls_do_not_become_acknowledgeable_on_resume() {
        let f = Fixture::new();
        f.call(SESSION, "bad", true);
        let path = f
            .config(SESSION)
            .join("projects/encoded-cwd/session/subagents/bad.jsonl");
        let mut record: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        record["message"]["usage"]["output_tokens"] = json!(1_u64 << 63);
        fs::write(path, format!("{record}\n")).unwrap();
        f.declare(vec![f.root(SESSION, true)]);
        f.tick(100);
        f.tick(200);
        assert!(f.events().is_empty());
        assert_eq!(f.acks()["read_through"], json!([]));
    }
}
