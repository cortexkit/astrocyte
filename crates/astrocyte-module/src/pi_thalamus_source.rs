//! THALAMUS's declared Pi sessions and post-commit deletion acknowledgements.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use astrocyte_readers::sessions::pi::{parse_pi_file_resumable, PiPrefixCheck};
use serde::Deserialize;

use super::declared_roots::{self, write_ack, Ack, Contained, ReadThrough, RootsFile};
use super::harness_drain::{self, HarnessSource};
use super::pi_source::{self, PiDrainOutcome};
use super::thalamus_payer_join::{WaitQueue, WaitingCall};
use super::SourceDrainError;
use crate::harness_paths::ClaudeSessionPaths;
use crate::state::SharedState;

pub(crate) const SOURCE_KEY: &str = "pi-thalamus";

#[derive(Clone, Deserialize)]
struct Root {
    path: PathBuf,
    #[serde(deserialize_with = "Option::<String>::deserialize")]
    session: Option<String>,
    owner_session: String,
    owner_principal: String,
    state: SessionState,
    done_at_ms: Option<i64>,
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum SessionState {
    Active,
    Done,
}

#[derive(Default)]
struct Cache {
    roots: Vec<Root>,
    ack: Option<Ack>,
    payer_wait: WaitQueue,
}

pub(crate) struct PiThalamusSource {
    paths: Result<ClaudeSessionPaths, String>,
    normal_root: Result<PathBuf, String>,
    cache: Mutex<Cache>,
}

impl PiThalamusSource {
    pub(crate) fn from_data_home(home: PathBuf) -> Self {
        Self {
            paths: crate::harness_paths::pi_sessions(home),
            normal_root: crate::harness_paths::pi(&cortexkit_paths::SystemEnv)
                .map_err(|e| e.to_string()),
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
            .map_err(|_| "Pi session cache lock poisoned".to_string())?;
        let mut problems = Vec::new();
        let valid = match declared_roots::read::<Root>(&paths.roots, "Pi session roots") {
            RootsFile::Absent => {
                if !cache.payer_wait.is_empty() {
                    cache.payer_wait = pi_source::record_calls(
                        state,
                        SOURCE_KEY,
                        &[],
                        &[],
                        at,
                        false,
                        &mut PiDrainOutcome::default(),
                        &|| stop.load(Ordering::Relaxed),
                        &cache.payer_wait,
                    )?;
                    state.set_payer_waiting(SOURCE_KEY, cache.payer_wait.len());
                    if !cache.payer_wait.is_empty() {
                        return Ok(None);
                    }
                }
                harness_drain::remove_runtime_row(state, SOURCE_KEY)?;
                *cache = Cache::default();
                if paths.ack.exists() {
                    write_ack(
                        &paths.ack,
                        &Ack {
                            version: 1,
                            read_through: vec![],
                        },
                    )
                    .map_err(|e| format!("write Pi session ack: {e}"))?;
                }
                return Ok(None);
            }
            RootsFile::UnknownVersion => {
                harness_drain::ensure_runtime_row(state, SOURCE_KEY)?;
                return Err("unknown Pi session roots version".to_string().into());
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
                        .map_err(|e| format!("decode Pi session ack: {e}"))?;
                    if ack.version != 1 {
                        return Err("unknown Pi session ack version".to_string().into());
                    }
                    ack
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ack {
                    version: 1,
                    read_through: vec![],
                },
                Err(e) => return Err(format!("read Pi session ack: {e}").into()),
            });
        }
        let folder = fs::canonicalize(
            paths
                .roots
                .parent()
                .expect("roots parent")
                .parent()
                .expect("harness folder"),
        )
        .map_err(|e| format!("resolve Pi harness folder: {e}"))?;
        let roots = cache.roots.clone();
        let normal_root = self
            .normal_root
            .as_ref()
            .map_err(|e| SourceDrainError::Failed(e.clone()))?;
        let normal_root = match fs::canonicalize(normal_root) {
            Ok(path) => Some(path),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(format!("resolve normal Pi session root: {e}").into()),
        };
        let mut owners = HashMap::new();
        let mut folders = HashMap::new();
        let mut resolved = Vec::new();
        for root in &roots {
            *owners.entry(root.owner_session.as_str()).or_insert(0usize) += 1;
            // Count canonical aliases before validating the folder's shape, so
            // an invalid alias cannot make another declaration win ownership.
            if let Ok(path) = fs::canonicalize(&root.path) {
                *folders.entry(path).or_insert(0usize) += 1;
            }
            resolved.push(declared_roots::contain_pi_session(&folder, &root.path));
        }
        let mut calls = Vec::new();
        let mut ready = HashSet::new();
        let mut accepted = HashSet::new();
        let mut set_aside = Vec::new();
        for (root, canonical) in roots.iter().zip(resolved) {
            let refuse = if root.owner_session.is_empty() {
                Some("empty owner_session")
            } else if owners[root.owner_session.as_str()] > 1 {
                Some("duplicate owner_session")
            } else if root.state == SessionState::Done && root.done_at_ms.is_none() {
                Some("done entry lacks done_at_ms")
            } else {
                None
            };
            if let Some(reason) = refuse {
                problems.push(format!(
                    "refuse Pi folder {}: {reason}",
                    root.path.display()
                ));
                continue;
            }
            let canonical = match canonical {
                Ok(Contained::Inside(path)) => path,
                Ok(Contained::Gone) => continue,
                Ok(Contained::Refused(reason)) | Err(reason) => {
                    problems.push(reason);
                    continue;
                }
            };
            if normal_root
                .as_ref()
                .is_some_and(|normal| canonical.starts_with(normal))
            {
                problems.push(format!(
                    "refuse Pi folder {} inside Pi's own session root",
                    root.path.display()
                ));
                continue;
            }
            if folders.get(&canonical).is_some_and(|count| *count > 1) {
                problems.push(format!(
                    "refuse Pi folder {}: two entries resolve to one folder",
                    root.path.display()
                ));
                continue;
            }
            let mut folder_calls = Vec::new();
            let mut folder_problems = Vec::new();
            let files = harness_drain::jsonl_files(&canonical)
                .map_err(|e| format!("read Pi folder {}: {e}", root.path.display()))?;
            for path in files {
                if stop.load(Ordering::Relaxed) {
                    return Err(SourceDrainError::Stopped);
                }
                let parsed = parse_pi_file_resumable(&path, None, PiPrefixCheck::Full);
                let header = parsed.resume.as_ref().and_then(|point| point.session_id());
                if root
                    .session
                    .as_deref()
                    .is_some_and(|id| header.is_some_and(|native| native != id))
                {
                    folder_problems.push(format!("session header mismatch in {}", path.display()));
                    continue;
                }
                if parsed.resume.is_none() || parsed.skipped_lines > 0 {
                    folder_problems.push(format!(
                        "unreadable or rejected Pi records in {}",
                        path.display()
                    ));
                }
                for (offset, reason) in parsed.set_aside {
                    set_aside.push(super::SetAsideRecord {
                        locator: format!("{path:?}:byte:{offset}"),
                        reason,
                    });
                }
                for record in parsed.messages {
                    let completed_at_ms = record.occurred_at_ms;
                    let native_session = record.message.session_id.clone();
                    match pi_source::map_message(record) {
                        Ok(mut request) => {
                            request.session = Some(root.owner_session.clone());
                            request.work_class = Some(root.owner_principal.clone());
                            folder_calls.push(WaitingCall {
                                request,
                                completed_at_ms,
                                native_session: Some(native_session),
                            });
                        }
                        Err(reason) => folder_problems.push(reason),
                    }
                }
            }
            // A header disagreement refuses the entire folder, including files
            // parsed before the disagreement. Never guess which session paid.
            if !folder_problems.is_empty() {
                problems.push(format!(
                    "refuse Pi folder {}: {}",
                    root.path.display(),
                    folder_problems.join("; ")
                ));
                continue;
            }
            accepted.insert(root.owner_session.clone());
            if root.state == SessionState::Done && root.done_at_ms.is_some_and(|done| at >= done) {
                ready.insert(root.owner_session.clone());
            }
            calls.extend(folder_calls);
        }
        let mut queue = cache.payer_wait.clone();
        queue.retain_recorded_sessions(&accepted);
        let mut outcome = PiDrainOutcome::default();
        let next = pi_source::record_calls(
            state,
            SOURCE_KEY,
            &calls,
            &set_aside,
            at,
            true,
            &mut outcome,
            &|| stop.load(Ordering::Relaxed),
            &queue,
        )?;
        cache.payer_wait = next;
        state.set_payer_waiting(SOURCE_KEY, cache.payer_wait.len());
        if outcome.quarantined_redeliveries > 0 {
            problems.push("Pi session calls were quarantined".into());
            ready.clear();
        }
        let waiting = cache.payer_wait.pending_recorded_sessions();
        ready.retain(|owner| !waiting.contains(owner));
        if valid {
            let previous = &cache.ack.as_ref().expect("ack loaded").read_through;
            let entries: BTreeMap<_, _> = ready
                .into_iter()
                .map(|session| {
                    let at_ms = previous
                        .iter()
                        .find(|r| r.session == session)
                        .map_or(at, |r| {
                            r.at_ms.max(
                                roots
                                    .iter()
                                    .find(|root| root.owner_session == session)
                                    .and_then(|r| r.done_at_ms)
                                    .unwrap_or(at),
                            )
                        });
                    (session, at_ms)
                })
                .collect();
            let ack = Ack {
                version: 1,
                read_through: entries
                    .into_iter()
                    .map(|(session, at_ms)| ReadThrough { session, at_ms })
                    .collect(),
            };
            if cache.ack.as_ref() != Some(&ack) || !paths.ack.exists() {
                write_ack(&paths.ack, &ack).map_err(|e| format!("write Pi session ack: {e}"))?;
                cache.ack = Some(ack);
            }
        }
        Ok((!problems.is_empty()).then(|| problems.join("; ")))
    }
}

impl HarnessSource for PiThalamusSource {
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

    fn drain_fast(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<(), SourceDrainError> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "Pi session cache lock poisoned".to_string())?;
        if cache.payer_wait.is_empty() {
            return Ok(());
        }
        cache.payer_wait = pi_source::record_calls(
            state,
            SOURCE_KEY,
            &[],
            &[],
            at,
            false,
            &mut PiDrainOutcome::default(),
            &|| stop.load(Ordering::Relaxed),
            &cache.payer_wait,
        )?;
        state.set_payer_waiting(SOURCE_KEY, cache.payer_wait.len());
        Ok(())
    }
}

#[cfg(test)]
#[path = "pi_thalamus_source_tests.rs"]
mod tests;
