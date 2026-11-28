//! Roots files that thalamus-harness writes to declare its private harness
//! folders, and the containment checks astrocyte applies to every declared path.
//! THALAMUS's harness session contracts share file decoding and path resolution,
//! but allow different folder shapes. File acknowledgements share an atomic writer.

use std::fs;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::Ordering;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Deserialize, Serialize)]
pub(crate) struct Ack {
    pub(crate) version: u64,
    pub(crate) read_through: Vec<ReadThrough>,
}

#[derive(PartialEq, Deserialize, Serialize)]
pub(crate) struct ReadThrough {
    pub(crate) session: String,
    pub(crate) at_ms: i64,
}

pub(crate) fn write_ack(path: &Path, ack: &impl Serialize) -> std::io::Result<()> {
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

pub(crate) fn publish_ack(state: &crate::state::SharedState, source: &str, ack: &Ack) {
    state.bus_publications().acknowledgements(
        source,
        ack.read_through
            .iter()
            .map(|entry| (entry.session.as_str(), entry.at_ms)),
    );
}

pub(crate) fn write_ack_and_publish(
    state: &crate::state::SharedState,
    source: &str,
    path: &Path,
    ack: &Ack,
) -> std::io::Result<()> {
    write_ack(path, ack)?;
    publish_ack(state, source, ack);
    Ok(())
}

/// What one read of a roots file found.
pub(crate) enum RootsFile<T> {
    /// The file does not exist: nothing is declared.
    Absent,
    /// The file names a `version` this binary does not know. Its entries are
    /// not guessed at.
    UnknownVersion,
    /// The file could not be read or decoded. The reason says why; the caller
    /// keeps the last good entry list.
    Unusable(String),
    /// A version 1 file and its entries.
    Declared(Vec<T>),
}

#[derive(Deserialize)]
struct File<T> {
    roots: Vec<T>,
}

/// Read a version 1 roots file. Unknown fields, at the top level or in an
/// entry, are ignored, so either module can add fields without a coordinated
/// release. `label` names the file in reasons.
pub(crate) fn read<T: DeserializeOwned>(path: &Path, label: &str) -> RootsFile<T> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return RootsFile::Absent,
        Err(error) => return RootsFile::Unusable(format!("read {label}: {error}")),
    };
    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(error) => return RootsFile::Unusable(format!("decode {label}: {error}")),
    };
    match value.get("version").and_then(serde_json::Value::as_u64) {
        Some(1) => {}
        Some(_) => return RootsFile::UnknownVersion,
        None => return RootsFile::Unusable(format!("missing or invalid {label} version")),
    }
    match serde_json::from_value::<File<T>>(value) {
        Ok(file) => RootsFile::Declared(file.roots),
        Err(error) => RootsFile::Unusable(format!("decode {label}: {error}")),
    }
}

/// Where a declared path leads.
pub(crate) enum Contained {
    /// The path is accepted. Holds the resolved path.
    Inside(PathBuf),
    /// The path is refused, for the reason given. Other entries still read.
    Refused(String),
    /// Nothing exists at the path: the harness module erased it, or has not
    /// created it yet.
    Gone,
}

/// Contain one declared path. It must be absolute and, after symlinks are
/// resolved, strictly inside `folder`, the canonical folder that holds the
/// roots file. This stops a declaration from pointing astrocyte at arbitrary
/// folders. Without a `folder` (a fast read of paths a full read already
/// resolved), only the absolute-path rule is checked. An error means the path
/// could not be resolved for a reason other than being gone; `label` names
/// the entry kind in reasons.
pub(crate) fn contain(
    folder: Option<&Path>,
    declared: &Path,
    label: &str,
) -> Result<Contained, String> {
    contain_with(folder, declared, label, |folder, canonical| {
        canonical != folder && canonical.starts_with(folder)
    })
}

/// Resolve a declared file itself, rather than trusting the containment of
/// its parent. The file must remain below `folder` and be a regular file after
/// every link has been followed.
pub(crate) fn contain_file(
    folder: &Path,
    declared: &Path,
    label: &str,
) -> Result<Contained, String> {
    contain_typed(folder, declared, label, "regular file", |metadata| {
        metadata.is_file()
    })
}

/// Resolve a declared directory itself, rather than trusting the containment
/// of its parent. Enumeration below the resolved directory never starts on a
/// link that escaped the checked folder.
pub(crate) fn contain_directory(
    folder: &Path,
    declared: &Path,
    label: &str,
) -> Result<Contained, String> {
    contain_typed(folder, declared, label, "directory", |metadata| {
        metadata.is_dir()
    })
}

fn contain_typed(
    folder: &Path,
    declared: &Path,
    label: &str,
    expected: &str,
    accepts: impl FnOnce(&fs::Metadata) -> bool,
) -> Result<Contained, String> {
    let contained = contain(Some(folder), declared, label)?;
    let path = match contained {
        Contained::Inside(path) => path,
        other => return Ok(other),
    };
    match fs::metadata(&path) {
        Ok(metadata) if accepts(&metadata) => Ok(Contained::Inside(path)),
        Ok(_) => Ok(Contained::Refused(format!(
            "refuse {label}: expected {expected} at {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Contained::Gone),
        Err(error) => Err(format!("inspect {label} {}: {error}", path.display())),
    }
}

/// OpenCode also reads runner heads in the harness data folder. Their data
/// folders cannot move because OpenCode and other modules already refer to them.
pub(crate) fn contain_opencode_session(
    folder: &Path,
    declared: &Path,
) -> Result<Contained, String> {
    contain_with(
        Some(folder),
        declared,
        "OpenCode session folder",
        |folder, canonical| {
            if canonical != folder && canonical.starts_with(folder) {
                return true;
            }
            let Some(relative) = folder
                .parent()
                .and_then(|harness| canonical.strip_prefix(harness).ok())
            else {
                return false;
            };
            let mut parts = relative.components();
            matches!(
                (parts.next(), parts.next(), parts.next(), parts.next()),
                (Some(Component::Normal(head)), Some(Component::Normal(data)),
                    Some(Component::Normal(opencode)), None)
                    if head.as_encoded_bytes().strip_prefix(b"launch-head-")
                        .is_some_and(|name| !name.is_empty())
                        && data == "data" && opencode == "opencode"
            )
        },
    )
}

/// The two shared OpenCode 2 servers have fixed data folders beside the sessions folder.
pub(crate) fn contain_opencode_shared(folder: &Path, declared: &Path) -> Result<Contained, String> {
    contain_with(
        Some(folder),
        declared,
        "shared OpenCode store",
        |folder, canonical| {
            folder.parent().is_some_and(|harness| {
                canonical == harness.join("opencode2-server/data/opencode")
                    || canonical == harness.join("opencode2-worker-server/data/opencode")
            })
        },
    )
}

/// A private Claude Code config must resolve to this session's exact folder,
/// not merely to another folder somewhere below the harness data directory.
pub(crate) fn contain_claude_session(
    folder: &Path,
    declared: &Path,
    session: &str,
) -> Result<Contained, String> {
    if session.is_empty() || session == "." || session == ".." || session.contains(['/', '\\']) {
        return Ok(Contained::Refused("invalid Claude Code session id".into()));
    }
    contain_with(
        Some(folder),
        declared,
        "Claude Code session config",
        |folder, canonical| {
            let Ok(relative) = canonical.strip_prefix(folder) else {
                return false;
            };
            let mut parts = relative.components();
            matches!(
                (parts.next(), parts.next(), parts.next(), parts.next()),
                (Some(Component::Normal(sessions)), Some(Component::Normal(id)),
                    Some(Component::Normal(config)), None)
                    if sessions == "sessions" && id == session && config == "config"
            )
        },
    )
}

fn contain_with(
    folder: Option<&Path>,
    declared: &Path,
    label: &str,
    accepts: impl FnOnce(&Path, &Path) -> bool,
) -> Result<Contained, String> {
    if !declared.is_absolute() {
        return Ok(Contained::Refused(format!(
            "refuse relative {label} {}",
            declared.display()
        )));
    }
    let Some(folder) = folder else {
        return Ok(Contained::Inside(declared.to_path_buf()));
    };
    let canonical = match std::fs::canonicalize(declared) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Contained::Gone),
        Err(error) => return Err(format!("resolve {label} {}: {error}", declared.display())),
    };
    if !accepts(folder, &canonical) {
        return Ok(Contained::Refused(format!(
            "refuse {label} outside declared folder: {}",
            declared.display()
        )));
    }
    Ok(Contained::Inside(canonical))
}

/// Pi mints its session id after launch, so only the private folder's shape,
/// not its name, is tied to the declaration.
pub(crate) fn contain_pi_session(folder: &Path, declared: &Path) -> Result<Contained, String> {
    contain_with(
        Some(folder),
        declared,
        "Pi session folder",
        |folder, canonical| {
            let Ok(relative) = canonical.strip_prefix(folder) else {
                return false;
            };
            let mut parts = relative.components();
            matches!((parts.next(), parts.next(), parts.next()),
            (Some(Component::Normal(sessions)), Some(Component::Normal(_)), None)
                if sessions == "pi-sessions")
        },
    )
}

#[cfg(test)]
mod bus_publications_ack_tests {
    use super::*;
    use crate::bus_publications::tests::TestBus;
    use crate::state::test_state;

    #[tokio::test]
    async fn bus_publications_ack_follows_file_success_and_revokes_removed_sessions() {
        let state = test_state();
        let Some(bus) = TestBus::start(&state).await else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ack.json");
        let ack = Ack {
            version: 1,
            read_through: vec![ReadThrough {
                session: "s/_.é".into(),
                at_ms: 123,
            }],
        };
        let before = bus.attempts();
        write_ack_and_publish(&state, "pi-thalamus", &path, &ack).unwrap();
        bus.after(before).await;
        assert_eq!(
            std::fs::read(&path).unwrap(),
            "{\"version\":1,\"read_through\":[{\"session\":\"s/_.é\",\"at_ms\":123}]}\n".as_bytes()
        );
        let key = "thalamus.ack.x70692d7468616c616d7573.x732f5f2ec3a9";
        assert_eq!(
            bus.read_marks.get(key).await.unwrap().unwrap().as_ref(),
            "{\"session\":\"s/_.é\",\"source\":\"pi-thalamus\",\"through_ms\":123}".as_bytes()
        );
        let before = bus.attempts();
        write_ack_and_publish(
            &state,
            "pi-thalamus",
            &path.join("blocked.json"),
            &Ack {
                version: 1,
                read_through: vec![],
            },
        )
        .unwrap_err();
        assert_eq!(bus.attempts(), before);
        assert!(bus.read_marks.get(key).await.unwrap().is_some());
        write_ack_and_publish(
            &state,
            "pi-thalamus",
            &path,
            &Ack {
                version: 1,
                read_through: vec![],
            },
        )
        .unwrap();
        bus.after(before).await;
        assert!(bus.read_marks.get(key).await.unwrap().is_none());
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"{\"version\":1,\"read_through\":[]}\n"
        );
        let oversized = Ack {
            version: 1,
            read_through: vec![ReadThrough {
                session: "s".repeat(256),
                at_ms: 123,
            }],
        };
        let before = bus.attempts();
        write_ack_and_publish(&state, "pi-thalamus", &path, &oversized).unwrap();
        bus.after(before).await;
        let key = format!(
            "thalamus.ack.x70692d7468616c616d7573.{}",
            crate::bus_publications::key_component(&oversized.read_through[0].session)
        );
        assert!(bus.read_marks.get(key).await.unwrap().is_none());
        assert!(state.crash_cause().is_none());
        bus.finish().await;
    }

    #[tokio::test]
    async fn bus_publications_ack_outage_keeps_file_and_module_healthy() {
        let state = test_state();
        let Some(mut bus) = TestBus::start(&state).await else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ack.json");
        bus.server.stop();
        let before = bus.attempts();
        let ack = Ack {
            version: 1,
            read_through: vec![ReadThrough {
                session: "session".into(),
                at_ms: 456,
            }],
        };
        write_ack_and_publish(&state, "claude-code-thalamus", &path, &ack).unwrap();
        bus.after(before).await;
        let file: Ack = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert!(file == ack);
        assert!(state.crash_cause().is_none());
        bus.finish().await;
    }
}
