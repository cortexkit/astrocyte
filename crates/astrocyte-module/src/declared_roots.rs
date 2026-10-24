//! Roots files that thalamus-harness writes to declare its private harness
//! folders, and the containment rule astrocyte applies to every declared path.
//! The Claude workers contract (`docs/claude-workers-contract.md`) and the
//! OpenCode sessions contract (`docs/thalamus-opencode-sessions-contract.md`)
//! share both.

use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Deserialize;

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
    /// The path resolves strictly inside the declaring folder. Holds the
    /// resolved path.
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
    if canonical == folder || !canonical.starts_with(folder) {
        return Ok(Contained::Refused(format!(
            "refuse {label} outside declared folder: {}",
            declared.display()
        )));
    }
    Ok(Contained::Inside(canonical))
}
