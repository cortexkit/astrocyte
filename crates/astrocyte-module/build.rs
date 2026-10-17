//! Stamp source and dependency provenance into the module binary.
//!
//! Git metadata is read directly so builds do not depend on a `git` executable.
//! When tree cleanliness cannot be established from repository files alone, the
//! build records that it was not derived rather than claiming a clean checkout.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

#[derive(Debug, PartialEq, Eq)]
struct HeadResolution {
    revision: String,
    head_path: PathBuf,
    ref_path: Option<PathBuf>,
}

fn main() {
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"));
    let lock_path = workspace_lock_path(&manifest_dir).expect("workspace Cargo.lock must exist");
    let dot_git = find_dot_git(&manifest_dir);
    let head = dot_git.as_deref().and_then(resolve_head);

    if let Some(git_dir) = dot_git.as_deref().and_then(resolve_git_dir) {
        println!("cargo:rerun-if-changed={}", git_dir.join("HEAD").display());
    }
    if let Some(ref_path) = head
        .as_ref()
        .and_then(|resolution| resolution.ref_path.as_deref())
    {
        println!("cargo:rerun-if-changed={}", ref_path.display());
    }
    println!("cargo:rerun-if-changed={}", lock_path.display());
    println!("cargo:rerun-if-changed=build.rs");

    println!(
        "cargo:rustc-env=CK_ASTROCYTE_BUILD_GIT_SHA={}",
        head.as_ref()
            .map_or("", |resolution| resolution.revision.as_str())
    );
    // Cleanliness needs git's index and ignore rules, which repository files
    // alone cannot reproduce; ask the git binary when one is present and
    // otherwise say so. A stamp that guessed `clean` would let a build from a
    // dirty tree claim a commit it does not match. Edits to tracked sources
    // rebuild this crate but do not by themselves rerun a build script, so the
    // paths that can flip the answer are declared as inputs below.
    let tree_state = match dot_git.as_deref() {
        None => "no_git_dir",
        Some(_) => {
            let root = workspace_root(&lock_path);
            for path in tree_state_inputs(&root).unwrap_or_else(|| vec![root.clone()]) {
                println!("cargo:rerun-if-changed={}", path.display());
            }
            if let Some(git_dir) = dot_git.as_deref().and_then(resolve_git_dir) {
                println!("cargo:rerun-if-changed={}", git_dir.join("index").display());
            }
            match git_tree_state(&root) {
                Some(true) => "clean",
                Some(false) => "dirty",
                None => "unknown",
            }
        }
    };
    println!("cargo:rustc-env=CK_ASTROCYTE_BUILD_TREE_STATE={tree_state}");
    println!(
        "cargo:rustc-env=CK_ASTROCYTE_BUILD_LOCK_DIGEST={}",
        lock_digest(&lock_path).expect("workspace Cargo.lock must be readable")
    );
}

fn workspace_root(lock_path: &Path) -> PathBuf {
    lock_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// `Some(true)` for a clean tree, `Some(false)` for a dirty one, `None` when
/// no git binary answered. Untracked files are ignored: they cannot be in the
/// build, so they do not make the stamped commit a false claim.
///
/// `--no-optional-locks` stops `git status` from refreshing and rewriting the
/// index. The index is one of this script's declared inputs, so a rewrite
/// would mark the crate dirty again and rebuild it on the next cargo run in
/// every fresh worktree.
fn git_tree_state(root: &Path) -> Option<bool> {
    let output = std::process::Command::new("git")
        .args([
            "--no-optional-locks",
            "status",
            "--porcelain",
            "--untracked-files=no",
        ])
        .current_dir(root)
        .output()
        .ok()?;
    output.status.success().then_some(output.stdout.is_empty())
}

/// Every tracked path affects the whole-checkout cleanliness stamp, including
/// documentation and scripts. The index watcher covers additions and removals.
fn tree_state_inputs(root: &Path) -> Option<Vec<PathBuf>> {
    let output = std::process::Command::new("git")
        .args(["--no-optional-locks", "ls-files", "-z"])
        .current_dir(root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let paths = std::str::from_utf8(&output.stdout).ok()?;
    Some(
        paths
            .split('\0')
            .filter(|path| !path.is_empty())
            .map(|path| root.join(path))
            .collect(),
    )
}

fn workspace_lock_path(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .map(|directory| directory.join("Cargo.lock"))
        .find(|candidate| candidate.is_file())
}

fn find_dot_git(start: &Path) -> Option<PathBuf> {
    let lock = workspace_lock_path(start)?;
    let root = workspace_root(&lock);
    start
        .ancestors()
        .take_while(|directory| directory.starts_with(&root))
        .map(|directory| directory.join(".git"))
        .find(|candidate| candidate.exists())
}

fn resolve_git_dir(dot_git: &Path) -> Option<PathBuf> {
    if dot_git.is_dir() {
        return Some(dot_git.to_path_buf());
    }
    let marker = std::fs::read_to_string(dot_git).ok()?;
    let target = marker.trim().strip_prefix("gitdir:")?.trim();
    let target = PathBuf::from(target);
    if target.is_absolute() {
        Some(target)
    } else {
        Some(dot_git.parent()?.join(target))
    }
}

fn common_git_dir(git_dir: &Path) -> PathBuf {
    let Some(common) = std::fs::read_to_string(git_dir.join("commondir"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return git_dir.to_path_buf();
    };
    let common = PathBuf::from(common);
    let candidate = if common.is_absolute() {
        common
    } else {
        git_dir.join(common)
    };
    candidate.canonicalize().unwrap_or(candidate)
}

/// Resolve HEAD from a `.git` directory or worktree `gitdir:` file.
fn resolve_head(dot_git: &Path) -> Option<HeadResolution> {
    let git_dir = resolve_git_dir(dot_git)?;
    let head_path = git_dir.join("HEAD");
    let head = std::fs::read_to_string(&head_path).ok()?;
    let head = head.trim();

    let Some(reference) = head.strip_prefix("ref: ").map(str::trim) else {
        return Some(HeadResolution {
            revision: canonical_sha(head)?,
            head_path,
            ref_path: None,
        });
    };

    let common_dir = common_git_dir(&git_dir);
    for ref_path in [git_dir.join(reference), common_dir.join(reference)] {
        if let Some(revision) = read_sha(&ref_path) {
            return Some(HeadResolution {
                revision,
                head_path,
                ref_path: Some(ref_path),
            });
        }
    }

    for packed_path in [git_dir.join("packed-refs"), common_dir.join("packed-refs")] {
        if let Some(revision) = packed_ref(&packed_path, reference) {
            return Some(HeadResolution {
                revision,
                head_path,
                ref_path: Some(packed_path),
            });
        }
    }
    None
}

fn packed_ref(path: &Path, reference: &str) -> Option<String> {
    let packed = std::fs::read_to_string(path).ok()?;
    packed.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let revision = fields.next()?;
        let name = fields.next()?;
        (name == reference)
            .then(|| canonical_sha(revision))
            .flatten()
    })
}

fn read_sha(path: &Path) -> Option<String> {
    canonical_sha(std::fs::read_to_string(path).ok()?.trim())
}

fn canonical_sha(value: &str) -> Option<String> {
    (value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()))
    .then(|| value.to_string())
}

fn lock_digest(lock_path: &Path) -> Option<String> {
    let bytes = std::fs::read(lock_path).ok()?;
    Some(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    const LOOSE_SHA: &str = "1111111111111111111111111111111111111111";
    const PACKED_SHA: &str = "2222222222222222222222222222222222222222";
    const WORKTREE_SHA: &str = "3333333333333333333333333333333333333333";

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static SEQUENCE: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "astrocyte-build-{tag}-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn exported_workspace_does_not_stamp_the_parent_repository() {
        let parent = TempDir::new("exported");
        let dot_git = parent.0.join(".git");
        std::fs::create_dir_all(&dot_git).unwrap();
        std::fs::write(dot_git.join("HEAD"), LOOSE_SHA).unwrap();
        let workspace = parent.0.join("exported");
        let manifest = workspace.join("crates/module");
        std::fs::create_dir_all(&manifest).unwrap();
        std::fs::write(workspace.join("Cargo.lock"), "exported lock").unwrap();

        let stamp = find_dot_git(&manifest).as_deref().and_then(resolve_head);
        assert_eq!(stamp, None, "an exported workspace has no source revision");
        assert_eq!(find_dot_git(&manifest), None);
    }

    #[test]
    fn cleanliness_inputs_include_tracked_files_outside_crates() {
        let root = TempDir::new("inputs");
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .current_dir(&root.0)
                .args(args)
                .output()
                .expect("git is needed to test whole-checkout provenance");
            assert!(output.status.success(), "{output:?}");
        };
        std::fs::create_dir_all(root.0.join("crates/module")).unwrap();
        std::fs::create_dir_all(root.0.join("docs")).unwrap();
        for path in ["Cargo.toml", "crates/module/lib.rs", "docs/guide.md"] {
            std::fs::write(root.0.join(path), "original\n").unwrap();
        }
        git(&["init"]);
        git(&["add", "."]);
        git(&[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "fixture",
        ]);
        assert_eq!(git_tree_state(&root.0), Some(true));
        let inputs = tree_state_inputs(&root.0).unwrap();
        std::fs::write(root.0.join("docs/guide.md"), "edited\n").unwrap();
        assert_eq!(git_tree_state(&root.0), Some(false));
        assert!(
            inputs.contains(&root.0.join("docs/guide.md")),
            "a tracked documentation edit must rerun the cleanliness stamp: {inputs:?}"
        );
        assert!(inputs.contains(&root.0.join("Cargo.toml")));
        assert!(inputs.contains(&root.0.join("crates/module/lib.rs")));
    }

    #[test]
    fn resolve_head_reads_loose_ref() {
        let root = TempDir::new("loose");
        let dot_git = root.0.join(".git");
        std::fs::create_dir_all(dot_git.join("refs/heads")).unwrap();
        std::fs::write(dot_git.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        std::fs::write(dot_git.join("refs/heads/main"), format!("{LOOSE_SHA}\n")).unwrap();

        let resolved = resolve_head(&dot_git).unwrap();
        assert_eq!(resolved.revision, LOOSE_SHA);
        assert_eq!(resolved.ref_path, Some(dot_git.join("refs/heads/main")));
    }

    #[test]
    fn resolve_head_reads_packed_refs() {
        let root = TempDir::new("packed");
        let dot_git = root.0.join(".git");
        std::fs::create_dir_all(&dot_git).unwrap();
        std::fs::write(dot_git.join("HEAD"), "ref: refs/heads/release\n").unwrap();
        std::fs::write(
            dot_git.join("packed-refs"),
            format!("# pack-refs with: peeled fully-peeled\n{PACKED_SHA} refs/heads/release\n"),
        )
        .unwrap();

        let resolved = resolve_head(&dot_git).unwrap();
        assert_eq!(resolved.revision, PACKED_SHA);
        assert_eq!(resolved.ref_path, Some(dot_git.join("packed-refs")));
    }

    #[test]
    fn resolve_head_follows_worktree_gitdir_file() {
        let root = TempDir::new("worktree");
        let worktree = root.0.join("checkout");
        let common = root.0.join("repository.git");
        let git_dir = common.join("worktrees/checkout");
        std::fs::create_dir_all(&worktree).unwrap();
        std::fs::create_dir_all(common.join("refs/heads")).unwrap();
        std::fs::create_dir_all(&git_dir).unwrap();
        std::fs::write(
            worktree.join(".git"),
            "gitdir: ../repository.git/worktrees/checkout\n",
        )
        .unwrap();
        std::fs::write(git_dir.join("HEAD"), "ref: refs/heads/topic\n").unwrap();
        std::fs::write(git_dir.join("commondir"), "../..\n").unwrap();
        std::fs::write(common.join("refs/heads/topic"), format!("{WORKTREE_SHA}\n")).unwrap();

        let resolved = resolve_head(&worktree.join(".git")).unwrap();
        assert_eq!(resolved.revision, WORKTREE_SHA);
        assert_eq!(
            resolved.ref_path,
            Some(common.canonicalize().unwrap().join("refs/heads/topic"))
        );
    }
}
