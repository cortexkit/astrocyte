//! Helpers for integration tests that run the real `ck-astrocyte` binary.

pub use astrocyte_module::test_support::output_within as output_within_guard;
use std::path::{Path, PathBuf};

/// The binary to run, as `ckdev-astrocyte`.
///
/// A test process must be distinguishable by executable name from the deployed
/// metering module (`ck-astrocyte`), so tests run a separately named copy. The copy is
/// not a hard link to cargo's output: exec through such a link has been seen
/// to die by SIGKILL at startup on macOS. It is not a fresh file under the
/// per-user temp dir in /var/folders either, where macOS's first policy check of
/// a new executable has been seen to hang for many minutes. So on macOS it lives
/// under `$TMPDIR` when that is set, absolute and outside /var/folders (a
/// sandboxed worker can write only there), and under /tmp otherwise. Other
/// platforms use their system temp directory. The build's SHA-256 names the
/// directory, reused while the build is unchanged, and a new build is published
/// by rename, never written over an existing file.
pub fn ckdev_binary() -> PathBuf {
    use sha2::{Digest, Sha256};
    let source = Path::new(env!("CARGO_BIN_EXE_ck-astrocyte"));
    let digest = Sha256::digest(std::fs::read(source).unwrap());
    let key: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
    #[cfg(target_os = "macos")]
    let temp = macos_copy_root(std::env::var_os("TMPDIR"));
    #[cfg(not(target_os = "macos"))]
    let temp = std::env::temp_dir();
    let dir = temp.join(format!("astrocyte-ckdev-{key}"));
    let name = format!("ckdev-astrocyte{}", std::env::consts::EXE_SUFFIX);
    let binary = dir.join(&name);
    if !binary.exists() {
        std::fs::create_dir_all(&dir).unwrap();
        // Tests in one binary run on parallel threads of one process, so the
        // staging name needs the thread as well as the process to be unique.
        // Another thread may publish first; then its copy is the same build.
        let thread = format!("{:?}", std::thread::current().id());
        let thread: String = thread.chars().filter(char::is_ascii_digit).collect();
        let staging = dir.join(format!(".{name}.{}.{thread}", std::process::id()));
        std::fs::copy(source, &staging).unwrap();
        if let Err(error) = std::fs::rename(&staging, &binary) {
            let _ = std::fs::remove_file(&staging);
            assert!(binary.exists(), "publish {}: {error}", binary.display());
        }
    }
    binary
}

/// Where the macOS copy goes: `$TMPDIR` if it is set, absolute and not under
/// /var/folders (compared after resolving symlinks, since /var is a link to
/// /private/var), else /tmp.
#[cfg(target_os = "macos")]
fn macos_copy_root(tmpdir: Option<std::ffi::OsString>) -> PathBuf {
    let usable = tmpdir.map(PathBuf::from).filter(|dir| {
        dir.is_absolute()
            && std::fs::canonicalize(dir)
                .is_ok_and(|real| !real.starts_with("/private/var/folders"))
    });
    usable.unwrap_or_else(|| PathBuf::from("/tmp"))
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::macos_copy_root;
    use std::path::PathBuf;

    #[test]
    fn copy_root_follows_tmpdir_except_under_var_folders() {
        let worker = tempfile::tempdir_in("/tmp").unwrap();
        let real = std::fs::canonicalize(worker.path()).unwrap();
        assert_eq!(
            std::fs::canonicalize(macos_copy_root(Some(worker.path().into()))).unwrap(),
            real
        );
        assert_eq!(macos_copy_root(None), PathBuf::from("/tmp"));
        assert_eq!(
            macos_copy_root(Some("relative/dir".into())),
            PathBuf::from("/tmp")
        );
        assert_eq!(
            macos_copy_root(Some("/var/folders".into())),
            PathBuf::from("/tmp")
        );
    }
}

/// Every file and directory below `dir`, recursively.
pub fn entries_under(dir: &Path) -> Vec<String> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        found.push(path.display().to_string());
        if path.is_dir() {
            found.extend(entries_under(&path));
        }
    }
    found
}
