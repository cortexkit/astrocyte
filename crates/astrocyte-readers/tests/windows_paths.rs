//! Windows path spellings through the readers' public API.
//!
//! The portable tests read committed transcript fixtures whose recorded
//! working directory is a UNC share or a drive-letter path, so every platform
//! checks how such a directory becomes a workspace key. The Windows-only test
//! opens one real file under several spellings of its path; it needs no
//! network share, because `\\?\C:\...` is the local, UNC-style verbatim form of
//! an ordinary drive path. CI runs all of them in the native Windows job.

use std::path::{Path, PathBuf};

use astrocyte_readers::sessions::normalize_workspace_key;
use astrocyte_readers::sessions::pi::parse_pi_file;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// A Pi session started on a UNC share keeps the share's double-slash prefix
/// and its server in the workspace key; only separators and the trailing
/// separator change.
#[test]
fn pi_session_on_a_unc_share_is_keyed_by_server_and_share() {
    let messages = parse_pi_file(&fixture("pi_unc_cwd.jsonl"));
    assert_eq!(messages.len(), 1, "the fixture holds one assistant call");
    assert_eq!(
        messages[0].workspace_key.as_deref(),
        Some("//build-server/share/Team Repo")
    );
    assert_eq!(messages[0].workspace_label.as_deref(), Some("Team Repo"));
}

/// A Pi session started under a lower-case drive letter keeps that case: the
/// key is not folded to upper case.
#[test]
fn pi_session_under_a_lower_case_drive_letter_keeps_its_case() {
    let messages = parse_pi_file(&fixture("pi_drive_letter_cwd.jsonl"));
    assert_eq!(messages.len(), 1, "the fixture holds one assistant call");
    assert_eq!(
        messages[0].workspace_key.as_deref(),
        Some("c:/Users/Alice/Repo")
    );
    assert_eq!(messages[0].workspace_label.as_deref(), Some("Repo"));
}

/// The spellings a Windows producer may record for the same directory, and
/// the key each one becomes today. Drive-letter case is kept, so `C:` and
/// `c:` are different keys; folding them together would merge workspace
/// groups and is not done here.
#[test]
fn unc_and_drive_letter_spellings_normalize_as_recorded() {
    for (raw, key) in [
        (r"\\server\share", "//server/share"),
        (r"\\server\share\", "//server/share"),
        (r"\\server/share\repo", "//server/share/repo"),
        ("//server//share//repo//", "//server/share/repo"),
        (r"C:\", "C:/"),
        (r"c:\", "c:/"),
        (r"C:\Repo\", "C:/Repo"),
        (r"c:\Repo\", "c:/Repo"),
        ("C:/Repo", "C:/Repo"),
    ] {
        assert_eq!(normalize_workspace_key(raw).as_deref(), Some(key), "{raw}");
    }
    assert_ne!(
        normalize_workspace_key(r"C:\Repo"),
        normalize_workspace_key(r"c:\Repo")
    );
}

/// One file reached through its plain drive path, the same path with the
/// drive letter in the other case, and its `\\?\` verbatim form has one file
/// identity, and a Pi resume point taken through one spelling resumes through
/// the others: identity is the volume serial number and file index of the
/// opened handle, not the path text.
#[cfg(windows)]
#[test]
fn windows_path_spellings_of_one_file_share_identity_and_resume() {
    use astrocyte_readers::file_identity::file_identity;
    use astrocyte_readers::sessions::pi::{parse_pi_file_resumable, PiPrefixCheck};
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let written = dir.path().join("session.jsonl");
    std::fs::copy(fixture("pi_drive_letter_cwd.jsonl"), &written).unwrap();

    let verbatim = std::fs::canonicalize(&written).unwrap();
    let verbatim_text = verbatim.to_str().unwrap();
    let plain_text = verbatim_text
        .strip_prefix(r"\\?\")
        .unwrap_or_else(|| panic!("canonical path {verbatim_text} is not verbatim"));
    let drive = plain_text.chars().next().unwrap();
    assert!(
        drive.is_ascii_alphabetic() && plain_text[1..].starts_with(r":\"),
        "{plain_text} is not a drive path"
    );
    let flipped_drive = if drive.is_ascii_uppercase() {
        drive.to_ascii_lowercase()
    } else {
        drive.to_ascii_uppercase()
    };
    let plain = PathBuf::from(plain_text);
    let flipped = PathBuf::from(format!("{flipped_drive}{}", &plain_text[1..]));

    let identity = |path: &Path| {
        let file = std::fs::File::open(path).unwrap();
        file_identity(&file, &file.metadata().unwrap())
    };
    let expected = identity(&plain);
    assert!(expected.is_some(), "Windows reports a file identity");
    assert_eq!(identity(&flipped), expected, "{}", flipped.display());
    assert_eq!(identity(&verbatim), expected, "{}", verbatim.display());

    let first = parse_pi_file_resumable(&plain, None, PiPrefixCheck::Full);
    assert_eq!(first.messages.len(), 1);
    let mut point = first.resume.expect("a readable file yields a resume point");

    for (index, spelling) in [&flipped, &verbatim].into_iter().enumerate() {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&written)
            .unwrap();
        writeln!(
            file,
            r#"{{"type":"message","id":"drive-append-{index}","timestamp":"2026-09-20T00:00:02.000Z","message":{{"role":"assistant","model":"claude-sonnet-4","provider":"anthropic","responseId":"drive-append-{index}","usage":{{"input":1,"output":2,"cacheRead":0,"cacheWrite":0,"totalTokens":3}}}}}}"#
        )
        .unwrap();
        drop(file);

        let resumed = parse_pi_file_resumable(spelling, Some(&point), PiPrefixCheck::Full);
        assert!(resumed.resumed, "{} did not resume", spelling.display());
        assert_eq!(
            resumed.messages.len(),
            1,
            "{} read more than the appended record",
            spelling.display()
        );
        point = resumed
            .resume
            .expect("a readable file yields a resume point");
    }
}
