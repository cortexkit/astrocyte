//! Identity of the opened file, not a second lookup of its pathname.

use std::fs::{File, Metadata};

/// Device and inode on Unix, or volume serial and file index on Windows.
#[cfg(unix)]
pub fn file_identity(_file: &File, metadata: &Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    Some((metadata.dev(), metadata.ino()))
}

#[cfg(windows)]
pub fn file_identity(file: &File, _metadata: &Metadata) -> Option<(u64, u64)> {
    let info = winapi_util::file::information(file).ok()?;
    Some((info.volume_serial_number(), info.file_index()))
}

#[cfg(not(any(unix, windows)))]
pub fn file_identity(_file: &File, _metadata: &Metadata) -> Option<(u64, u64)> {
    None
}

/// Identity for transcript offsets and JSON document caches. Inode numbers and
/// Windows file indexes are local to a device or volume, not globally unique.
pub fn inode_of(file: &File, metadata: &Metadata) -> Option<(u64, u64)> {
    file_identity(file, metadata)
}

/// An unavailable identity cannot justify reusing cached content or an offset.
pub fn same_identity<T: PartialEq>(current: Option<T>, previous: Option<T>) -> bool {
    current.is_some() && current == previous
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_file_identities_are_never_the_same() {
        assert!(!same_identity::<(u64, u64)>(None, None));
        assert!(!same_identity(None, Some((1, 2))));
        assert!(!same_identity(Some((1, 2)), None));
        assert!(same_identity(Some((1, 2)), Some((1, 2))));
        assert!(!same_identity(Some((1, 2)), Some((2, 2))));
        assert!(!same_identity(Some((1, 2)), Some((1, 3))));
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn identity_belongs_to_the_opened_file_after_its_path_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        std::fs::write(&path, "original").unwrap();
        let original = File::open(&path).unwrap();
        let before = file_identity(&original, &original.metadata().unwrap()).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "replacement").unwrap();
        let replacement = File::open(&path).unwrap();
        assert_eq!(
            file_identity(&original, &original.metadata().unwrap()),
            Some(before)
        );
        assert_ne!(
            file_identity(&replacement, &replacement.metadata().unwrap()),
            Some(before)
        );
    }
}
