//! Explicit permission for server-requested external-table filesystem access.
use std::{
    io,
    path::{Path, PathBuf},
};

/// Policy for paths requested by the appliance. Virtual import readers are separate.
#[derive(Debug, Clone, Default)]
pub enum ExternalFilePolicy {
    /// Reject filesystem imports, exports and log files (default).
    #[default]
    Disabled,
    /// Allow paths confined to an existing directory, resolving symlinks.
    Directory(PathBuf),
    /// Allow any local path. Enable only for a trusted appliance and SQL source.
    Unrestricted,
}
impl ExternalFilePolicy {
    pub(crate) fn resolve(&self, path: &Path) -> io::Result<PathBuf> {
        if path.as_os_str().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty external path",
            ));
        }
        match self {
            Self::Disabled => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "external file access disabled",
            )),
            Self::Unrestricted => Ok(path.to_owned()),
            Self::Directory(root) => {
                let root = root.canonicalize()?;
                let candidate = if path.is_absolute() {
                    path.to_owned()
                } else {
                    root.join(path)
                };
                let resolved = match std::fs::symlink_metadata(&candidate) {
                    Ok(_) => candidate.canonicalize()?,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => candidate
                        .parent()
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "external path has no parent",
                            )
                        })?
                        .canonicalize()?
                        .join(candidate.file_name().ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "external path has no filename",
                            )
                        })?),
                    Err(error) => return Err(error),
                };
                if !resolved.starts_with(&root) {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "external path escapes allowed directory",
                    ));
                }
                Ok(resolved)
            }
        }
    }
}
pub(crate) fn decode_filename(bytes: &[u8]) -> crate::NzResult<String> {
    let bytes = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    let name = std::str::from_utf8(bytes)
        .map_err(|_| crate::NzError::Protocol("external filename is not UTF-8".into()))?;
    if name.is_empty() || name.contains('\0') {
        return Err(crate::NzError::Protocol("invalid external filename".into()));
    }
    Ok(name.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_disabled_and_directory_traversal() {
        assert!(ExternalFilePolicy::Disabled
            .resolve(Path::new("file"))
            .is_err());
        let policy = ExternalFilePolicy::Directory(std::env::temp_dir());
        assert!(policy.resolve(Path::new("../etc/passwd")).is_err());
        assert!(policy
            .resolve(Path::new("nz-new-file.txt"))
            .unwrap()
            .starts_with(std::env::temp_dir().canonicalize().unwrap()));
    }
    #[test]
    fn filenames_reject_invalid_utf8_and_interior_nul() {
        assert!(decode_filename(&[0xff]).is_err());
        assert!(decode_filename(b"a\0b").is_err());
        assert_eq!(decode_filename(b"file\0").unwrap(), "file");
    }
    #[cfg(unix)]
    #[test]
    fn rejects_dangling_symlink_targets_outside_directory() {
        let root = std::env::temp_dir().join(format!("nz-policy-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let link = root.join("dangling");
        let outside =
            std::env::temp_dir().join(format!("nz-outside-missing-{}", std::process::id()));
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        let result = ExternalFilePolicy::Directory(root.clone()).resolve(&link);
        std::fs::remove_file(link).unwrap();
        std::fs::remove_dir(root).unwrap();
        assert!(result.is_err());
        assert!(!outside.exists());
    }
}
