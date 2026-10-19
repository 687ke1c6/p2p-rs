//! The node's secret key, which is its identity.
//!
//! The key file holds the 32 raw key bytes. [`load_or_init`] creates one only
//! when the file doesn't exist. A file that exists but can't be read or is the
//! wrong size is an error: replacing it would silently make this a different
//! node.

use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use iroh::SecretKey;

/// File name of the key inside the config directory.
pub const KEY_FILE: &str = "key";

#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("no key at {0}")]
    Missing(PathBuf),
    #[error("a key already exists at {0}: refusing to replace this node's identity")]
    Exists(PathBuf),
    #[error("key file {path} is {len} bytes, expected 32")]
    Corrupt { path: PathBuf, len: usize },
    #[error("key file {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
}

/// Generates a key and writes it to `path`, readable by the owner only.
/// Directories it creates are owner-only too: the config directory also holds
/// the docs store, with its namespace and author secrets. Fails if a file is
/// already there.
pub fn init(path: &Path) -> Result<SecretKey, KeyError> {
    let io_err = |source| KeyError::Io {
        path: path.to_owned(),
        source,
    };
    if let Some(dir) = path.parent() {
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(io_err)?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|err| match err.kind() {
            io::ErrorKind::AlreadyExists => KeyError::Exists(path.to_owned()),
            _ => io_err(err),
        })?;
    let key = SecretKey::generate();
    file.write_all(&key.to_bytes()).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    Ok(key)
}

/// Reads the key at `path`, or creates one there if no file exists. Returns
/// the key and whether it was created.
pub fn load_or_init(path: &Path) -> Result<(SecretKey, bool), KeyError> {
    match load(path) {
        Ok(key) => Ok((key, false)),
        Err(KeyError::Missing(_)) => match init(path) {
            Ok(key) => Ok((key, true)),
            // Another process created it between our read and create.
            Err(KeyError::Exists(_)) => Ok((load(path)?, false)),
            Err(err) => Err(err),
        },
        Err(err) => Err(err),
    }
}

/// Reads the key at `path`.
pub fn load(path: &Path) -> Result<SecretKey, KeyError> {
    let bytes = fs::read(path).map_err(|err| match err.kind() {
        io::ErrorKind::NotFound => KeyError::Missing(path.to_owned()),
        _ => KeyError::Io {
            path: path.to_owned(),
            source: err,
        },
    })?;
    let bytes: [u8; 32] = bytes.as_slice().try_into().map_err(|_| KeyError::Corrupt {
        path: path.to_owned(),
        len: bytes.len(),
    })?;
    Ok(SecretKey::from_bytes(&bytes))
}
