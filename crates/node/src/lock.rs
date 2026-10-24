//! Which process uses a config directory: the one holding the lock on its
//! `lock` file.
//!
//! Every node takes the lock when it starts (`run`, and `create` and `join`,
//! which start one too) and holds it until it is dropped. The operating
//! system lets go of it when the process ends, however it ends, so there is
//! never a stale lock to clean up. `--clear-config` asks for it to know that
//! no node uses the directory. The lock is advisory: it holds between
//! sync-node processes.

use std::fs::{DirBuilder, File, OpenOptions, TryLockError};
use std::io;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// File name of the lock inside the config directory. It holds nothing.
pub const LOCK_FILE: &str = "lock";

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("another sync-node process (`run`, `create` or `join`) is using {0}: stop it first")]
    Held(PathBuf),
    #[error("lock file {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
}

/// The lock on a config directory, held until dropped.
#[derive(Debug)]
pub struct DirLock {
    _file: File,
}

/// Takes the lock on `config_dir`, creating the directory and the file if
/// need be. Fails at once if another process holds it.
pub fn acquire(config_dir: &Path) -> Result<DirLock, LockError> {
    let path = config_dir.join(LOCK_FILE);
    let io_err = |source| LockError::Io {
        path: path.clone(),
        source,
    };
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(config_dir)
        .map_err(io_err)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(&path)
        .map_err(io_err)?;
    match file.try_lock() {
        Ok(()) => Ok(DirLock { _file: file }),
        Err(TryLockError::WouldBlock) => Err(LockError::Held(config_dir.to_owned())),
        Err(TryLockError::Error(source)) => Err(io_err(source)),
    }
}

/// Whether a process holds the lock on `config_dir` now. A directory with no
/// lock file has never had a node.
pub fn is_held(config_dir: &Path) -> Result<bool, LockError> {
    if !config_dir.join(LOCK_FILE).exists() {
        return Ok(false);
    }
    match acquire(config_dir) {
        Ok(_lock) => Ok(false),
        Err(LockError::Held(_)) => Ok(true),
        Err(err) => Err(err),
    }
}
