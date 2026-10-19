//! This node's group: which group it belongs to, who founded it, and its own
//! chain of records. Saved in the config directory as `group`.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use iroh::EndpointId;
use serde::{Deserialize, Serialize};

use crate::record::{validate_chain, ChainError, GroupId, SignedRecord};

/// File name of the group state inside the config directory.
pub const GROUP_FILE: &str = "group";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Group {
    pub id: GroupId,
    pub founder: EndpointId,
    /// This node's record first, then its signer's, up to the founder's. It is
    /// what the node presents to prove it is a member.
    pub chain: Vec<SignedRecord>,
}

#[derive(Debug, thiserror::Error)]
pub enum GroupError {
    #[error("already in group {0}")]
    AlreadyInGroup(GroupId),
    #[error("not in a group: run `create` or `join` first")]
    NotInGroup,
    #[error("group file {path} is corrupt: {source}")]
    Corrupt {
        path: PathBuf,
        source: postcard::Error,
    },
    #[error("group file {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("own record: {0}")]
    Chain(#[from] ChainError),
}

impl Group {
    /// The founder's group: its own self-signed record is the whole chain.
    pub fn founded(founder_record: SignedRecord) -> Self {
        Self {
            id: founder_record.record.group,
            founder: founder_record.record.member,
            chain: vec![founder_record],
        }
    }

    /// Builds the group from `member`'s chain, taking the founder to be the
    /// chain's self-signed root, and checks it.
    pub fn from_chain(member: EndpointId, chain: Vec<SignedRecord>) -> Result<Self, GroupError> {
        let root = chain
            .iter()
            .find(|signed| signed.record.member == signed.record.added_by)
            .ok_or(ChainError::Missing(member))?;
        let group = Self {
            id: root.record.group,
            founder: root.record.member,
            chain,
        };
        group.check(member)?;
        Ok(group)
    }

    /// Checks that the chain is `member`'s and reaches the founder.
    pub fn check(&self, member: EndpointId) -> Result<(), ChainError> {
        validate_chain(self.id, self.founder, member, &self.chain)
    }

    /// Reads the group file, or `None` if this node isn't in a group yet.
    pub fn load(config_dir: &Path) -> Result<Option<Self>, GroupError> {
        let path = config_dir.join(GROUP_FILE);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(GroupError::Io { path, source }),
        };
        postcard::from_bytes(&bytes)
            .map(Some)
            .map_err(|source| GroupError::Corrupt { path, source })
    }

    /// Writes the group file, owner-only. Fails if one exists: a node is in at
    /// most one group.
    pub fn save_new(&self, config_dir: &Path) -> Result<(), GroupError> {
        let path = config_dir.join(GROUP_FILE);
        let io_err = |source| GroupError::Io {
            path: path.clone(),
            source,
        };
        let bytes = postcard::to_stdvec(self).expect("group serializes");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .map_err(|err| match err.kind() {
                io::ErrorKind::AlreadyExists => match Self::load(config_dir) {
                    Ok(Some(existing)) => GroupError::AlreadyInGroup(existing.id),
                    _ => io_err(err),
                },
                _ => io_err(err),
            })?;
        file.write_all(&bytes).map_err(io_err)?;
        file.sync_all().map_err(io_err)?;
        Ok(())
    }
}
