//! Adding and removing members by node ID, and members' and the group's
//! display names.
//!
//! `member add <id> [--name <name>]` can't write to the membership doc itself:
//! the running node holds the docs store. Instead it queues the add as a file
//! `adds/<id>` in the config directory, holding the name if one was given, and
//! the monitor applies it on its next interval and removes the file.
//! `member remove <id>` queues `removes/<id>` and `group name <name>` queues
//! the file `group-name` the same way.
//!
//! Names live in the membership doc under `names/<id>`, apart from the signed
//! records, so any member can set or change one and every node sees the same
//! name. The newest entry per member wins. Names are unique: if two members
//! end up with the same name (two nodes set it at once), the one that took it
//! first keeps it and the other is shown by its short ID. The group's name
//! is the entry `group-name`, set by any member, the newest winning.

use std::collections::{HashMap, HashSet};
use std::fs::{self, DirBuilder};
use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::Context;
use iroh::{EndpointId, SecretKey};
use iroh_blobs::store::fs::FsStore;
use iroh_docs::protocol::Docs;
use iroh_docs::store::Query;
use n0_future::StreamExt;

use crate::record::{unix_now, GroupId, Record, Removal, SignedRecord, SignedRemoval};

/// Directory of queued adds inside the config directory.
pub const ADDS_DIR: &str = "adds";

/// Directory of queued removals inside the config directory.
pub const REMOVES_DIR: &str = "removes";

/// The queued group name inside the config directory.
pub const GROUP_NAME_FILE: &str = "group-name";

/// The group name's key in the membership doc. No other key starts with it,
/// nor is a start of it (see `index::doc_key` for why that matters).
const GROUP_NAME_KEY: &str = "group-name";

/// Prefix of every name's key in the membership doc: `names/<id>`.
pub const NAMES_PREFIX: &str = "names/";

/// Longest name allowed, in characters.
pub const MAX_NAME_CHARS: usize = 32;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NameError {
    #[error("name is empty")]
    Empty,
    #[error("name is longer than {MAX_NAME_CHARS} characters")]
    TooLong,
    #[error("name contains control characters")]
    Control,
}

/// Checks a name and returns it trimmed.
pub fn check_name(name: &str) -> Result<String, NameError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(NameError::Empty);
    }
    if name.chars().count() > MAX_NAME_CHARS {
        return Err(NameError::TooLong);
    }
    if name.chars().any(char::is_control) {
        return Err(NameError::Control);
    }
    Ok(name.to_string())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RemoveError {
    #[error("the founder can't be removed")]
    Founder,
    #[error("a node can't remove itself")]
    Itself,
}

/// Checks that node `me` may remove `id` from the group `founder` founded.
/// Whether `id` is a member is up to the running node.
pub fn check_removable(
    id: EndpointId,
    me: EndpointId,
    founder: EndpointId,
) -> Result<(), RemoveError> {
    if id == founder {
        return Err(RemoveError::Founder);
    }
    if id == me {
        return Err(RemoveError::Itself);
    }
    Ok(())
}

fn name_key(id: EndpointId) -> String {
    format!("{NAMES_PREFIX}{id}")
}

/// Members' names and the group's name as this node last read them from the
/// membership doc. Shared by everything that logs or shows a member.
#[derive(Debug, Clone, Default)]
pub struct Names {
    members: Arc<RwLock<HashMap<EndpointId, String>>>,
    group: Arc<RwLock<Option<String>>>,
}

/// A change of a member's name between two reads of the membership doc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rename {
    pub id: EndpointId,
    pub old: Option<String>,
    pub new: Option<String>,
}

impl Names {
    pub fn get(&self, id: EndpointId) -> Option<String> {
        self.members.read().unwrap().get(&id).cloned()
    }

    pub fn group(&self) -> Option<String> {
        self.group.read().unwrap().clone()
    }

    /// Replaces the group's name; whether it changed.
    pub fn set_group(&self, name: Option<String>) -> bool {
        let mut current = self.group.write().unwrap();
        let changed = *current != name;
        *current = name;
        changed
    }

    /// How to show `id`: its name if it has one, otherwise its short ID.
    pub fn display(&self, id: EndpointId) -> String {
        self.get(id).unwrap_or_else(|| id.fmt_short().to_string())
    }

    /// The member that has `name`, if any.
    pub fn holder(&self, name: &str) -> Option<EndpointId> {
        self.members
            .read()
            .unwrap()
            .iter()
            .find(|(_, n)| n.as_str() == name)
            .map(|(id, _)| *id)
    }

    /// Replaces all names and returns what changed.
    pub fn replace(&self, names: HashMap<EndpointId, String>) -> Vec<Rename> {
        let mut current = self.members.write().unwrap();
        let ids: HashSet<EndpointId> = current.keys().chain(names.keys()).copied().collect();
        let mut renames: Vec<Rename> = ids
            .into_iter()
            .filter_map(|id| {
                let (old, new) = (current.get(&id).cloned(), names.get(&id).cloned());
                (old != new).then_some(Rename { id, old, new })
            })
            .collect();
        renames.sort_by_key(|rename| rename.id);
        *current = names;
        renames
    }
}

/// Signs a record adding `member` to `group` and writes it to the membership
/// doc. Signing your own record founds the group.
pub(crate) async fn write_record(
    docs: &Docs,
    group: GroupId,
    key: &SecretKey,
    member: EndpointId,
) -> anyhow::Result<SignedRecord> {
    let record = Record {
        group,
        member,
        added_by: key.public(),
        added_at: unix_now(),
    }
    .sign(key);
    let doc = docs.open(group).await?.context("membership doc missing")?;
    let author = docs.author_default().await?;
    doc.set_bytes(author, Record::doc_key(member), record.to_bytes())
        .await?;
    Ok(record)
}

/// Signs a removal of `member` from `group` and writes it to the membership
/// doc. The caller checks [`check_removable`].
pub(crate) async fn write_removal(
    docs: &Docs,
    group: GroupId,
    key: &SecretKey,
    member: EndpointId,
) -> anyhow::Result<SignedRemoval> {
    let removal = Removal {
        group,
        member,
        removed_by: key.public(),
        removed_at: unix_now(),
    }
    .sign(key);
    let doc = docs.open(group).await?.context("membership doc missing")?;
    let author = docs.author_default().await?;
    doc.set_bytes(author, Removal::doc_key(member), removal.to_bytes())
        .await?;
    Ok(removal)
}

/// Writes the group's name to the membership doc.
pub(crate) async fn write_group_name(
    docs: &Docs,
    group: GroupId,
    name: &str,
) -> anyhow::Result<()> {
    let doc = docs.open(group).await?.context("membership doc missing")?;
    let author = docs.author_default().await?;
    doc.set_bytes(author, GROUP_NAME_KEY, name.as_bytes().to_vec())
        .await?;
    Ok(())
}

/// The group's name in a copy of the membership doc: the newest entry.
pub(crate) async fn read_group_name(
    docs: &Docs,
    blobs: &FsStore,
    group: GroupId,
) -> anyhow::Result<Option<String>> {
    let doc = docs.open(group).await?.context("membership doc missing")?;
    let Some(entry) = doc
        .get_many(Query::single_latest_per_key().key_exact(GROUP_NAME_KEY))
        .await?
        .collect::<Vec<_>>()
        .await
        .pop()
        .transpose()?
    else {
        return Ok(None);
    };
    let Ok(bytes) = blobs.get_bytes(entry.content_hash()).await else {
        return Ok(None); // Content not fetched yet.
    };
    Ok(std::str::from_utf8(&bytes)
        .ok()
        .and_then(|name| check_name(name).ok()))
}

/// Writes `id`'s name to the membership doc. The caller checks it is unique.
pub(crate) async fn write_name(
    docs: &Docs,
    group: GroupId,
    id: EndpointId,
    name: &str,
) -> anyhow::Result<()> {
    let doc = docs.open(group).await?.context("membership doc missing")?;
    let author = docs.author_default().await?;
    doc.set_bytes(author, name_key(id), name.as_bytes().to_vec())
        .await?;
    Ok(())
}

/// The names in a copy of the membership doc, for `members` only. Of two
/// members with the same name, the one that took it first keeps it.
pub(crate) async fn read_names(
    docs: &Docs,
    blobs: &FsStore,
    group: GroupId,
    members: &[SignedRecord],
) -> anyhow::Result<HashMap<EndpointId, String>> {
    let doc = docs.open(group).await?.context("membership doc missing")?;
    let members: HashSet<EndpointId> = members.iter().map(|s| s.record.member).collect();
    let entries = doc
        .get_many(Query::single_latest_per_key().key_prefix(NAMES_PREFIX))
        .await?
        .collect::<Vec<_>>()
        .await;
    let mut claims = Vec::new();
    for entry in entries {
        let entry = entry?;
        let Some(id) = std::str::from_utf8(entry.key())
            .ok()
            .and_then(|key| key.strip_prefix(NAMES_PREFIX))
            .and_then(|id| id.parse::<EndpointId>().ok())
            .filter(|id| members.contains(id))
        else {
            continue;
        };
        let Ok(bytes) = blobs.get_bytes(entry.content_hash()).await else {
            continue; // Content not fetched yet.
        };
        let Some(name) = std::str::from_utf8(&bytes)
            .ok()
            .and_then(|name| check_name(name).ok())
        else {
            continue;
        };
        claims.push((entry.timestamp(), id, name));
    }
    claims.sort();
    let mut taken = HashSet::new();
    let mut names = HashMap::new();
    for (_, id, name) in claims {
        if taken.insert(name.clone()) {
            names.insert(id, name);
        }
    }
    Ok(names)
}

/// Queues adding `id` to the group, or renaming it if it is already a member,
/// for the running node to apply. Queuing the same ID again replaces the
/// earlier request.
pub fn queue_add(config_dir: &Path, id: EndpointId, name: Option<&str>) -> io::Result<PathBuf> {
    let dir = config_dir.join(ADDS_DIR);
    DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
    let path = dir.join(id.to_string());
    let tmp = dir.join(format!("{id}.tmp"));
    fs::write(&tmp, name.unwrap_or_default())?;
    fs::rename(&tmp, &path)?;
    Ok(path)
}

/// A queued add, read back by the monitor.
#[derive(Debug)]
pub(crate) struct QueuedAdd {
    pub path: PathBuf,
    /// The member and its name, or why the file is unusable.
    pub add: Result<(EndpointId, Option<String>), String>,
}

/// The adds waiting in `adds/`.
pub(crate) fn queued_adds(config_dir: &Path) -> io::Result<Vec<QueuedAdd>> {
    let mut adds = Vec::new();
    for (path, file_name) in queued_files(&config_dir.join(ADDS_DIR))? {
        let add = match (file_name.parse::<EndpointId>(), fs::read_to_string(&path)) {
            (Err(_), _) => Err("file name is not a node ID".to_string()),
            (_, Err(err)) => Err(err.to_string()),
            (Ok(id), Ok(text)) if text.trim().is_empty() => Ok((id, None)),
            (Ok(id), Ok(text)) => check_name(&text)
                .map(|name| (id, Some(name)))
                .map_err(|err| err.to_string()),
        };
        adds.push(QueuedAdd { path, add });
    }
    Ok(adds)
}

/// Queues removing `id` from the group, for the running node to apply.
pub fn queue_remove(config_dir: &Path, id: EndpointId) -> io::Result<PathBuf> {
    let dir = config_dir.join(REMOVES_DIR);
    DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
    let path = dir.join(id.to_string());
    fs::write(&path, "")?;
    Ok(path)
}

/// A queued removal, read back by the monitor.
#[derive(Debug)]
pub(crate) struct QueuedRemove {
    pub path: PathBuf,
    /// The member, or why the file is unusable.
    pub id: Result<EndpointId, String>,
}

/// The removals waiting in `removes/`.
pub(crate) fn queued_removes(config_dir: &Path) -> io::Result<Vec<QueuedRemove>> {
    Ok(queued_files(&config_dir.join(REMOVES_DIR))?
        .into_iter()
        .map(|(path, file_name)| QueuedRemove {
            path,
            id: file_name
                .parse()
                .map_err(|_| "file name is not a node ID".to_string()),
        })
        .collect())
}

/// Queues naming the group `name` (checked by the caller), for the running
/// node to apply. Queuing again replaces the earlier request.
pub fn queue_group_name(config_dir: &Path, name: &str) -> io::Result<PathBuf> {
    let path = config_dir.join(GROUP_NAME_FILE);
    let tmp = config_dir.join(format!("{GROUP_NAME_FILE}.tmp"));
    fs::write(&tmp, name)?;
    fs::rename(&tmp, &path)?;
    Ok(path)
}

/// The queued group name, if any: the file, and the name or why it is
/// unusable.
pub(crate) fn queued_group_name(
    config_dir: &Path,
) -> io::Result<Option<(PathBuf, Result<String, String>)>> {
    let path = config_dir.join(GROUP_NAME_FILE);
    match fs::read_to_string(&path) {
        Ok(text) => {
            let name = check_name(&text).map_err(|err| err.to_string());
            Ok(Some((path, name)))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// The files in queue directory `dir`, with their names, leaving out ones
/// still being written. None if `dir` doesn't exist.
fn queued_files(dir: &Path) -> io::Result<Vec<(PathBuf, String)>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    let mut files = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if file_name.ends_with(".tmp") {
            continue;
        }
        let file_name = file_name.to_string();
        files.push((path, file_name));
    }
    Ok(files)
}
