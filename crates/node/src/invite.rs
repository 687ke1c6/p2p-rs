//! Invites: one-time tickets that let a new node join the group.
//!
//! The inviter keeps only a hash of each ticket's secret, as a file in
//! `invites/` named after the hash and holding the expiry time and, on a
//! second line, the name to give the new member if one was set. Redeeming a
//! ticket deletes that file, so a ticket works once: a second redemption, or
//! a race between two, finds no file.

use std::fmt;
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use data_encoding::BASE32_NOPAD;
use iroh::EndpointAddr;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::member::check_name;
use crate::record::{unix_now, GroupId};

/// Directory of pending invites inside the config directory.
pub const INVITES_DIR: &str = "invites";

const TICKET_PREFIX: &str = "invite";

/// What `invite create` prints and `join` takes. Anyone holding it can join
/// once, so it is as secret as a password until it is used or expires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InviteTicket {
    pub inviter: EndpointAddr,
    pub group: GroupId,
    pub secret: [u8; 32],
    /// Unix seconds.
    pub expires_at: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum InviteError {
    #[error("invite is unknown or already used")]
    Unknown,
    #[error("invite has expired")]
    Expired,
    #[error("invite file {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
}

#[derive(Debug, thiserror::Error)]
#[error("not an invite ticket")]
pub struct ParseTicketError;

/// Writes a ticket as `prefix` followed by its bytes in lowercase base32.
fn encode_ticket<T: Serialize>(
    f: &mut fmt::Formatter<'_>,
    prefix: &str,
    ticket: &T,
) -> fmt::Result {
    let bytes = postcard::to_stdvec(ticket).expect("ticket serializes");
    write!(
        f,
        "{prefix}{}",
        BASE32_NOPAD.encode(&bytes).to_ascii_lowercase()
    )
}

/// Reads a ticket written by [`encode_ticket`] with the same `prefix`.
fn decode_ticket<T: DeserializeOwned>(s: &str, prefix: &str) -> Result<T, ParseTicketError> {
    let encoded = s.trim().strip_prefix(prefix).ok_or(ParseTicketError)?;
    let bytes = BASE32_NOPAD
        .decode(encoded.to_ascii_uppercase().as_bytes())
        .map_err(|_| ParseTicketError)?;
    postcard::from_bytes(&bytes).map_err(|_| ParseTicketError)
}

impl fmt::Display for InviteTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        encode_ticket(f, TICKET_PREFIX, self)
    }
}

impl FromStr for InviteTicket {
    type Err = ParseTicketError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        decode_ticket(s, TICKET_PREFIX)
    }
}

/// Makes a ticket for `group` valid for `ttl`, and records its secret's hash
/// under `config_dir` so the running node can redeem it. `name`, already
/// checked, is given to the new member when it joins. Doesn't need the node
/// to be running.
pub fn create(
    config_dir: &Path,
    inviter: EndpointAddr,
    group: GroupId,
    ttl: Duration,
    name: Option<&str>,
) -> Result<InviteTicket, InviteError> {
    let dir = config_dir.join(INVITES_DIR);
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&dir)
        .map_err(|source| InviteError::Io {
            path: dir.clone(),
            source,
        })?;
    let ticket = InviteTicket {
        inviter,
        group,
        secret: rand::random(),
        expires_at: unix_now().saturating_add(ttl.as_secs()),
    };
    let path = invite_path(config_dir, &ticket.secret);
    let io_err = |source| InviteError::Io {
        path: path.clone(),
        source,
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .map_err(io_err)?;
    let mut contents = ticket.expires_at.to_string();
    if let Some(name) = name {
        contents.push('\n');
        contents.push_str(name);
    }
    file.write_all(contents.as_bytes()).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    Ok(ticket)
}

/// Uses up the invite with `secret` and returns the name set for the new
/// member, if any. Succeeds at most once per invite, and only before it
/// expires.
pub fn redeem(config_dir: &Path, secret: &[u8; 32]) -> Result<Option<String>, InviteError> {
    let path = invite_path(config_dir, secret);
    let io_err = |source| InviteError::Io {
        path: path.clone(),
        source,
    };
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Err(InviteError::Unknown),
        Err(err) => return Err(io_err(err)),
    };
    // Removing the file is the claim: of two racing redemptions, only one
    // removes it.
    match fs::remove_file(&path) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Err(InviteError::Unknown),
        Err(err) => return Err(io_err(err)),
    }
    let (expiry, name) = contents.split_once('\n').unwrap_or((&contents, ""));
    // An unreadable expiry counts as expired.
    let expires_at: u64 = expiry.trim().parse().unwrap_or(0);
    if unix_now() >= expires_at {
        return Err(InviteError::Expired);
    }
    // A name that no longer checks out is dropped rather than failing the join.
    Ok(check_name(name).ok())
}

fn invite_path(config_dir: &Path, secret: &[u8; 32]) -> PathBuf {
    config_dir
        .join(INVITES_DIR)
        .join(blake3::hash(secret).to_hex().as_str())
}
