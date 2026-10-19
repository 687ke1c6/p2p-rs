//! Signed membership records and the chain check that makes a node a member.
//!
//! A record says "`added_by` added `member` to `group`" and is signed by
//! `added_by`. The founder's record is self-signed. A member's record is valid
//! if its chain of signers leads back to the founder, each link signed by the
//! key it names.

use std::collections::{HashMap, HashSet};

use iroh::{EndpointId, SecretKey, Signature};
use iroh_docs::NamespaceId;
use serde::{Deserialize, Serialize};

/// Prefix of every record's key in the membership doc: `members/<id>`.
pub const MEMBERS_PREFIX: &str = "members/";

/// Signed bytes start with this, so a record signature can't be replayed as
/// anything else the node key signs.
const SIGNING_TAG: &[u8] = b"sync-node membership record v0\0";

/// The group is the membership doc's namespace.
pub type GroupId = NamespaceId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub group: GroupId,
    pub member: EndpointId,
    pub added_by: EndpointId,
    /// Unix seconds.
    pub added_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedRecord {
    pub record: Record,
    pub signature: Signature,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ChainError {
    #[error("no record for {0}")]
    Missing(EndpointId),
    #[error("record for {0} is for another group")]
    WrongGroup(EndpointId),
    #[error("record for {0} has a bad signature")]
    BadSignature(EndpointId),
    #[error("chain ends at {0}, which is not the founder")]
    NotFounder(EndpointId),
    #[error("chain loops at {0}")]
    Loop(EndpointId),
}

impl Record {
    /// The key this record is stored under in the membership doc.
    pub fn doc_key(member: EndpointId) -> String {
        format!("{MEMBERS_PREFIX}{member}")
    }

    fn signing_bytes(&self) -> Vec<u8> {
        let mut bytes = SIGNING_TAG.to_vec();
        bytes.extend(postcard::to_stdvec(self).expect("record serializes"));
        bytes
    }

    /// Signs the record with `key`, which must be `added_by`'s.
    pub fn sign(self, key: &SecretKey) -> SignedRecord {
        assert_eq!(key.public(), self.added_by, "signer must be added_by");
        let signature = key.sign(&self.signing_bytes());
        SignedRecord {
            record: self,
            signature,
        }
    }
}

impl SignedRecord {
    /// Checks the signature only, not the chain.
    pub fn verify_signature(&self) -> bool {
        self.record
            .added_by
            .verify(&self.record.signing_bytes(), &self.signature)
            .is_ok()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("record serializes")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// Checks that `member` has a record in `records` whose chain of signers
/// reaches `founder`'s self-signed record, all within `group`.
pub fn validate_chain<'a>(
    group: GroupId,
    founder: EndpointId,
    member: EndpointId,
    records: impl IntoIterator<Item = &'a SignedRecord>,
) -> Result<(), ChainError> {
    let by_member: HashMap<EndpointId, &SignedRecord> = records
        .into_iter()
        .map(|signed| (signed.record.member, signed))
        .collect();
    let mut seen = HashSet::new();
    let mut current = member;
    loop {
        if !seen.insert(current) {
            return Err(ChainError::Loop(current));
        }
        let signed = by_member
            .get(&current)
            .ok_or(ChainError::Missing(current))?;
        if signed.record.group != group {
            return Err(ChainError::WrongGroup(current));
        }
        if !signed.verify_signature() {
            return Err(ChainError::BadSignature(current));
        }
        let signer = signed.record.added_by;
        if signer == current {
            return if current == founder {
                Ok(())
            } else {
                Err(ChainError::NotFounder(current))
            };
        }
        current = signer;
    }
}

/// Picks out `member`'s chain from `records`: its record, its signer's, and so
/// on up to the founder. Stops early, without error, if a link is missing.
pub fn chain_of<'a>(
    member: EndpointId,
    records: impl IntoIterator<Item = &'a SignedRecord>,
) -> Vec<SignedRecord> {
    let by_member: HashMap<EndpointId, &SignedRecord> = records
        .into_iter()
        .map(|signed| (signed.record.member, signed))
        .collect();
    let mut chain = Vec::new();
    let mut current = member;
    while let Some(signed) = by_member.get(&current) {
        if chain.len() > by_member.len() {
            break;
        }
        chain.push((*signed).clone());
        if signed.record.added_by == current {
            break;
        }
        current = signed.record.added_by;
    }
    chain
}

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
