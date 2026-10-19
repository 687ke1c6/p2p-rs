use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{presets, ConnectionError};
use iroh::{Endpoint, EndpointId, SecretKey};
use sync_node::gate::NOT_A_MEMBER;
use sync_node::record::{unix_now, Record};
use sync_node::{Config, Group, InviteTicket, Network, Node};

const TIMEOUT: Duration = Duration::from_secs(20);
const HOUR: Duration = Duration::from_secs(3600);

async fn spawn(dir: &Path) -> Node {
    Node::spawn(dir, SecretKey::generate(), Network::Local)
        .await
        .unwrap()
}

async fn member_ids(node: &Node) -> BTreeSet<EndpointId> {
    node.members()
        .await
        .unwrap()
        .into_iter()
        .map(|signed| signed.record.member)
        .collect()
}

/// Waits until `node`'s copy of the membership doc lists exactly `expected`.
async fn wait_for_members(node: &Node, expected: &BTreeSet<EndpointId>) {
    let result = tokio::time::timeout(TIMEOUT, async {
        loop {
            if &member_ids(node).await == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "node {} has members {:?}, expected {expected:?}",
        node.id(),
        member_ids(node).await
    );
}

#[tokio::test]
async fn invited_nodes_join_and_everyone_agrees() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path()).await;
    let b = spawn(dirs[1].path()).await;
    let c = spawn(dirs[2].path()).await;

    let group = a.create_group().await.unwrap();
    let ticket = a.create_invite(HOUR, None).unwrap();
    assert_eq!(b.join(&ticket).await.unwrap(), group);
    let ticket = a.create_invite(HOUR, None).unwrap();
    assert_eq!(c.join(&ticket).await.unwrap(), group);

    let expected: BTreeSet<_> = [a.id(), b.id(), c.id()].into();
    for node in [&a, &b, &c] {
        wait_for_members(node, &expected).await;
        assert_eq!(node.group().unwrap().founder, a.id());
    }

    // The group survives a restart.
    let key = b.endpoint().secret_key().clone();
    b.shutdown().await.unwrap();
    let b = Node::spawn(dirs[1].path(), key, Network::Local)
        .await
        .unwrap();
    assert_eq!(b.group().unwrap().id, group);
    wait_for_members(&b, &expected).await;

    for node in [a, b, c] {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn bad_invites_are_refused() {
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path()).await;
    a.create_group().await.unwrap();

    // Reused.
    let ticket = a.create_invite(HOUR, None).unwrap();
    let b = spawn(dirs[1].path()).await;
    b.join(&ticket).await.unwrap();
    let c = spawn(dirs[2].path()).await;
    let err = c.join(&ticket).await.unwrap_err();
    assert!(format!("{err:#}").contains("already used"), "{err:#}");

    // Expired.
    let ticket = a.create_invite(Duration::ZERO, None).unwrap();
    let err = c.join(&ticket).await.unwrap_err();
    assert!(format!("{err:#}").contains("expired"), "{err:#}");

    // Forged: a made-up secret for the right group and inviter.
    let mut forged: InviteTicket = a.create_invite(HOUR, None).unwrap();
    forged.secret = [7; 32];
    let err = c.join(&forged).await.unwrap_err();
    assert!(format!("{err:#}").contains("already used"), "{err:#}");

    assert!(c.group().is_none());
    assert!(!member_ids(&a).await.contains(&c.id()));

    for node in [a, b, c] {
        node.shutdown().await.unwrap();
    }
}

fn refused(err: &ConnectionError) -> bool {
    matches!(err, ConnectionError::ApplicationClosed(close) if close.error_code == NOT_A_MEMBER.into())
}

#[tokio::test]
async fn node_without_record_is_refused_both_ways() {
    let dir = tempfile::tempdir().unwrap();
    let a = spawn(dir.path()).await;
    a.create_group().await.unwrap();

    // A plain endpoint, with no record, that speaks the docs ALPN.
    let outsider = Endpoint::builder(presets::Minimal)
        .alpns(vec![iroh_docs::ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let accepting = outsider.clone();
    tokio::spawn(async move {
        while let Some(incoming) = accepting.accept().await {
            if let Ok(conn) = incoming.await {
                tokio::spawn(async move { conn.closed().await });
            }
        }
    });

    // Outsider dials the member: the member waits for a chain, then refuses.
    let conn = outsider.connect(a.addr(), iroh_docs::ALPN).await.unwrap();
    let err = tokio::time::timeout(TIMEOUT, conn.closed()).await.unwrap();
    assert!(refused(&err), "{err:?}");

    // Member dials the outsider: the outsider can't show a chain, so the
    // member's own hook refuses.
    let result = a.endpoint().connect(outsider.addr(), iroh_docs::ALPN).await;
    match result {
        Err(_) => {}
        Ok(conn) => {
            let err = tokio::time::timeout(TIMEOUT, conn.closed()).await.unwrap();
            assert!(refused(&err), "{err:?}");
        }
    }

    outsider.close().await;
    a.shutdown().await.unwrap();
}

#[tokio::test]
async fn chain_not_reaching_founder_is_refused() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_x = tempfile::tempdir().unwrap();
    let a = spawn(dir_a.path()).await;
    let group = a.create_group().await.unwrap();

    // x claims to have founded a's group.
    let x_key = SecretKey::generate();
    let record = Record {
        group,
        member: x_key.public(),
        added_by: x_key.public(),
        added_at: unix_now(),
    }
    .sign(&x_key);
    Group::founded(record).save_new(dir_x.path()).unwrap();
    // Its stores have no membership doc; the node starts anyway.
    let x = Node::spawn(dir_x.path(), x_key, Network::Local)
        .await
        .unwrap();

    let result = x.endpoint().connect(a.addr(), iroh_docs::ALPN).await;
    match result {
        Err(_) => {}
        Ok(conn) => {
            let err = tokio::time::timeout(TIMEOUT, conn.closed()).await.unwrap();
            assert!(refused(&err), "{err:?}");
        }
    }

    x.shutdown().await.unwrap();
    a.shutdown().await.unwrap();
}

/// Starts a node that publishes its address to `book`, the test's stand-in
/// for n0's DNS, and finds the others there.
async fn spawn_in(dir: &Path, key: SecretKey, book: &MemoryLookup) -> Node {
    let mut config = Config::new(Network::Local);
    config.address_book = Some(book.clone());
    Node::spawn_with(dir, key, config).await.unwrap()
}

#[tokio::test]
async fn members_sync_after_inviter_restarts() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let book = MemoryLookup::new();
    let a = spawn_in(dirs[0].path(), SecretKey::generate(), &book).await;
    let b = spawn_in(dirs[1].path(), SecretKey::generate(), &book).await;
    a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    let (a_key, b_key) = (
        a.endpoint().secret_key().clone(),
        b.endpoint().secret_key().clone(),
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();

    // Both restart: neither remembers admitting the other, and each resumes
    // syncing the membership doc on its own.
    let a = spawn_in(dirs[0].path(), a_key, &book).await;
    let b = spawn_in(dirs[1].path(), b_key, &book).await;

    // A new member joins through a; b only hears of it by syncing with a.
    let c = spawn_in(dirs[2].path(), SecretKey::generate(), &book).await;
    c.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    let expected: BTreeSet<_> = [a.id(), b.id(), c.id()].into();
    wait_for_members(&b, &expected).await;

    for node in [a, b, c] {
        node.shutdown().await.unwrap();
    }
}
