use std::collections::BTreeSet;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{presets, ConnectionError};
use iroh::{Endpoint, EndpointId, SecretKey};
use sync_node::gate::{JOIN_ALPN, NOT_A_MEMBER};
use sync_node::member::{self, RemoveError, ADDS_DIR, REMOVES_DIR};
use sync_node::{Config, Network, Node};

const INTERVAL: Duration = Duration::from_millis(200);
const TIMEOUT: Duration = Duration::from_secs(20);
const HOUR: Duration = Duration::from_secs(3600);

async fn spawn(dir: &Path) -> Node {
    let config = Config {
        network: Network::Local,
        presence_interval: INTERVAL,
        debounce: Duration::from_millis(100),
        address_book: None,
        direct_only: false,
    };
    Node::spawn_with(dir, SecretKey::generate(), config)
        .await
        .unwrap()
}

/// Lets each node find the others by ID alone, as n0's DNS does outside
/// tests.
fn learn_addrs(nodes: &[&Node]) {
    for node in nodes {
        let lookup = MemoryLookup::new();
        for peer in nodes.iter().filter(|peer| peer.id() != node.id()) {
            lookup.add_endpoint_info(peer.addr());
        }
        node.endpoint().address_lookup().unwrap().add(lookup);
    }
}

/// Waits until `check` holds, or panics with `what`.
async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while !check().await {
        assert!(start.elapsed() < TIMEOUT, "timed out waiting for {what}");
        tokio::time::sleep(INTERVAL / 4).await;
    }
}

async fn member_ids(node: &Node) -> BTreeSet<EndpointId> {
    node.members()
        .await
        .unwrap()
        .into_iter()
        .map(|signed| signed.record.member)
        .collect()
}

/// Waits until the running node has applied everything queued in `adds/`.
async fn wait_for_adds_applied(dir: &Path) {
    wait_for_queue(&dir.join(ADDS_DIR)).await;
}

async fn wait_for_queue(queue: &Path) {
    eventually("queued requests to be applied", || async {
        std::fs::read_dir(queue).map_or(true, |mut entries| entries.next().is_none())
    })
    .await;
}

/// Whether a docs connection from `from` to `to` is refused, by either
/// side's gate.
async fn docs_refused(from: &Node, to: &Node) -> bool {
    let refused = |err: &ConnectionError| {
        matches!(err, ConnectionError::ApplicationClosed(close)
            if close.error_code == NOT_A_MEMBER.into())
    };
    match from.endpoint().connect(to.addr(), iroh_docs::ALPN).await {
        Err(_) => true,
        Ok(conn) => match tokio::time::timeout(TIMEOUT, conn.closed()).await {
            Ok(err) => refused(&err),
            Err(_) => false,
        },
    }
}

#[tokio::test]
async fn node_added_by_id_joins_through_either_member() {
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path()).await;
    let b = spawn(dirs[1].path()).await;
    let c = spawn(dirs[2].path()).await;
    let d = spawn(dirs[3].path()).await;
    learn_addrs(&[&a, &b, &c, &d]);
    let group = a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();

    // c is added on a and joins through a, with no invite.
    member::queue_add(dirs[0].path(), c.id(), Some("carol")).unwrap();
    eventually("a to add c", || async {
        member_ids(&a).await.contains(&c.id())
    })
    .await;
    assert_eq!(c.join(a.addr()).await.unwrap(), group);

    // d is added on a and joins through b, once b has the record.
    member::queue_add(dirs[0].path(), d.id(), None).unwrap();
    eventually("b to hear d was added", || async {
        member_ids(&b).await.contains(&d.id())
    })
    .await;
    assert_eq!(d.join(b.addr()).await.unwrap(), group);

    let expected: BTreeSet<_> = [a.id(), b.id(), c.id(), d.id()].into();
    for node in [&a, &b, &c, &d] {
        eventually("every node to have every member", || async {
            member_ids(node).await == expected
        })
        .await;
        eventually("every node to know c's name", || async {
            node.name(c.id()).as_deref() == Some("carol")
        })
        .await;
    }

    for node in [a, b, c, d] {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn removed_member_is_cut_off_with_the_members_it_added() {
    let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path()).await;
    let b = spawn(dirs[1].path()).await;
    let c = spawn(dirs[2].path()).await;
    let d = spawn(dirs[3].path()).await;
    learn_addrs(&[&a, &b, &c, &d]);
    a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    c.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    // d is b's: its chain goes through b.
    d.join(&b.create_invite(HOUR, None).unwrap()).await.unwrap();
    let all: BTreeSet<_> = [a.id(), b.id(), c.id(), d.id()].into();
    for node in [&a, &c] {
        eventually("a and c to have every member", || async {
            member_ids(node).await == all
        })
        .await;
    }

    // c removes b: d goes with it, on every member.
    member::queue_remove(dirs[2].path(), b.id()).unwrap();
    let left: BTreeSet<_> = [a.id(), c.id()].into();
    for node in [&a, &c] {
        eventually("b and d to be gone", || async {
            member_ids(node).await == left
        })
        .await;
        let removed = node.membership().await.unwrap().removed;
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].removal.member, b.id());
        assert_eq!(removed[0].removal.removed_by, c.id());
        // The status is the monitor's view, which can lag the doc by an
        // interval.
        eventually("the status to list b as removed", || async {
            node.status().unwrap().render().contains("removed (1):")
        })
        .await;
    }

    // Neither side lets a docs connection through.
    eventually("b to be refused both ways", || async {
        docs_refused(&b, &a).await && docs_refused(&a, &b).await
    })
    .await;

    // b can't be added back; d can, by a member that is still one.
    member::queue_add(dirs[0].path(), b.id(), None).unwrap();
    member::queue_add(dirs[0].path(), d.id(), None).unwrap();
    let back: BTreeSet<_> = [a.id(), c.id(), d.id()].into();
    for node in [&a, &c] {
        eventually("d to be back", || async { member_ids(node).await == back }).await;
    }
    tokio::time::sleep(INTERVAL * 3).await;
    assert_eq!(member_ids(&a).await, back);

    for node in [a, b, c, d] {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn founder_and_self_cant_be_removed() {
    let (founder, me, other) = (
        SecretKey::generate().public(),
        SecretKey::generate().public(),
        SecretKey::generate().public(),
    );
    assert_eq!(
        member::check_removable(founder, me, founder),
        Err(RemoveError::Founder)
    );
    assert_eq!(
        member::check_removable(me, me, founder),
        Err(RemoveError::Itself)
    );
    assert_eq!(member::check_removable(other, me, founder), Ok(()));

    // A queued removal that gets past the CLI is refused by the running node.
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path()).await;
    let b = spawn(dirs[1].path()).await;
    learn_addrs(&[&a, &b]);
    a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    member::queue_remove(dirs[1].path(), a.id()).unwrap();
    member::queue_remove(dirs[1].path(), b.id()).unwrap();
    wait_for_queue(&dirs[1].path().join(REMOVES_DIR)).await;
    tokio::time::sleep(INTERVAL * 3).await;
    let both: BTreeSet<_> = [a.id(), b.id()].into();
    for node in [&a, &b] {
        assert_eq!(member_ids(node).await, both);
        assert!(node.membership().await.unwrap().removed.is_empty());
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_name_spreads_and_changes() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path()).await;
    let b = spawn(dirs[1].path()).await;
    learn_addrs(&[&a, &b]);
    a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    let both = [&a, &b];
    let named_everywhere = |name: &'static str| async move {
        for node in both {
            eventually(
                &format!("{} to call the group {name}", node.id()),
                || async { node.group_name().as_deref() == Some(name) },
            )
            .await;
        }
    };

    // Any member names it, and any member renames it.
    member::queue_group_name(dirs[1].path(), "home").unwrap();
    named_everywhere("home").await;
    member::queue_group_name(dirs[0].path(), "family").unwrap();
    named_everywhere("family").await;
    let group = a.group().unwrap().id;
    assert!(a
        .status()
        .unwrap()
        .render()
        .contains(&format!("group   family ({group})")));

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn id_not_in_the_group_is_refused() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path()).await;
    let x = spawn(dirs[1].path()).await;
    a.create_group().await.unwrap();

    let err = x.join(a.addr()).await.unwrap_err();
    assert!(format!("{err:#}").contains("not a member"), "{err:#}");
    assert!(x.group().is_none());
    assert!(!member_ids(&a).await.contains(&x.id()));

    a.shutdown().await.unwrap();
    x.shutdown().await.unwrap();
}

#[tokio::test]
async fn names_spread_change_and_stay_unique() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path()).await;
    let b = spawn(dirs[1].path()).await;
    learn_addrs(&[&a, &b]);
    a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, Some("bee")).unwrap())
        .await
        .unwrap();
    let both = [&a, &b];
    let name_everywhere = |id: EndpointId, name: &'static str| async move {
        for node in both {
            eventually(&format!("{} to call {id} {name}", node.id()), || async {
                node.name(id).as_deref() == Some(name)
            })
            .await;
        }
    };

    // Named by the invite.
    name_everywhere(b.id(), "bee").await;

    // Renamed on a, by re-adding an existing member.
    member::queue_add(dirs[0].path(), b.id(), Some("bumble")).unwrap();
    name_everywhere(b.id(), "bumble").await;
    assert!(a.status().unwrap().render().contains("bumble"));

    // b names a: any member can name any member, itself included.
    member::queue_add(dirs[1].path(), a.id(), Some("ay")).unwrap();
    name_everywhere(a.id(), "ay").await;

    // a can't take b's name.
    member::queue_add(dirs[0].path(), a.id(), Some("bumble")).unwrap();
    wait_for_adds_applied(dirs[0].path()).await;
    tokio::time::sleep(INTERVAL * 3).await;
    for node in both {
        assert_eq!(node.name(a.id()).as_deref(), Some("ay"));
        assert_eq!(node.name(b.id()).as_deref(), Some("bumble"));
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn idle_join_connection_does_not_block_joins() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path()).await;
    let b = spawn(dirs[1].path()).await;
    a.create_group().await.unwrap();

    // Connects on the join ALPN and never sends a request.
    let idle = Endpoint::builder(presets::Minimal).bind().await.unwrap();
    let _conn = idle.connect(a.addr(), JOIN_ALPN).await.unwrap();

    let ticket = a.create_invite(HOUR, None).unwrap();
    tokio::time::timeout(Duration::from_secs(5), b.join(&ticket))
        .await
        .expect("join blocked by the idle connection")
        .unwrap();

    idle.close().await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
