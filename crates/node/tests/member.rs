use std::collections::BTreeSet;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointId, SecretKey};
use sync_node::gate::JOIN_ALPN;
use sync_node::member::{self, ADDS_DIR};
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
    let adds = dir.join(ADDS_DIR);
    eventually("queued adds to be applied", || async {
        std::fs::read_dir(&adds).map_or(true, |mut entries| entries.next().is_none())
    })
    .await;
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
