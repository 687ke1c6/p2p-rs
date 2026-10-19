use std::path::Path;
use std::time::{Duration, Instant};

use iroh::address_lookup::memory::MemoryLookup;
use iroh::SecretKey;
use sync_node::presence::{LinkState, OFFLINE_AFTER_INTERVALS};
use sync_node::status::STATUS_FILE;
use sync_node::{Config, Network, Node, Status};

const INTERVAL: Duration = Duration::from_millis(200);
const TIMEOUT: Duration = Duration::from_secs(20);
const HOUR: Duration = Duration::from_secs(3600);

async fn spawn(dir: &Path) -> Node {
    let config = Config {
        network: Network::Local,
        presence_interval: INTERVAL,
        debounce: Duration::from_millis(100),
        address_book: None,
        content: false,
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

/// Waits until `check` holds for `node`'s status, or fails with the last one.
async fn wait_for(node: &Node, what: &str, check: impl Fn(&Status) -> bool) -> Duration {
    let start = Instant::now();
    loop {
        let status = node.status().expect("in a group");
        if check(&status) {
            return start.elapsed();
        }
        if start.elapsed() > TIMEOUT {
            panic!(
                "{}: {what}; status:\n{}",
                node.id().fmt_short(),
                status.render()
            );
        }
        tokio::time::sleep(INTERVAL / 4).await;
    }
}

#[tokio::test]
async fn three_nodes_agree_then_notice_one_leaving() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = spawn(dirs[0].path()).await;
    let b = spawn(dirs[1].path()).await;
    let c = spawn(dirs[2].path()).await;
    learn_addrs(&[&a, &b, &c]);
    a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    c.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    let ids = [a.id(), b.id(), c.id()];

    // Every node sees all three members online and every pair direct.
    for node in [&a, &b, &c] {
        wait_for(node, "all online and direct", |status| {
            status.members.len() == 3
                && status.members.iter().all(|m| m.online)
                && status.links.len() == 3
                && status.links.iter().all(|l| l.state == LinkState::Direct)
        })
        .await;
    }

    // a's own connection to b carried traffic, all of it direct.
    let status = a.status().unwrap();
    let peer = status.member(b.id()).unwrap().peer.clone().unwrap();
    assert!(peer.connections > 0);
    assert!(peer.traffic.direct_sent > 0 && peer.traffic.direct_recv > 0);
    assert_eq!(peer.traffic.relay_sent + peer.traffic.relay_recv, 0);

    // The status file catches up within an interval or two.
    let path = dirs[0].path().join(STATUS_FILE);
    let start = Instant::now();
    let text = loop {
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        if text.contains("members (3, 3 online)") || start.elapsed() > TIMEOUT {
            break text;
        }
        tokio::time::sleep(INTERVAL / 4).await;
    };
    assert!(text.contains("members (3, 3 online)"), "{text}");
    for id in ids {
        assert!(text.contains(&id.fmt_short().to_string()), "{text}");
    }

    // c leaves; a and b notice within the offline window plus a little.
    let c_id = c.id();
    c.shutdown().await.unwrap();
    let limit = INTERVAL * (OFFLINE_AFTER_INTERVALS + 3);
    for node in [&a, &b] {
        let took = wait_for(node, "c offline", |status| {
            !status.member(c_id).unwrap().online
                && status.link(a.id(), c_id) == Some(LinkState::Unknown)
                && status.link(b.id(), c_id) == Some(LinkState::Unknown)
                && status.link(a.id(), b.id()) == Some(LinkState::Direct)
        })
        .await;
        assert!(took < limit, "took {took:?}, limit {limit:?}");
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
