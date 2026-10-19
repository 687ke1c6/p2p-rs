//! Nodes that start unable to reach each other find each other once they
//! can, without a restart. iroh-gossip 0.101.0 never redials a peer whose
//! first dial failed; `patches/iroh-gossip` fixes that.

use std::path::Path;
use std::time::{Duration, Instant};

use iroh::address_lookup::memory::MemoryLookup;
use iroh::SecretKey;
use sync_node::{Config, Network, Node};

const INTERVAL: Duration = Duration::from_millis(200);
const TIMEOUT: Duration = Duration::from_secs(20);
const HOUR: Duration = Duration::from_secs(3600);

async fn spawn(dir: &Path, key: SecretKey, book: Option<&MemoryLookup>) -> Node {
    let config = Config {
        network: Network::Local,
        presence_interval: INTERVAL,
        debounce: Duration::from_millis(100),
        address_book: book.cloned(),
        content: false,
    };
    Node::spawn_with(dir, key, config).await.unwrap()
}

/// Whether `node` sees `other` online.
fn sees_online(node: &Node, other: &Node) -> bool {
    node.status()
        .and_then(|s| s.member(other.id()).map(|m| m.online))
        .unwrap_or(false)
}

#[tokio::test]
async fn nodes_apart_at_start_find_each_other() {
    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let (ka, kb) = (SecretKey::generate(), SecretKey::generate());

    // Form the group while they can reach each other.
    let book = MemoryLookup::new();
    let a = spawn(dirs[0].path(), ka.clone(), Some(&book)).await;
    let b = spawn(dirs[1].path(), kb.clone(), Some(&book)).await;
    a.create_group().await.unwrap();
    b.join(&a.create_invite(HOUR, None).unwrap()).await.unwrap();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();

    // Restart with no way to find each other: every dial fails.
    let a = spawn(dirs[0].path(), ka, None).await;
    let b = spawn(dirs[1].path(), kb, None).await;
    tokio::time::sleep(INTERVAL * 5).await;
    assert!(!sees_online(&a, &b) && !sees_online(&b, &a));

    // The network comes back: each learns the other's address.
    for (node, peer) in [(&a, &b), (&b, &a)] {
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(peer.addr());
        node.endpoint().address_lookup().unwrap().add(lookup);
    }
    let start = Instant::now();
    while !(sees_online(&a, &b) && sees_online(&b, &a)) {
        assert!(
            start.elapsed() < TIMEOUT,
            "the nodes didn't find each other:\n{}\n{}",
            a.status().unwrap().render(),
            b.status().unwrap().render()
        );
        tokio::time::sleep(INTERVAL / 2).await;
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
