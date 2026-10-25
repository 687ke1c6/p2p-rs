//! One node per config directory: the directory's lock.

use iroh::SecretKey;
use sync_node::lock::{self, LockError};
use sync_node::{Network, Node};

async fn spawn(dir: &std::path::Path) -> anyhow::Result<Node> {
    Node::spawn(dir, SecretKey::generate(), Network::Local).await
}

#[tokio::test]
async fn a_second_node_on_the_same_dir_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    // Never used: no lock to hold.
    assert!(!lock::is_held(dir.path()).unwrap());

    let first = spawn(dir.path()).await.unwrap();
    assert!(lock::is_held(dir.path()).unwrap());
    let Err(err) = spawn(dir.path()).await else {
        panic!("a second node started on the same dir");
    };
    assert!(
        matches!(err.downcast_ref::<LockError>(), Some(LockError::Held(_))),
        "{err:#}"
    );
    assert!(format!("{err:#}").contains("stop it first"), "{err:#}");
    // Asking doesn't take it: still held by the first node.
    assert!(lock::is_held(dir.path()).unwrap());

    // Let go on shutdown, at once.
    first.shutdown().await.unwrap();
    assert!(!lock::is_held(dir.path()).unwrap());
    let again = spawn(dir.path()).await.unwrap();
    assert!(lock::is_held(dir.path()).unwrap());

    // And when the node is dropped without a shutdown.
    drop(again);
    assert!(!lock::is_held(dir.path()).unwrap());
}
