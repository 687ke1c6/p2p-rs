use std::time::Duration;

use iroh::SecretKey;
use iroh_docs::api::protocol::{AddrInfoOptions, ShareMode};
use sync_node::{Network, Node};

const TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::test]
async fn restart_keeps_identity_and_docs() {
    let dir = tempfile::tempdir().unwrap();
    let key = SecretKey::generate();

    let node = Node::spawn(dir.path(), key.clone(), Network::Local)
        .await
        .unwrap();
    let id = node.id();
    let author = node.docs().author_default().await.unwrap();
    let doc = node.docs().create().await.unwrap();
    doc.set_bytes(author, "greeting", "hello").await.unwrap();
    let doc_id = doc.id();
    node.shutdown().await.unwrap();

    let node = Node::spawn(dir.path(), key, Network::Local).await.unwrap();
    assert_eq!(node.id(), id);
    let doc = node
        .docs()
        .open(doc_id)
        .await
        .unwrap()
        .expect("doc persisted");
    let entry = doc
        .get_exact(author, "greeting", false)
        .await
        .unwrap()
        .expect("entry persisted");
    let value = node.blobs().get_bytes(entry.content_hash()).await.unwrap();
    assert_eq!(&value[..], b"hello");
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn two_nodes_sync_a_doc() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let a = Node::spawn(dir_a.path(), SecretKey::generate(), Network::Local)
        .await
        .unwrap();
    let b = Node::spawn(dir_b.path(), SecretKey::generate(), Network::Local)
        .await
        .unwrap();
    // The gate only lets group members sync.
    a.create_group().await.unwrap();
    b.join(&a.create_invite(Duration::from_secs(60), None).unwrap())
        .await
        .unwrap();

    let author = a.docs().author_default().await.unwrap();
    let doc_a = a.docs().create().await.unwrap();
    doc_a.set_bytes(author, "greeting", "hello").await.unwrap();
    let ticket = doc_a
        .share(ShareMode::Write, AddrInfoOptions::Addresses)
        .await
        .unwrap();

    let doc_b = b.docs().import(ticket).await.unwrap();
    let value = tokio::time::timeout(TIMEOUT, async {
        loop {
            if let Some(entry) = doc_b.get_exact(author, "greeting", false).await.unwrap() {
                if let Ok(value) = b.blobs().get_bytes(entry.content_hash()).await {
                    break value;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("entry and content reach node b");
    assert_eq!(&value[..], b"hello");

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
