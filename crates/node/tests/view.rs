//! The merge rules, on hand-made entries.

use iroh::{EndpointId, SecretKey};
use iroh_blobs::Hash;
use sync_node::blocks::Hasher;
use sync_node::index::FileMeta;
use sync_node::view::{
    conflict_name, merge, node_state, render_entries, Candidate, NodeState, Outcome, Version,
};

fn node() -> EndpointId {
    SecretKey::generate().public()
}

fn v(edits: &[(EndpointId, u64)]) -> Version {
    Version(edits.iter().copied().collect())
}

fn file(node: EndpointId, content: &str, mtime_ns: u64, version: Version) -> FileMeta {
    let mut hasher = Hasher::new(content.len() as u64);
    hasher.update(content.as_bytes());
    FileMeta::file(
        hasher.finish(),
        content.len() as u64,
        mtime_ns,
        node,
        version,
    )
}

fn deleted(node: EndpointId, version: Version) -> FileMeta {
    FileMeta {
        mtime_ns: 0,
        ..FileMeta::tombstone(node, version)
    }
}

fn content(outcome: &Outcome) -> Option<Hash> {
    match outcome {
        Outcome::File(c) => Some(c.content),
        _ => None,
    }
}

#[test]
fn version_order() {
    let (a, b) = (node(), node());
    assert!(v(&[(a, 2)]).after(&v(&[(a, 1)])));
    assert!(v(&[(a, 1), (b, 1)]).after(&v(&[(b, 1)])));
    assert!(!v(&[(a, 1)]).after(&v(&[(b, 1)])));
    assert!(!v(&[(b, 1)]).after(&v(&[(a, 1)])));
    assert!(!v(&[(a, 1)]).after(&v(&[(a, 1)])));
    assert_eq!(v(&[(a, 1)]).bumped(b), v(&[(a, 1), (b, 1)]));
}

#[test]
fn one_node_or_same_content_is_that_file() {
    let (a, b) = (node(), node());
    let only = [file(a, "x", 1, v(&[(a, 1)]))];
    assert_eq!(content(&merge(&only).unwrap()), Some(Hash::new("x")));
    // Made independently, same content: no conflict.
    let same = [file(a, "x", 1, v(&[(a, 1)])), file(b, "x", 2, v(&[(b, 1)]))];
    assert_eq!(content(&merge(&same).unwrap()), Some(Hash::new("x")));
    assert!(merge(&[]).is_none());
}

#[test]
fn a_later_version_wins_however_many_edits_were_skipped() {
    let (a, b) = (node(), node());
    // a edited three times after taking b's version in; b still holds it.
    let entries = [
        file(a, "v4", 1, v(&[(a, 3), (b, 1)])),
        file(b, "v1", 9, v(&[(b, 1)])),
    ];
    assert_eq!(content(&merge(&entries).unwrap()), Some(Hash::new("v4")));
}

#[test]
fn concurrent_edits_conflict_and_the_newest_keeps_the_name() {
    let (a, b) = (node(), node());
    let entries = [
        file(a, "mine", 10, v(&[(a, 1)])),
        file(b, "theirs", 20, v(&[(b, 1)])),
    ];
    let Outcome::Conflict { winner, others } = merge(&entries).unwrap() else {
        panic!("expected a conflict");
    };
    assert_eq!(winner.node, b);
    assert_eq!(others.len(), 1);
    assert_eq!(others[0].node, a);

    // Same modification time: the smaller node ID keeps the name.
    let entries = [
        file(a, "mine", 10, v(&[(a, 1)])),
        file(b, "theirs", 10, v(&[(b, 1)])),
    ];
    let Outcome::Conflict { winner, .. } = merge(&entries).unwrap() else {
        panic!("expected a conflict");
    };
    assert_eq!(winner.node, a.min(b));
}

#[test]
fn deletes() {
    let (a, b) = (node(), node());
    // A delete after the version the other holds: deleted.
    let entries = [deleted(a, v(&[(a, 2)])), file(b, "x", 1, v(&[(a, 1)]))];
    assert_eq!(merge(&entries).unwrap(), Outcome::Deleted);
    // An edit concurrent with a delete: the edit wins.
    let entries = [
        deleted(a, v(&[(a, 2)])),
        file(b, "edited", 1, v(&[(a, 1), (b, 1)])),
    ];
    assert_eq!(
        content(&merge(&entries).unwrap()),
        Some(Hash::new("edited"))
    );
    // Deleted on both, independently: deleted.
    let entries = [deleted(a, v(&[(a, 2)])), deleted(b, v(&[(a, 1), (b, 1)]))];
    assert_eq!(merge(&entries).unwrap(), Outcome::Deleted);
}

#[test]
fn where_each_node_stands() {
    let (a, b) = (node(), node());
    let x = file(a, "x", 1, v(&[(a, 1)]));
    let outcome = merge(std::slice::from_ref(&x)).unwrap();
    assert_eq!(node_state(Some(&x), &outcome), NodeState::Current);
    assert_eq!(node_state(None, &outcome), NodeState::Missing);
    let old = file(b, "old", 1, v(&[]));
    assert_eq!(node_state(Some(&old), &outcome), NodeState::Outdated);
    assert_eq!(
        node_state(Some(&x), &Outcome::Deleted),
        NodeState::WillDelete
    );
}

#[test]
fn conflict_names() {
    // 2026-10-18 14:30:22 UTC.
    let mtime_ns = 1_792_333_822 * 1_000_000_000;
    let candidate = Candidate {
        node: node(),
        content: Hash::new("x"),
        size: 1,
        mtime_ns,
    };
    assert_eq!(
        conflict_name("trip/b.jpg", &candidate, "pi"),
        "trip/b.sync-conflict-20261018-143022-pi.jpg"
    );
    assert_eq!(
        conflict_name("notes", &candidate, "pi 4"),
        "notes.sync-conflict-20261018-143022-pi 4"
    );
    assert_eq!(
        conflict_name(".bashrc", &candidate, "a/b"),
        ".bashrc.sync-conflict-20261018-143022-a_b"
    );
    assert_eq!(
        conflict_name("a.tar.gz", &candidate, "pi"),
        "a.tar.sync-conflict-20261018-143022-pi.gz"
    );
}

#[test]
fn raw_entries_render() {
    let (a, b) = (node(), node());
    let label = |id: EndpointId| {
        if id == a {
            "a".to_string()
        } else {
            "bb".to_string()
        }
    };
    // 2026-10-19 10:01:02.000000007 UTC.
    let mtime = 1_792_404_062_000_000_007;
    let entries = [
        (
            "dir/x.txt".to_string(),
            [
                (a, file(a, "x", mtime, v(&[(a, 2), (b, 1)]))),
                (b, deleted(b, v(&[(b, 1)]))),
            ]
            .into_iter()
            .collect(),
        ),
        (
            "y".to_string(),
            [(b, file(b, "yy", mtime, v(&[(b, 3)])))]
                .into_iter()
                .collect(),
        ),
    ]
    .into_iter()
    .collect();
    let text = render_entries(&entries, &label);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], "3 entries for 2 paths");
    assert_eq!(lines[2], "dir/x.txt");
    // Entries and version vectors are ordered by node ID, not by label.
    let (a_version, b_first) = if a < b {
        ("{a:2, bb:1}", false)
    } else {
        ("{bb:1, a:2}", true)
    };
    let a_line = format!(
        "  a   {}           1 B      1 × 128K  20261019-100102.000000007  {a_version}",
        Hash::new("x")
    );
    let b_line = format!(
        "  bb  {:64}           0 B             -  19700101-000000.000000000  {{bb:1}}",
        "deleted"
    );
    let expected = if b_first {
        [&b_line, &a_line]
    } else {
        [&a_line, &b_line]
    };
    assert_eq!(lines[3], expected[0], "{text}");
    assert_eq!(lines[4], expected[1], "{text}");
    assert_eq!(lines[6], "y");
    assert!(lines[7].starts_with("  bb  "), "{text}");
}
