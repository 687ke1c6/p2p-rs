//! Traffic counts per connection: the total never goes down while paths
//! open and close.

use sync_node::peers::{PathTotals, Traffic};

fn recv(bytes: u64) -> Traffic {
    Traffic {
        direct_recv: bytes,
        ..Traffic::default()
    }
}

#[test]
fn a_path_gone_before_its_close_is_handled_still_counts() {
    let mut totals = PathTotals::default();
    totals.observe(1, recv(10 << 20));
    totals.observe(2, recv(100));
    assert_eq!(totals.total(), recv((10 << 20) + 100));
    // Path 1 is no longer listed; its close hasn't been handled. Only path 2
    // is observed, and path 1 keeps its bytes.
    totals.observe(2, recv(200));
    assert_eq!(totals.total(), recv((10 << 20) + 200));
    // The close brings its final bytes, counted once.
    totals.path_closed(1, recv((10 << 20) + 5));
    totals.path_closed(1, recv((10 << 20) + 5));
    assert_eq!(totals.total(), recv((10 << 20) + 205));
    // A closed path observed again (a late snapshot) changes nothing.
    totals.observe(1, recv(1));
    assert_eq!(totals.total(), recv((10 << 20) + 205));
}

#[test]
fn closing_without_final_bytes_keeps_the_bytes_last_seen() {
    let mut totals = PathTotals::default();
    totals.observe(1, recv(1000));
    totals.observe(2, recv(50));
    // Final bytes for path 2 only.
    assert_eq!(totals.finish([(2, recv(60))]), recv(1060));
}
