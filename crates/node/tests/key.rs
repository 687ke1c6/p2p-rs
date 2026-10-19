use std::fs;
use std::os::unix::fs::PermissionsExt;

use sync_node::key::{self, KeyError, KEY_FILE};

#[test]
fn init_then_load_returns_the_same_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested").join(KEY_FILE);
    let created = key::init(&path).unwrap();
    let loaded = key::load(&path).unwrap();
    assert_eq!(created.public(), loaded.public());
    let mode = fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(
        mode & 0o777,
        0o600,
        "key must be readable by the owner only"
    );
    let mode = fs::metadata(path.parent().unwrap())
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o700, "config dir must be owner-only");
}

#[test]
fn init_refuses_to_replace_a_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(KEY_FILE);
    let first = key::init(&path).unwrap();
    assert!(matches!(key::init(&path), Err(KeyError::Exists(_))));
    assert_eq!(key::load(&path).unwrap().public(), first.public());
}

#[test]
fn missing_key_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(KEY_FILE);
    assert!(matches!(key::load(&path), Err(KeyError::Missing(_))));
    assert!(!path.exists(), "load must not create a key");
}

#[test]
fn corrupt_key_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(KEY_FILE);
    fs::write(&path, [7u8; 31]).unwrap();
    assert!(matches!(
        key::load(&path),
        Err(KeyError::Corrupt { len: 31, .. })
    ));
}

#[test]
fn load_or_init_creates_once_then_reuses() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested").join(KEY_FILE);
    let (first, created) = key::load_or_init(&path).unwrap();
    assert!(created);
    let (second, created) = key::load_or_init(&path).unwrap();
    assert!(!created);
    assert_eq!(first.public(), second.public());
}

#[test]
fn load_or_init_never_replaces_a_corrupt_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(KEY_FILE);
    fs::write(&path, [7u8; 31]).unwrap();
    assert!(matches!(
        key::load_or_init(&path),
        Err(KeyError::Corrupt { len: 31, .. })
    ));
    assert_eq!(fs::read(&path).unwrap(), [7u8; 31]);
}
