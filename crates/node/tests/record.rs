use iroh::SecretKey;
use iroh_docs::NamespaceSecret;
use sync_node::record::{chain_of, validate_chain, ChainError, GroupId, Record, SignedRecord};

fn group() -> GroupId {
    NamespaceSecret::from_bytes(&rand::random()).id()
}

fn record(group: GroupId, member: &SecretKey, signer: &SecretKey) -> SignedRecord {
    Record {
        group,
        member: member.public(),
        added_by: signer.public(),
        added_at: 1,
    }
    .sign(signer)
}

#[test]
fn chain_to_founder_is_valid() {
    let group = group();
    let (f, a, b) = (
        SecretKey::generate(),
        SecretKey::generate(),
        SecretKey::generate(),
    );
    let records = [
        record(group, &f, &f),
        record(group, &a, &f),
        record(group, &b, &a),
    ];
    for member in [&f, &a, &b] {
        assert_eq!(
            validate_chain(group, f.public(), member.public(), &records),
            Ok(())
        );
    }
    let chain = chain_of(b.public(), &records);
    let members: Vec<_> = chain.iter().map(|signed| signed.record.member).collect();
    assert_eq!(members, [b.public(), a.public(), f.public()]);
}

#[test]
fn chain_to_someone_else_is_refused() {
    let group = group();
    let (f, x, n) = (
        SecretKey::generate(),
        SecretKey::generate(),
        SecretKey::generate(),
    );
    // x claims to found the group and adds n.
    let records = [record(group, &x, &x), record(group, &n, &x)];
    assert_eq!(
        validate_chain(group, f.public(), n.public(), &records),
        Err(ChainError::NotFounder(x.public()))
    );
}

#[test]
fn forged_signature_is_refused() {
    let group = group();
    let (f, a, x) = (
        SecretKey::generate(),
        SecretKey::generate(),
        SecretKey::generate(),
    );
    // x signs a record that claims f added it.
    let mut forged = record(group, &x, &x);
    forged.record.added_by = f.public();
    let records = [record(group, &f, &f), record(group, &a, &f), forged];
    assert_eq!(
        validate_chain(group, f.public(), x.public(), &records),
        Err(ChainError::BadSignature(x.public()))
    );
}

#[test]
fn record_for_another_group_is_refused() {
    let (group, other) = (group(), group());
    let (f, a) = (SecretKey::generate(), SecretKey::generate());
    let records = [record(group, &f, &f), record(other, &a, &f)];
    assert_eq!(
        validate_chain(group, f.public(), a.public(), &records),
        Err(ChainError::WrongGroup(a.public()))
    );
}

#[test]
fn missing_link_and_loop_are_refused() {
    let group = group();
    let (f, a, b) = (
        SecretKey::generate(),
        SecretKey::generate(),
        SecretKey::generate(),
    );
    let records = [record(group, &f, &f), record(group, &b, &a)];
    assert_eq!(
        validate_chain(group, f.public(), b.public(), &records),
        Err(ChainError::Missing(a.public()))
    );
    let records = [record(group, &a, &b), record(group, &b, &a)];
    assert_eq!(
        validate_chain(group, f.public(), a.public(), &records),
        Err(ChainError::Loop(a.public()))
    );
}

#[test]
fn record_round_trips() {
    let group = group();
    let f = SecretKey::generate();
    let signed = record(group, &f, &f);
    let parsed = SignedRecord::from_bytes(&signed.to_bytes()).unwrap();
    assert_eq!(parsed, signed);
    assert!(parsed.verify_signature());
}
