use std::time::Duration;

use iroh::{EndpointAddr, SecretKey};
use iroh_docs::NamespaceSecret;
use sync_node::invite::{self, InviteError, InviteTicket};
use sync_node::Ticket;

fn group() -> sync_node::record::GroupId {
    NamespaceSecret::from_bytes(&rand::random()).id()
}

#[test]
fn ticket_round_trips_as_text() {
    let dir = tempfile::tempdir().unwrap();
    let inviter = EndpointAddr::new(SecretKey::generate().public());
    let ticket =
        invite::create(dir.path(), inviter, group(), Duration::from_secs(60), None).unwrap();
    let text = ticket.to_string();
    assert!(text.starts_with("invite"));
    assert_eq!(text.parse::<InviteTicket>().unwrap(), ticket);
    assert!("invitexyz".parse::<InviteTicket>().is_err());
}

#[test]
fn invite_works_once() {
    let dir = tempfile::tempdir().unwrap();
    let inviter = EndpointAddr::new(SecretKey::generate().public());
    let ticket =
        invite::create(dir.path(), inviter, group(), Duration::from_secs(60), None).unwrap();
    assert_eq!(invite::redeem(dir.path(), &ticket.secret).unwrap(), None);
    assert!(matches!(
        invite::redeem(dir.path(), &ticket.secret),
        Err(InviteError::Unknown)
    ));
}

#[test]
fn expired_and_unknown_invites_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let inviter = EndpointAddr::new(SecretKey::generate().public());
    let ticket = invite::create(dir.path(), inviter, group(), Duration::ZERO, None).unwrap();
    assert!(matches!(
        invite::redeem(dir.path(), &ticket.secret),
        Err(InviteError::Expired)
    ));
    assert!(matches!(
        invite::redeem(dir.path(), &[7; 32]),
        Err(InviteError::Unknown)
    ));
}

#[test]
fn invite_carries_the_new_members_name() {
    let dir = tempfile::tempdir().unwrap();
    let inviter = EndpointAddr::new(SecretKey::generate().public());
    let ticket = invite::create(
        dir.path(),
        inviter,
        group(),
        Duration::from_secs(60),
        Some("pi"),
    )
    .unwrap();
    assert_eq!(
        invite::redeem(dir.path(), &ticket.secret).unwrap(),
        Some("pi".to_string())
    );
}

#[test]
fn join_takes_an_invite_or_a_node_id() {
    let dir = tempfile::tempdir().unwrap();
    let member = SecretKey::generate().public();
    let invite = invite::create(
        dir.path(),
        EndpointAddr::new(member),
        group(),
        Duration::from_secs(60),
        None,
    )
    .unwrap();
    assert_eq!(
        invite.to_string().parse::<Ticket>().unwrap(),
        Ticket::Invite(invite)
    );
    assert_eq!(
        member.to_string().parse::<Ticket>().unwrap(),
        Ticket::Member(EndpointAddr::new(member))
    );
    assert!("invitexyz".parse::<Ticket>().is_err());
    assert!("not-a-node-id".parse::<Ticket>().is_err());
}
