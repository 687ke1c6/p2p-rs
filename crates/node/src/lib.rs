//! Local-first folder sync over iroh. See `plans/sync-plan.md`.

pub mod folder;
pub mod gate;
pub mod group;
pub mod index;
pub mod invite;
pub mod join;
pub mod key;
pub mod member;
pub(crate) mod monitor;
pub mod node;
pub mod peers;
pub mod presence;
pub mod record;
pub mod status;
pub mod view;

pub use group::Group;
pub use invite::InviteTicket;
pub use join::Ticket;
pub use node::{Config, Network, Node};
pub use status::Status;
