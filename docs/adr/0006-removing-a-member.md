---
status: proposed
---

# Removing a member: a signed removal that always wins, and cascades

Any member removes any other with a **removal**: "`removed_by` removed `member` from `group`", signed by `removed_by`, stored in the membership doc under `removed/<id>` (`record::SignedRemoval`). `member remove <id>` and `POST /api/members/remove` queue it as `removes/<id>`, and the monitor writes it, as for adds. The founder can't be removed, and a node can't remove itself.

A removal counts if its signature checks out, it is for this group, it doesn't remove the founder, and its signer has a chain of records to the founder. A node is a member if it isn't removed and a member signed a record for it, from the founder out (`node::read_membership`). So:

- **A removal always wins.** The removed ID can't come back: a later record for it doesn't count, and `member add` and invites refuse it. The node can join again with a new ID (`--clear-config`).
- **It cascades.** A member whose chain goes through the removed node is no longer one, unless another member adds it again (a new record, signed by a member that is still one).
- **The signer's own removal doesn't undo it.** Two members that remove each other at once are both removed.

The gate never admits a removed node, whatever chain it presents (`Gate::set_removed`), and the monitor closes the connections it has open. Members drop its presence, and its folder and membership syncs fail at the gate.

Proposed 2026-10-23, from the sync plan's open item **Removing a member** ("the likely default is that any member may remove any other, and a removal always wins"). To be confirmed with the developer.

## Why

- Without the cascade, a removed node could still sign records for new nodes, and those would be members: its key is the only thing a record checks.
- "Always wins" needs no ordering between nodes' clocks: a removal and an add can race on two nodes, and each node computes the same members from the same doc.
- Who may remove whom stays as simple as who may add whom: any member, any member.

## Considered Options

- **No cascade**: the members a removed node added stay members. Fewer surprises, but the removed node keeps the power to add members, as above.
- **Only the founder (or the adder) may remove**: needs roles the group doesn't have; a group whose founder is gone could never remove anyone.
- **A removal may be undone** (an add after it wins): needs an order of events, so trusted timestamps or a counter; the removed node could re-add itself through a member that hasn't seen the removal yet.

## Consequences

- **Data already sent stays.** The removed node keeps its copy of every folder and the docs' secrets. Removal stops it syncing from now on; it does not rotate keys or erase anything. Replacing a folder's doc after a removal is a later step.
- **Removals by removed nodes in transit**: a removed node can still sign removals, and a member that hasn't seen its removal yet admits it and takes them in. They count if its chain is good. The window closes once every member has the removal.
- The removed node learns of its removal only if it syncs the membership doc before it is cut off; then it logs it. Otherwise it sees refused connections.
- Its folder entries stay in the folder docs and in the merged views until ADR 0003's own index exchange leaves iroh-docs.
- `status` and `/api/status` list the removed nodes; the web UI's Group view removes members and says which members go with them.
