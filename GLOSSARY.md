# sync-node

A local-first tool that keeps folders in sync between a small group of trusted machines over direct peer-to-peer connections.

## Membership

**Group**:
The set of nodes that trust each other, rooted at the node that founded it.
_Avoid_: Network, cluster, swarm

**Node**:
One running instance, identified by its public key.
_Avoid_: Device, client. A node is a **peer** only in a swarm (see Content).

**Member**:
A node holding a valid signed record in its group, whose chain of records reaches the founder through no removed node.
_Avoid_: User, participant

**Removal**:
A member's signed statement that a node is no longer in the group. It always wins: the removed ID can't come back. Members whose chain goes through the removed node are no longer members either, unless another member adds them again (ADR 0006).
_Avoid_: Revocation, ban, kick

**Group name**:
The group's display name, set by any member, the same on every node. Cosmetic: the group ID identifies the group.

## Folders

**Folder**:
A directory kept in sync across the nodes that keep it, known in the group by its name.
_Avoid_: Share, repo, sync root

**Document/doc**
The iroh-doc document that is replicated across the group

**Disabled folder**:
A folder turned off on one node (`folder disable`): not watched or indexed, fetches nothing and serves nothing, while its doc still syncs and its entries stay. Enabled again, it catches up as a node coming back online does: its first scan writes what changed meanwhile, deletes included.
_Avoid_: Paused (that's a folder whose root or marker is missing), muted, unmapped

**Entry**:
One members description of one path in a folder: its content, size, modification time and version, stored in the 'folder' iroh-doc document
_Avoid_: Record (that's membership), file info

**Tombstone**:
An entry saying the path was deleted.
_Avoid_: Deletion marker

**Version**:
Which edits to a path an entry has seen, one counter per node; one version came after another if it has seen everything the other has.
_Avoid_: Revision, generation

**Merged view**:
What a folder should look like once every node has synced, computed from all nodes' entries.
_Avoid_: Global state, target state

**Conflict**:
Two or more versions of a path where none came after the others.

**Conflict copy**:
A losing version of a conflict, kept beside the winner under a name that says when and where it was made.
_Avoid_: Backup, duplicate

## Content

Content moves as in BitTorrent, and uses its terms. One version of one file (its content hash) is the torrent; the content hash has the role of the info hash.

**Piece**:
A fixed-size part of a file's content, identified by its hash. The unit that is verified, written and announced.
_Avoid_: Block (that's a part of a piece), chunk

**Piece hashes**:
The hashes of a file's pieces, in order, carried in its entry.
_Avoid_: Block list, manifest, outboard

**Piece length**:
The size of every piece of a file but the last. It depends on the file size only.
_Avoid_: Block size

**Block**:
A part of a piece (16 KiB, the last one may be short) that one request asks for. Not verified by itself: its piece is.
_Avoid_: Slice, chunk

**Swarm**:
The nodes that hold or fetch one version of one file in a folder.

**Peer**:
Another node in the same swarm, at the other end of a peer-wire connection.
_Avoid_: Provider, source

**Seeder**:
A peer that has every piece: its entry has the version.

**Leecher**:
A peer that is still fetching. It serves the pieces it has verified.

**Bitfield**:
Which pieces a peer has, sent once when a swarm starts on a connection. **Have** then announces each piece as it is verified.

**Interested**:
A peer wants pieces the other end has.

**Choke**:
A peer doesn't serve the other end's requests now. **Unchoke** ends it. Upload slots are managed with it; there is no tit-for-tat.

**Optimistic unchoke**:
An unchoke that rotates to another interested peer, so its speed can be measured.

**Snubbed**:
A peer that has requests open but sent no block for 60 s. It is asked for one block at a time until it sends.

**Stuck**:
A download with no peer in its swarm that both unchokes this node and has a piece it lacks. With no peer in the swarm for 30 s, the download stops, for a later try.
_Avoid_: Stalled, hung

**Rarest first**:
Pieces held by the fewest peers are asked for first.

**Endgame**:
Once every missing block is asked for, an overdue block is asked of another peer too; the first answer wins and the other is cancelled.

**Staging file**:
The file a fetch writes pieces into, at the folder root, until it is complete and placed. A leecher serves its verified pieces from it.
_Avoid_: Temporary file, partial file

## Interface

**Web UI**:
The page that the running node serves to a browser: what the node sees, and the setup actions (create, join, invites, members, folders). Part of `sync-node run` (ADR 0004).
_Avoid_: Dashboard, front end, metrics page, console

**UI address**:
The address the web UI's server listens on, set by `--ui-addr` / `SYNC_NODE_UI_ADDR`. No UI address, no server.
_Avoid_: Port, bind (alone)

**UI topic**:
One read the web UI subscribes to over its WebSocket, named as its read route (`status`, `folders`, `folders/<name>`, `transfers`); the node sends its data whenever it changes (ADR 0005).
_Avoid_: Topic (alone: that's the gossip presence topic), channel, feed, subscription

**UI dir**:
The directory of the built page that the server sends, set by `--ui-dir` / `SYNC_NODE_UI_DIR`; `ui/` next to the executable by default. Built from the `ui/` source directory into `ui/dist/`.
_Avoid_: Static dir, assets

## Processes

**Download ceremony**
A download is not just sending physical bytes. In this distributed system (like bittorrent), a download is the process by which an interested member discovers peers of the swarm, requesting their bitfields, requesting blocks from peers, building local blocks/pieces, and becoming a leecher. 