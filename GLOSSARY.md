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
A node holding a valid signed record in its group.
_Avoid_: User, participant

## Folders

**Folder**:
A directory kept in sync across the nodes that keep it, known in the group by its name.
_Avoid_: Share, repo, sync root

**Entry**:
One node's description of one path in a folder: its content, size, modification time and version.
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

**Rarest first**:
Pieces held by the fewest peers are asked for first.

**Endgame**:
Once every missing block is asked for, an overdue block is asked of another peer too; the first answer wins and the other is cancelled.

**Staging file**:
The file a fetch writes pieces into, at the folder root, until it is complete and placed. A leecher serves its verified pieces from it.
_Avoid_: Temporary file, partial file
