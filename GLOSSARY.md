# sync-node

A local-first tool that keeps folders in sync between a small group of trusted machines over direct peer-to-peer connections.

## Membership

**Group**:
The set of nodes that trust each other, rooted at the node that founded it.
_Avoid_: Network, cluster, swarm

**Node**:
One running instance, identified by its public key.
_Avoid_: Peer (except for "the other end of a connection"), device, client

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

**Block**:
A fixed-size piece of a file's content, identified by its hash.
_Avoid_: Chunk, piece

**Block list**:
The hashes of a file's blocks, in order, carried in its entry.
_Avoid_: Manifest, outboard

**Provider**:
A node whose entry for a path has the content another node wants.
_Avoid_: Source, seeder
