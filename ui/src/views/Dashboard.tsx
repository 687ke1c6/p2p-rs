import { Link } from "@tanstack/solid-router";
import ArrowDown from "lucide-solid/icons/arrow-down";
import ArrowUp from "lucide-solid/icons/arrow-up";
import Cable from "lucide-solid/icons/cable";
import FolderInput from "lucide-solid/icons/folder-input";
import FolderPlus from "lucide-solid/icons/folder-plus";
import Pencil from "lucide-solid/icons/pencil";
import Trash from "lucide-solid/icons/trash";
import Users from "lucide-solid/icons/users";
import { createEffect, createMemo, createSignal, For, type JSX, Show } from "solid-js";
import type { FolderInfo } from "@/api/FolderInfo";
import type { GroupStatus } from "@/api/GroupStatus";
import type { LinkState } from "@/api/LinkState";
import type { MemberInfo } from "@/api/MemberInfo";
import type { PeerInfo } from "@/api/PeerInfo";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge, type BadgeProps } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPortal,
  DialogTitle,
} from "@/components/ui/dialog";
import { Switch, SwitchControl, SwitchInput, SwitchLabel, SwitchThumb } from "@/components/ui/switch";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { TextField, TextFieldInput, TextFieldLabel } from "@/components/ui/text-field";
import { action } from "@/lib/action";
import {
  addFolder,
  addMember,
  nameGroup,
  removeFolder,
  removeMember,
  setFolderEnabled,
} from "@/lib/api";
import { bytes } from "@/lib/format";
import { live } from "@/lib/socket";
import { Failed, Form, Queued } from "./Forms";
import { LiveError } from "./LiveError";

/** The landing view: the state at a glance, then the group (`sync-node status`). */
export function Dashboard() {
  const status = live("status", "Status");
  return (
    <div class="space-y-4">
      <LiveError error={status().error} />
      <Show when={status().value}>
        {(answer) => (
          <Show
            when={answer().group}
            fallback={
              <Alert>
                <AlertTitle>In no group</AlertTitle>
                <AlertDescription>
                  <p>
                    This node ({answer().node}) is in no group yet.{" "}
                    <Link to="/setup" class={buttonVariants({ variant: "link", class: "h-auto p-0" })}>
                      Create a group
                    </Link>{" "}
                    or join one.
                  </p>
                </AlertDescription>
              </Alert>
            }
          >
            {(group) => (
              <>
                <Stats group={group()} />
                <GroupDetails node={answer().node} group={group()} />
              </>
            )}
          </Show>
        )}
      </Show>
    </div>
  );
}

/** Totals of this node's traffic to a peer, both paths. */
const sent = (peer: PeerInfo) => peer.traffic.direct_sent + peer.traffic.relay_sent;
const received = (peer: PeerInfo) => peer.traffic.direct_recv + peer.traffic.relay_recv;

function Stats(props: { group: GroupStatus }) {
  const others = () => props.group.members.filter((m) => !m.this_node);
  const connected = () => others().filter((m) => (m.peer?.connections ?? 0) > 0);
  const kind = (k: string) => connected().filter((m) => m.peer?.kind === k).length;
  const peers = () => props.group.members.flatMap((m) => (m.peer ? [m.peer] : []));
  const total = (of: (peer: PeerInfo) => number) => peers().reduce((sum, p) => sum + of(p), 0);
  return (
    <div class="grid grid-cols-2 gap-4 lg:grid-cols-4">
      <Stat
        icon={<Users class="size-4" />}
        label="Members online"
        value={`${props.group.members.filter((m) => m.online).length} / ${props.group.members.length}`}
      />
      <Stat
        icon={<Cable class="size-4" />}
        label="Connected"
        value={String(connected().length)}
        detail={`${kind("Direct")} direct, ${kind("Relay")} relay`}
      />
      <Stat
        icon={<ArrowUp class="size-4" />}
        label="Sent"
        value={bytes(total(sent))}
        detail="since this node started"
      />
      <Stat
        icon={<ArrowDown class="size-4" />}
        label="Received"
        value={bytes(total(received))}
        detail="since this node started"
      />
    </div>
  );
}

function Stat(props: { icon: JSX.Element; label: string; value: string; detail?: string }) {
  return (
    <Card class="gap-2 py-4">
      <CardHeader class="px-4">
        <CardDescription class="flex items-center gap-2">
          {props.icon}
          {props.label}
        </CardDescription>
      </CardHeader>
      <CardContent class="px-4">
        <div class="text-2xl font-semibold tabular-nums">{props.value}</div>
        <Show when={props.detail}>
          <div class="text-muted-foreground text-xs">{props.detail}</div>
        </Show>
      </CardContent>
    </Card>
  );
}

/** A part of the group's card, under its own heading. */
function Section(props: { title: string; description?: string; action?: JSX.Element; children: JSX.Element }) {
  return (
    <section class="space-y-3 border-t pt-4">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h3 class="font-medium">{props.title}</h3>
          <Show when={props.description}>
            <p class="text-muted-foreground text-sm">{props.description}</p>
          </Show>
        </div>
        {props.action}
      </div>
      {props.children}
    </section>
  );
}

function GroupDetails(props: { node: string; group: GroupStatus }) {
  const label = (id: string) => props.group.members.find((m) => m.id === id)?.label ?? id.slice(0, 10);
  const member = (id: string) => props.group.members.find((m) => m.id === id);
  // The dialogs are outside the rows, by member ID, so an update of the
  // status doesn't close them.
  const [renaming, setRenaming] = createSignal<string>();
  const [removing, setRemoving] = createSignal<string>();
  const [naming, setNaming] = createSignal(false);
  // The member a dialog shows stays while it closes.
  const lastRenamed = createMemo<string | undefined>((last) => renaming() ?? last);
  const lastRemoved = createMemo<string | undefined>((last) => removing() ?? last);
  return (
    <Card>
      <CardHeader>
        <div class="flex items-start justify-between gap-2">
          <div class="space-y-1.5">
            <CardTitle>{props.group.name ?? "Group"}</CardTitle>
            <CardDescription>Founded by {label(props.group.founder)}</CardDescription>
          </div>
          <Button size="sm" variant="outline" onClick={() => setNaming(true)}>
            <Pencil class="size-4" />
            {props.group.name === null ? "Name" : "Rename"}
          </Button>
        </div>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
          <span class="text-muted-foreground">node</span>
          <span class="font-mono break-all">{props.node}</span>
          <span class="text-muted-foreground">group</span>
          <span class="font-mono break-all">{props.group.id}</span>
        </div>

        <Section
          title="Members"
          description={`${props.group.members.length} members, ${
            props.group.members.filter((m) => m.online).length
          } online. Traffic is this node's, since it started.`}
        >
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Member</TableHead>
                <TableHead>State</TableHead>
                <TableHead>Connection</TableHead>
                <TableHead class="text-right">Sent / received</TableHead>
                <TableHead>Added by</TableHead>
                <TableHead />
              </TableRow>
            </TableHeader>
            <TableBody>
              <For each={props.group.members}>
                {(m) => (
                  <TableRow>
                    <TableCell title={m.id}>{m.label}</TableCell>
                    <TableCell>
                      <MemberState member={m} />
                    </TableCell>
                    <TableCell>
                      <Show when={m.peer} fallback="–">
                        {(peer) => <Connection peer={peer()} />}
                      </Show>
                    </TableCell>
                    <TableCell class="text-right tabular-nums">
                      <Show when={m.peer} fallback="–">
                        {(peer) => (
                          <span
                            title={`direct ↑${bytes(peer().traffic.direct_sent)} ↓${bytes(
                              peer().traffic.direct_recv,
                            )}, relay ↑${bytes(peer().traffic.relay_sent)} ↓${bytes(
                              peer().traffic.relay_recv,
                            )}`}
                          >
                            ↑{bytes(sent(peer()))} ↓{bytes(received(peer()))}
                          </span>
                        )}
                      </Show>
                    </TableCell>
                    <TableCell>
                      {m.id === props.group.founder ? "founder" : label(m.added_by)}
                    </TableCell>
                    <TableCell>
                      <div class="flex justify-end gap-1">
                        <Button
                          size="icon-sm"
                          variant="ghost"
                          title="Rename"
                          aria-label={`Rename ${m.label}`}
                          onClick={() => setRenaming(m.id)}
                        >
                          <Pencil class="size-4" />
                        </Button>
                        <Show when={!m.this_node && m.id !== props.group.founder}>
                          <Button
                            size="icon-sm"
                            variant="ghost"
                            title="Remove from the group"
                            aria-label={`Remove ${m.label}`}
                            onClick={() => setRemoving(m.id)}
                          >
                            <Trash class="size-4" />
                          </Button>
                        </Show>
                      </div>
                    </TableCell>
                  </TableRow>
                )}
              </For>
            </TableBody>
          </Table>
        </Section>

        <Show when={props.group.removed.length > 0}>
          <Section
            title="Removed"
            description="No member connects to these nodes or syncs with them. They can't come back with the same ID."
          >
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Node</TableHead>
                  <TableHead>Removed by</TableHead>
                  <TableHead>When</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                <For each={props.group.removed}>
                  {(r) => (
                    <TableRow>
                      <TableCell class="font-mono" title={r.id}>
                        {r.id.slice(0, 10)}
                      </TableCell>
                      <TableCell>{label(r.removed_by)}</TableCell>
                      <TableCell>{new Date(r.removed_at * 1000).toLocaleString()}</TableCell>
                    </TableRow>
                  )}
                </For>
              </TableBody>
            </Table>
          </Section>
        </Show>

        <Show when={props.group.links.length > 0}>
          <Section title="Links" description="Each pair's connection, as the group reports it.">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Pair</TableHead>
                  <TableHead>Connection</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                <For each={props.group.links}>
                  {(link) => (
                    <TableRow>
                      <TableCell>
                        {label(link.a)} ↔ {label(link.b)}
                      </TableCell>
                      <TableCell>
                        <Badge variant={linkVariant[link.state]}>{linkText[link.state]}</Badge>
                      </TableCell>
                    </TableRow>
                  )}
                </For>
              </TableBody>
            </Table>
          </Section>
        </Show>

        <FoldersSection group={props.group} />
      </CardContent>

      <NameGroupDialog
        open={naming()}
        name={props.group.name}
        onClose={() => setNaming(false)}
      />
      <RenameDialog
        open={renaming() !== undefined}
        member={member(lastRenamed() ?? "")}
        onClose={() => setRenaming(undefined)}
      />
      <RemoveDialog
        open={removing() !== undefined}
        id={lastRemoved()}
        group={props.group}
        label={label}
        onClose={() => setRemoving(undefined)}
      />
    </Card>
  );
}

/** A row being edited in the folders table: a new folder, or keeping an available one. */
type Editing = { kind: "new" } | { kind: "map"; name: string };

/** The folders this node keeps, those it could keep, and a row to add one. */
function FoldersSection(props: { group: GroupStatus }) {
  const [editing, setEditing] = createSignal<Editing>();
  // Outside the rows, by name, as the member dialogs.
  const [removing, setRemoving] = createSignal<string>();
  const lastRemoved = createMemo<string | undefined>((last) => removing() ?? last);
  const [name, setName] = createSignal("");
  const [path, setPath] = createSignal("");
  const [added, startAdd] = action(addFolder);
  const edit = (next: Editing) => {
    setName(next.kind === "map" ? next.name : "");
    setPath("");
    setEditing(next);
  };
  const mapping = (folder: string) => {
    const now = editing();
    return now?.kind === "map" && now.name === folder;
  };
  const save = async () => {
    await startAdd({ name: name(), path: path() });
    if (added().answer) setEditing(undefined);
  };
  const empty = () =>
    props.group.folders.length === 0 && props.group.available.length === 0 && !editing();
  return (
    <Section
      title="Folders"
      description="Kept on this node, and kept by other nodes of the group: keep one here by giving it a directory."
      action={
        <Button size="sm" variant="outline" disabled={editing()?.kind === "new"} onClick={() => edit({ kind: "new" })}>
          <FolderPlus class="size-4" />
          New folder
        </Button>
      }
    >
      <Show
        when={!empty()}
        fallback={<p class="text-muted-foreground text-sm">The group has no folders yet.</p>}
      >
        <Form onSubmit={() => void save()}>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Folder</TableHead>
                <TableHead>Directory on this node</TableHead>
                <TableHead class="text-right">Files</TableHead>
                <TableHead class="text-right">Size</TableHead>
                <TableHead>State</TableHead>
                <TableHead class="text-right">On this node</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <For each={props.group.folders}>
                {(folder) => (
                  <TableRow>
                    <TableCell>
                      <Link
                        to="/folders/$name"
                        params={{ name: folder.name }}
                        class={buttonVariants({ variant: "link", class: "h-auto p-0" })}
                      >
                        {folder.name}
                      </Link>
                    </TableCell>
                    <TableCell class="font-mono">{folder.path}</TableCell>
                    <TableCell class="text-right tabular-nums">{folder.files}</TableCell>
                    <TableCell class="text-right tabular-nums">{bytes(folder.bytes)}</TableCell>
                    <TableCell>
                      <FolderState folder={folder} />
                    </TableCell>
                    <TableCell>
                      <div class="flex items-center justify-end gap-1">
                        <FolderSwitch folder={folder} />
                        <Button
                          size="icon-sm"
                          variant="ghost"
                          type="button"
                          title="Remove from this node"
                          aria-label={`Remove ${folder.name} from this node`}
                          onClick={() => setRemoving(folder.name)}
                        >
                          <Trash class="size-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                )}
              </For>
              <For each={props.group.available}>
                {(folder) => (
                  <TableRow class="text-muted-foreground">
                    <TableCell>{folder.name}</TableCell>
                    <Show
                      when={mapping(folder.name)}
                      fallback={
                        <>
                          <TableCell>not kept here</TableCell>
                          <TableCell class="text-right">–</TableCell>
                          <TableCell class="text-right">–</TableCell>
                          <TableCell>
                            <Badge variant="outline">available, kept by {folder.nodes} nodes</Badge>
                          </TableCell>
                          <TableCell>
                            <div class="flex justify-end">
                              <Button
                                size="sm"
                                variant="ghost"
                                type="button"
                                onClick={() => edit({ kind: "map", name: folder.name })}
                              >
                                <FolderInput class="size-4" />
                                Keep here
                              </Button>
                            </div>
                          </TableCell>
                        </>
                      }
                    >
                      <TableCell colSpan={4}>
                        <PathInput value={path()} onChange={setPath} />
                      </TableCell>
                      <TableCell>
                        <EditButtons busy={added().busy} onCancel={() => setEditing(undefined)} />
                      </TableCell>
                    </Show>
                  </TableRow>
                )}
              </For>
              <Show when={editing()?.kind === "new"}>
                <TableRow>
                  <TableCell>
                    <TextField value={name()} onChange={setName}>
                      <TextFieldLabel class="sr-only">Folder name</TextFieldLabel>
                      <TextFieldInput class="h-8" placeholder="name, the same on every node" required />
                    </TextField>
                  </TableCell>
                  <TableCell colSpan={4}>
                    <PathInput value={path()} onChange={setPath} />
                  </TableCell>
                  <TableCell>
                    <EditButtons busy={added().busy} onCancel={() => setEditing(undefined)} />
                  </TableCell>
                </TableRow>
              </Show>
            </TableBody>
          </Table>
        </Form>
      </Show>
      <Failed state={added()} />
      <Show when={added().answer}>
        {(answer) => <Queued within={answer().within_secs}>The folder then shows above.</Queued>}
      </Show>
      <RemoveFolderDialog
        open={removing() !== undefined}
        folder={props.group.folders.find((f) => f.name === lastRemoved())}
        onClose={() => setRemoving(undefined)}
      />
    </Section>
  );
}

function FolderState(props: { folder: FolderInfo }) {
  return (
    <Show
      when={!props.folder.disabled}
      fallback={<Badge variant="outline">disabled: not watched, fetched or served</Badge>}
    >
      <Show
        when={!props.folder.paused}
        fallback={<Badge variant="destructive">paused: root or its marker missing</Badge>}
      >
        <Badge variant="secondary">synced with {props.folder.peers} nodes</Badge>
      </Show>
    </Show>
  );
}

/**
 * Enables or disables a folder on this node, as `folder enable|disable`. Shows
 * the asked-for state until the node has applied it (within one interval).
 */
function FolderSwitch(props: { folder: FolderInfo }) {
  const [asked, setAsked] = createSignal<boolean>();
  // Called from the change handler only.
  // eslint-disable-next-line solid/reactivity
  const [state, start] = action((enabled: boolean) =>
    setFolderEnabled(props.folder.name, { enabled }),
  );
  const enabled = () => !props.folder.disabled;
  // Applied (or changed elsewhere since): follow the node again.
  createEffect(() => {
    if (asked() === enabled()) setAsked(undefined);
  });
  const toggle = async (on: boolean) => {
    setAsked(on);
    await start(on);
    if (state().error) setAsked(undefined);
  };
  return (
    <Switch
      class="flex items-center gap-x-2"
      checked={asked() ?? enabled()}
      disabled={state().busy}
      onChange={(on) => void toggle(on)}
      title={state().error}
    >
      <SwitchInput />
      <SwitchLabel class="sr-only">Enabled on this node</SwitchLabel>
      <SwitchControl>
        <SwitchThumb />
      </SwitchControl>
    </Switch>
  );
}

/** Stops keeping a folder on this node, as `folder remove`. Its files stay. */
function RemoveFolderDialog(props: { open: boolean; folder?: FolderInfo; onClose: () => void }) {
  return (
    <Dialog open={props.open} onOpenChange={(open) => !open && props.onClose()}>
      <DialogPortal>
        <DialogContent>
          <Show when={props.folder}>
            {(folder) => <RemoveFolderForm folder={folder()} onClose={() => props.onClose()} />}
          </Show>
        </DialogContent>
      </DialogPortal>
    </Dialog>
  );
}

function RemoveFolderForm(props: { folder: FolderInfo; onClose: () => void }) {
  const [removed, start] = action(removeFolder);
  const submit = async () => {
    await start(props.folder.name);
    if (!removed().error) props.onClose();
  };
  return (
    <Form onSubmit={() => void submit()}>
      <DialogHeader>
        <DialogTitle>Remove {props.folder.name} from this node?</DialogTitle>
        <DialogDescription>
          This node stops keeping it, as before it was added. The files in{" "}
          <span class="font-mono break-all">{props.folder.path}</span> stay; only this node's own
          files there (the marker, staging files) go. The group keeps the folder, and this node can
          keep it again later.
        </DialogDescription>
      </DialogHeader>
      <Failed state={removed()} />
      <DialogFooter>
        <Button variant="outline" type="button" onClick={() => props.onClose()}>
          Cancel
        </Button>
        <Button variant="destructive" type="submit" disabled={removed().busy}>
          <Trash class="size-4" />
          Remove
        </Button>
      </DialogFooter>
    </Form>
  );
}

function PathInput(props: { value: string; onChange: (value: string) => void }) {
  return (
    <TextField value={props.value} onChange={(value) => props.onChange(value)}>
      <TextFieldLabel class="sr-only">Directory on this node</TextFieldLabel>
      <TextFieldInput class="h-8 font-mono" placeholder="/home/…" required autofocus />
    </TextField>
  );
}

function EditButtons(props: { busy: boolean; onCancel: () => void }) {
  return (
    <div class="flex justify-end gap-1">
      <Button size="sm" type="submit" disabled={props.busy}>
        Save
      </Button>
      <Button size="sm" variant="ghost" type="button" onClick={() => props.onCancel()}>
        Cancel
      </Button>
    </div>
  );
}

/** Names or renames the group, as `group name`. */
function NameGroupDialog(props: { open: boolean; name: string | null; onClose: () => void }) {
  return (
    <Dialog open={props.open} onOpenChange={(open) => !open && props.onClose()}>
      <DialogPortal>
        <DialogContent>
          <NameForm
            title={props.name === null ? "Name the group" : "Rename the group"}
            description="Shown with the group ID, on every node."
            initial={props.name ?? ""}
            save={(name) => nameGroup({ name })}
            onClose={() => props.onClose()}
          />
        </DialogContent>
      </DialogPortal>
    </Dialog>
  );
}

/** Renames a member, as `member add <id> --name`. */
function RenameDialog(props: { open: boolean; member?: MemberInfo; onClose: () => void }) {
  return (
    <Dialog open={props.open} onOpenChange={(open) => !open && props.onClose()}>
      <DialogPortal>
        <DialogContent>
          <NameForm
            title={`Rename ${props.member?.label ?? "member"}`}
            description="Shown instead of the node ID, on every node. Names are unique in the group."
            initial={props.member?.name ?? ""}
            save={(name) => addMember({ id: props.member?.id ?? "", name })}
            onClose={() => props.onClose()}
          />
        </DialogContent>
      </DialogPortal>
    </Dialog>
  );
}

/** A dialog's name input with Save and Cancel. Mounted with the dialog, so each opening starts afresh. */
function NameForm(props: {
  title: string;
  description: string;
  initial: string;
  save: (name: string) => Promise<unknown>;
  onClose: () => void;
}) {
  // Read once: the form starts from the name the dialog opened with.
  const [name, setName] = createSignal(props.initial);
  // Called from the submit handler only.
  // eslint-disable-next-line solid/reactivity
  const [saved, start] = action((name: string) => props.save(name));
  const submit = async () => {
    await start(name());
    if (!saved().error) props.onClose();
  };
  return (
    <Form onSubmit={() => void submit()}>
      <DialogHeader>
        <DialogTitle>{props.title}</DialogTitle>
        <DialogDescription>{props.description}</DialogDescription>
      </DialogHeader>
      <TextField value={name()} onChange={setName}>
        <TextFieldLabel>Name</TextFieldLabel>
        <TextFieldInput required autofocus />
      </TextField>
      <Failed state={saved()} />
      <DialogFooter>
        <Button variant="outline" type="button" onClick={() => props.onClose()}>
          Cancel
        </Button>
        <Button type="submit" disabled={saved().busy}>
          Save
        </Button>
      </DialogFooter>
    </Form>
  );
}

/**
 * The members whose chain goes through `id`: they stop being members with it,
 * unless another member adds them again.
 */
function addedThrough(group: GroupStatus, id: string): MemberInfo[] {
  const byId = new Map(group.members.map((m) => [m.id, m]));
  return group.members.filter((member) => {
    const seen = new Set<string>();
    let current: MemberInfo | undefined = member;
    while (current && current.id !== group.founder && !seen.has(current.id)) {
      seen.add(current.id);
      if (current.added_by === id) return true;
      current = byId.get(current.added_by);
    }
    return false;
  });
}

/** Removes a member, as `member remove`. */
function RemoveDialog(props: {
  open: boolean;
  id?: string;
  group: GroupStatus;
  label: (id: string) => string;
  onClose: () => void;
}) {
  return (
    <Dialog open={props.open} onOpenChange={(open) => !open && props.onClose()}>
      <DialogPortal>
        <DialogContent>
          <Show when={props.id}>
            {(id) => (
              <RemoveForm
                id={id()}
                label={props.label(id())}
                cascade={addedThrough(props.group, id()).map((m) => m.label)}
                onClose={() => props.onClose()}
              />
            )}
          </Show>
        </DialogContent>
      </DialogPortal>
    </Dialog>
  );
}

function RemoveForm(props: { id: string; label: string; cascade: string[]; onClose: () => void }) {
  const [removed, start] = action(removeMember);
  const submit = async () => {
    await start({ id: props.id });
    if (!removed().error) props.onClose();
  };
  return (
    <Form onSubmit={() => void submit()}>
      <DialogHeader>
        <DialogTitle>Remove {props.label}?</DialogTitle>
        <DialogDescription>
          No member connects to it or syncs with it again, and it can't come back with this ID. It
          keeps the files it already has.
        </DialogDescription>
      </DialogHeader>
      <Show when={props.cascade.length > 0}>
        <Alert>
          <AlertTitle>These go too</AlertTitle>
          <AlertDescription>
            <p>
              {props.cascade.join(", ")}: {props.label} added them. Another member can add them
              again.
            </p>
          </AlertDescription>
        </Alert>
      </Show>
      <Failed state={removed()} />
      <DialogFooter>
        <Button variant="outline" type="button" onClick={() => props.onClose()}>
          Cancel
        </Button>
        <Button variant="destructive" type="submit" disabled={removed().busy}>
          <Trash class="size-4" />
          Remove
        </Button>
      </DialogFooter>
    </Form>
  );
}

function MemberState(props: { member: MemberInfo }) {
  return (
    <Show when={!props.member.this_node} fallback={<Badge>this node</Badge>}>
      <Show
        when={props.member.online}
        fallback={
          <Badge variant="outline">
            offline (
            {props.member.last_heard_secs === null
              ? "never heard"
              : `last heard ${props.member.last_heard_secs}s ago`}
            )
          </Badge>
        }
      >
        <Badge variant="secondary">online</Badge>
      </Show>
    </Show>
  );
}

/** This node's connection to a peer: its kind, and the path as `status` shows it. */
function Connection(props: { peer: PeerInfo }) {
  const rtt = () => (props.peer.rtt_ms === null ? "" : `, rtt ${props.peer.rtt_ms} ms`);
  return (
    <Show when={props.peer.connections > 0} fallback={<Badge variant="outline">not connected</Badge>}>
      <span class="flex items-center gap-2" title={`${props.peer.path ?? "no path selected"}${rtt()}`}>
        <Show when={props.peer.kind} fallback={<Badge variant="outline">no path yet</Badge>}>
          {(kind) => (
            <Badge variant={kind() === "Direct" ? "secondary" : "outline"}>
              {kind() === "Direct" ? "direct" : "relay"}
            </Badge>
          )}
        </Show>
        <span class="text-muted-foreground text-xs">
          {props.peer.connections} conn{rtt()}
        </span>
      </span>
    </Show>
  );
}

const linkText: Record<LinkState, string> = {
  Direct: "direct",
  Relay: "relay",
  NotConnected: "not connected",
  Unknown: "unknown (offline)",
};

const linkVariant: Record<LinkState, BadgeProps["variant"]> = {
  Direct: "secondary",
  Relay: "outline",
  NotConnected: "destructive",
  Unknown: "outline",
};
