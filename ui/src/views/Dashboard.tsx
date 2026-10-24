import { Link, Navigate } from "@tanstack/solid-router";
import ArrowDown from "lucide-solid/icons/arrow-down";
import ArrowUp from "lucide-solid/icons/arrow-up";
import Cable from "lucide-solid/icons/cable";
import FolderInput from "lucide-solid/icons/folder-input";
import FolderPlus from "lucide-solid/icons/folder-plus";
import Pencil from "lucide-solid/icons/pencil";
import CircleCheck from "lucide-solid/icons/circle-check";
import Clock from "lucide-solid/icons/clock";
import PartyPopper from "lucide-solid/icons/party-popper";
import Ticket from "lucide-solid/icons/ticket";
import Trash from "lucide-solid/icons/trash";
import UserPlus from "lucide-solid/icons/user-plus";
import Users from "lucide-solid/icons/users";
import { createEffect, createMemo, createSignal, For, type JSX, on, Show } from "solid-js";
import type { AvailableInfo } from "@/api/AvailableInfo";
import type { FolderInfo } from "@/api/FolderInfo";
import type { RemovedInfo } from "@/api/RemovedInfo";
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
  HoverCard,
  HoverCardContent,
  HoverCardPortal,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import { TableCell, TableRow } from "@/components/ui/table";
import {
  TextField,
  TextFieldErrorMessage,
  TextFieldInput,
  TextFieldLabel,
} from "@/components/ui/text-field";
import { action } from "@/lib/action";
import { cx } from "@/lib/cva";
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
import { setWelcome, welcome } from "@/lib/welcome";
import { columnsOf, DataTable, dense } from "./DataTable";
import { Banner } from "./Banner";
import { Failed, Form } from "./Forms";
import { AddMemberDialog, InviteDialog } from "./Invite";
import { LiveError } from "./LiveError";

/** The landing view: the group, the state at a glance, its members and its folders (`sync-node status`). */
export function Dashboard() {
  const status = live("status", "Status");
  return (
    <div class="space-y-3">
      <LiveError error={status().error} />
      <Show when={status().value}>
        {(answer) => (
          <Show when={answer().group} fallback={<Navigate to="/start" />}          >
            {(group) => <GroupDetails node={answer().node} group={group()} />}
          </Show>
        )}
      </Show>
    </div>
  );
}

/** Totals of this node's traffic to a peer, both paths. */
const sent = (peer: PeerInfo) => peer.traffic.direct_sent + peer.traffic.relay_sent;
const received = (peer: PeerInfo) => peer.traffic.direct_recv + peer.traffic.relay_recv;

function Stats(props: { group: GroupStatus; label: (id: string) => string }) {
  const others = () => props.group.members.filter((m) => !m.this_node);
  const connected = () => others().filter((m) => (m.peer?.connections ?? 0) > 0);
  const kind = (k: string) => connected().filter((m) => m.peer?.kind === k).length;
  const peers = () => props.group.members.flatMap((m) => (m.peer ? [m.peer] : []));
  const total = (of: (peer: PeerInfo) => number) => peers().reduce((sum, p) => sum + of(p), 0);
  return (
    <div class="grid grid-cols-2 gap-3 lg:grid-cols-4">
      <Stat
        icon={<Users class="size-4" />}
        label="Members online"
        value={`${props.group.members.filter((m) => m.online).length} / ${props.group.members.length}`}
      />
      <HoverCard openDelay={150} closeDelay={100}>
        <HoverCardTrigger as="div" class="cursor-default">
          <Stat
            icon={<Cable class="size-4" />}
            label="Connected"
            value={String(connected().length)}
            detail={`${kind("Direct")} direct, ${kind("Relay")} relay · hover for links`}
          />
        </HoverCardTrigger>
        <HoverCardPortal>
          <HoverCardContent class="w-80 p-3">
            <Links group={props.group} label={props.label} />
          </HoverCardContent>
        </HoverCardPortal>
      </HoverCard>
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
    <Card class="h-full gap-1 py-3">
      <CardHeader class="px-3">
        <CardDescription class="flex items-center gap-2">
          {props.icon}
          {props.label}
        </CardDescription>
      </CardHeader>
      <CardContent class="px-3">
        <div class="text-xl font-semibold tabular-nums">{props.value}</div>
        <Show when={props.detail}>
          <div class="text-muted-foreground text-xs">{props.detail}</div>
        </Show>
      </CardContent>
    </Card>
  );
}

/** Each pair's link, as the group reports it: the hover of the Connected tile. */
function Links(props: { group: GroupStatus; label: (id: string) => string }) {
  return (
    <div class="space-y-2">
      <p class="text-sm font-medium">Links</p>
      <Show
        when={props.group.links.length > 0}
        fallback={<p class="text-muted-foreground text-sm">No other member yet.</p>}
      >
        <ul class="space-y-1 text-sm">
          <For each={props.group.links}>
            {(link) => (
              <li class="flex items-center justify-between gap-2">
                <span class="truncate">
                  {props.label(link.a)} ↔ {props.label(link.b)}
                </span>
                <Badge variant={linkVariant[link.state]}>{linkText[link.state]}</Badge>
              </li>
            )}
          </For>
        </ul>
      </Show>
    </div>
  );
}

/** A card of the dashboard: a title, a line on it, an action, and its content. */
function SectionCard(props: {
  title: string;
  description?: string;
  action?: JSX.Element;
  children: JSX.Element;
}) {
  return (
    <Card class="gap-3 py-3">
      <CardHeader class="px-4">
        <div class="flex flex-wrap items-center justify-between gap-2">
          <div class="space-y-1">
            <CardTitle>{props.title}</CardTitle>
            <Show when={props.description}>
              <CardDescription>{props.description}</CardDescription>
            </Show>
          </div>
          {props.action}
        </div>
      </CardHeader>
      <CardContent class="px-4">{props.children}</CardContent>
    </Card>
  );
}

/** The group's name and founder, above its cards; its IDs on hover. */
function GroupHeader(props: {
  node: string;
  group: GroupStatus;
  label: (id: string) => string;
  onRename: () => void;
}) {
  return (
    <div class="space-y-0.5">
      <div class="flex items-center gap-1">
        <h1 class="text-lg font-semibold tracking-tight">{props.group.name ?? "Unnamed group"}</h1>
        <Button
          size="icon-sm"
          variant="ghost"
          title={props.group.name === null ? "Name the group" : "Rename the group"}
          aria-label={props.group.name === null ? "Name the group" : "Rename the group"}
          onClick={() => props.onRename()}
        >
          <Pencil class="size-4" />
        </Button>
      </div>
      <p class="text-muted-foreground flex flex-wrap gap-x-4 gap-y-0.5 text-xs">
        <span>Founded by {props.label(props.group.founder)}</span>
        <HoverCard openDelay={150} closeDelay={100}>
          <HoverCardTrigger as="span" class="cursor-default underline decoration-dotted underline-offset-2">
            IDs
          </HoverCardTrigger>
          <HoverCardPortal>
            <HoverCardContent class="w-96 space-y-2 p-3 text-sm">
              <div>
                <p class="text-muted-foreground text-xs">This node</p>
                <p class="font-mono break-all select-all">{props.node}</p>
              </div>
              <div>
                <p class="text-muted-foreground text-xs">Group</p>
                <p class="font-mono break-all select-all">{props.group.id}</p>
              </div>
            </HoverCardContent>
          </HoverCardPortal>
        </HoverCard>
      </p>
    </div>
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
  const [inviting, setInviting] = createSignal(false);
  const [adding, setAdding] = createSignal(false);
  // Bumped by the welcome banner's "Add a folder".
  const [newFolder, setNewFolder] = createSignal(0);
  // The folder last queued on this page, for its banner; cleared by its X.
  const [queued, setQueued] = createSignal<{ name: string; within: number }>();
  const arrived = () => {
    const folder = queued();
    return folder !== undefined && props.group.folders.some((f) => f.name === folder.name);
  };
  // The member a dialog shows stays while it closes.
  const lastRenamed = createMemo<string | undefined>((last) => renaming() ?? last);
  const lastRemoved = createMemo<string | undefined>((last) => removing() ?? last);
  const memberHelper = columnsOf<MemberInfo>();
  const memberColumns = memberHelper.columns([
    memberHelper.accessor((m) => m.label, {
      id: "member",
      header: "Member",
      cell: (info) => <span title={info.row.original.id}>{info.getValue()}</span>,
    }),
    // This node, then online, then offline.
    memberHelper.accessor((m) => (m.this_node ? 0 : m.online ? 1 : 2), {
      id: "state",
      header: "State",
      cell: (info) => <MemberState member={info.row.original} />,
    }),
    memberHelper.accessor((m) => connectionRank(m.peer), {
      id: "connection",
      header: "Connection",
      cell: (info) => (
        <Show when={info.row.original.peer} fallback="–">
          {(peer) => <Connection peer={peer()} />}
        </Show>
      ),
    }),
    memberHelper.accessor((m) => (m.peer ? sent(m.peer) + received(m.peer) : -1), {
      id: "traffic",
      header: "Sent / received",
      meta: { class: "text-right tabular-nums" },
      cell: (info) => (
        <Show when={info.row.original.peer} fallback="–">
          {(peer) => (
            <span
              title={`direct ↑${bytes(peer().traffic.direct_sent)} ↓${bytes(
                peer().traffic.direct_recv,
              )}, relay ↑${bytes(peer().traffic.relay_sent)} ↓${bytes(peer().traffic.relay_recv)}`}
            >
              ↑{bytes(sent(peer()))} ↓{bytes(received(peer()))}
            </span>
          )}
        </Show>
      ),
    }),
    // The value sorts; the cell reads the name again, so a rename shows.
    // eslint-disable-next-line solid/reactivity
    memberHelper.accessor((m) => (m.id === props.group.founder ? "founder" : label(m.added_by)), {
      id: "added_by",
      header: "Added by",
      cell: (info) => {
        const m = info.row.original;
        return m.id === props.group.founder ? "founder" : label(m.added_by);
      },
    }),
    memberHelper.display({
      id: "actions",
      header: "",
      cell: (info) => {
        const m = info.row.original;
        return (
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
        );
      },
    }),
  ]);
  const removedHelper = columnsOf<RemovedInfo>();
  const removedColumns = removedHelper.columns([
    removedHelper.accessor((r) => r.id, {
      id: "node",
      header: "Node",
      meta: { class: "font-mono" },
      cell: (info) => <span title={info.getValue()}>{info.getValue().slice(0, 10)}</span>,
    }),
    // eslint-disable-next-line solid/reactivity
    removedHelper.accessor((r) => label(r.removed_by), {
      id: "removed_by",
      header: "Removed by",
      cell: (info) => label(info.row.original.removed_by),
    }),
    removedHelper.accessor((r) => r.removed_at, {
      id: "when",
      header: "When",
      cell: (info) => new Date(info.getValue() * 1000).toLocaleString(),
    }),
  ]);
  return (
    <>
      <GroupHeader node={props.node} group={props.group} label={label} onRename={() => setNaming(true)} />
      {/* The page's banners: news of what was just done, each closed with its X. */}
      <Show when={welcome() || queued()}>
        <div class="space-y-2">
          <Show when={welcome()}>
            {(how) => (
              <Banner
                tone="success"
                icon={<PartyPopper />}
                title={`${how() === "created" ? "Group created" : "Joined the group"}${
                  props.group.name === null ? "" : `: ${props.group.name}`
                }.`}
                actions={
                  <>
                    <Button size="sm" onClick={() => setInviting(true)}>
                      <Ticket class="size-4" />
                      Invite a node
                    </Button>
                    <Button size="sm" variant="outline" onClick={() => setNewFolder((n) => n + 1)}>
                      <FolderPlus class="size-4" />
                      Add a folder
                    </Button>
                  </>
                }
                onClose={() => setWelcome(undefined)}
              >
                {how() === "created"
                  ? "This node is its only member: bring in another node, or start keeping a folder."
                  : "Keep one of the group's folders here, or invite another node."}
              </Banner>
            )}
          </Show>
          <Show when={queued()}>
            {(folder) => (
              <Show
                when={arrived()}
                fallback={
                  <Banner
                    tone="info"
                    icon={<Clock />}
                    title={`Folder ${folder().name} queued.`}
                    onClose={() => setQueued(undefined)}
                  >
                    The node adds it within {folder().within} s; it then shows under Folders.
                  </Banner>
                }
              >
                <Banner
                  tone="success"
                  icon={<CircleCheck />}
                  title={`Folder ${folder().name} is added.`}
                  onClose={() => setQueued(undefined)}
                >
                  It syncs with the other nodes that keep it.
                </Banner>
              </Show>
            )}
          </Show>
        </div>
      </Show>
      <Stats group={props.group} label={label} />

      <SectionCard
        title="Members"
        description={`${props.group.members.length} members, ${
          props.group.members.filter((m) => m.online).length
        } online. Traffic is this node's, since it started.`}
        action={
          <div class="flex gap-2">
            <Button size="sm" variant="outline" onClick={() => setInviting(true)}>
              <Ticket class="size-4" />
              Invite
            </Button>
            <Button size="sm" variant="outline" onClick={() => setAdding(true)}>
              <UserPlus class="size-4" />
              Add by ID
            </Button>
          </div>
        }
      >
        <DataTable
          class={dense}
          data={props.group.members}
          columns={memberColumns}
          rowId={(m) => m.id}
        />

        <Show when={props.group.removed.length > 0}>
          <div class="mt-4 space-y-1">
            <h3 class="text-sm font-medium">Removed</h3>
            <p class="text-muted-foreground text-sm">
              No member connects to these nodes or syncs with them. They can't come back with the
              same ID.
            </p>
            <DataTable
              class={dense}
              data={props.group.removed}
              columns={removedColumns}
              rowId={(r) => r.id}
            />
          </div>
        </Show>
      </SectionCard>

      <FoldersSection
        group={props.group}
        newFolder={newFolder()}
        onQueued={(name, within) => setQueued({ name, within })}
      />

      <InviteDialog open={inviting()} onClose={() => setInviting(false)} />
      <AddMemberDialog open={adding()} node={props.node} onClose={() => setAdding(false)} />
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
    </>
  );
}

/** A row of the folders table: a folder this node keeps, or one it could keep. */
type FolderRow = { id: string; name: string; kept?: FolderInfo; available?: AvailableInfo };

/** A row being edited in the folders table: a new folder, or keeping an available one. */
type Editing = { kind: "new" } | { kind: "map"; name: string };

/** The folders this node keeps, those it could keep, and a row to add one. */
function FoldersSection(props: {
  group: GroupStatus;
  newFolder: number;
  /** A folder was queued from here: for the page's banner. */
  onQueued: (name: string, withinSecs: number) => void;
}) {
  const [card, setCard] = createSignal<HTMLDivElement>();
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
  // The folder a new folder's name is already taken by, kept here or
  // available: names are unique in the group. (Two nodes creating the same
  // name while apart settle on one doc, as ever.)
  const taken = createMemo(() => {
    if (editing()?.kind !== "new") return undefined;
    const typed = name().trim();
    if (typed === "") return undefined;
    if (props.group.folders.some((f) => f.name === typed)) return { name: typed, kept: true };
    if (props.group.available.some((f) => f.name === typed)) return { name: typed, kept: false };
    return undefined;
  });
  const save = async () => {
    if (taken()) return;
    const folder = name().trim();
    await startAdd({ name: folder, path: path() });
    if (added().answer) {
      setEditing(undefined);
      props.onQueued(folder, added().answer?.within_secs ?? 5);
    }
  };
  const empty = () =>
    props.group.folders.length === 0 && props.group.available.length === 0 && !editing();
  // "Add a folder" elsewhere on the page: the new folder row, in view.
  createEffect(
    on(
      () => props.newFolder,
      () => {
        edit({ kind: "new" });
        card()?.scrollIntoView({ behavior: "smooth", block: "start" });
      },
      { defer: true },
    ),
  );
  // Kept folders, then the available ones: one table.
  const rows = createMemo<FolderRow[]>(() => [
    ...props.group.folders.map((kept) => ({ id: kept.id, name: kept.name, kept })),
    ...props.group.available.map((available) => ({
      id: `available:${available.id}`,
      name: available.name,
      available,
    })),
  ]);
  const helper = columnsOf<FolderRow>();
  const columns = helper.columns([
    helper.accessor((row) => row.name, {
      id: "folder",
      header: "Folder",
      cell: (info) => (
        <Show when={info.row.original.kept} fallback={info.getValue()}>
          {(kept) => (
            <Link
              to="/folders/$name"
              params={{ name: kept().name }}
              class={buttonVariants({ variant: "link", class: "h-auto p-0" })}
            >
              {kept().name}
            </Link>
          )}
        </Show>
      ),
    }),
    helper.accessor((row) => row.kept?.path ?? "", {
      id: "directory",
      header: "Directory on this node",
      cell: (info) => (
        <Show
          when={info.row.original.kept}
          fallback={
            <Show when={mapping(info.row.original.name)} fallback="not kept here">
              <PathInput value={path()} onChange={setPath} />
            </Show>
          }
        >
          {(kept) => <span class="font-mono">{kept().path}</span>}
        </Show>
      ),
    }),
    helper.accessor((row) => row.kept?.files ?? -1, {
      id: "files",
      header: "Files",
      meta: { class: "text-right tabular-nums" },
      cell: (info) => (info.getValue() < 0 ? "–" : info.getValue()),
    }),
    helper.accessor((row) => row.kept?.bytes ?? -1, {
      id: "size",
      header: "Size",
      meta: { class: "text-right tabular-nums" },
      cell: (info) => (info.getValue() < 0 ? "–" : bytes(info.getValue())),
    }),
    helper.accessor((row) => (row.kept ? folderStateText(row.kept) : "available"), {
      id: "state",
      header: "State",
      cell: (info) => (
        <Show
          when={info.row.original.kept}
          fallback={
            <Badge variant="outline">
              available, kept by {info.row.original.available?.nodes ?? 0} nodes
            </Badge>
          }
        >
          {(kept) => <FolderState folder={kept()} />}
        </Show>
      ),
    }),
    helper.display({
      id: "actions",
      header: "On this node",
      meta: { class: "text-right" },
      cell: (info) => (
        <Show
          when={info.row.original.kept}
          fallback={
            <Show
              when={mapping(info.row.original.name)}
              fallback={
                <div class="flex justify-end">
                  <Button
                    size="sm"
                    variant="ghost"
                    type="button"
                    onClick={() => edit({ kind: "map", name: info.row.original.name })}
                  >
                    <FolderInput class="size-4" />
                    Keep here
                  </Button>
                </div>
              }
            >
              <EditButtons busy={added().busy} onCancel={() => setEditing(undefined)} />
            </Show>
          }
        >
          {(kept) => (
            <div class="flex items-center justify-end gap-1">
              <FolderSwitch folder={kept()} />
              <Button
                size="icon-sm"
                variant="ghost"
                type="button"
                title="Remove from this node"
                aria-label={`Remove ${kept().name} from this node`}
                onClick={() => setRemoving(kept().name)}
              >
                <Trash class="size-4" />
              </Button>
            </div>
          )}
        </Show>
      ),
    }),
  ]);
  return (
    <div ref={setCard}>
      <SectionCard
        title="Folders"
        description="Kept here, and available from the group: keep one here by giving it a directory."
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
            <DataTable
              class={dense}
              data={rows()}
              columns={columns}
              rowId={(row) => row.id}
              rowClass={(row) =>
                cx(
                  row.available && "text-muted-foreground",
                  // The folder a new folder's name is taken by.
                  row.name === taken()?.name &&
                    "bg-amber-50 text-foreground ring-1 ring-amber-300 ring-inset hover:bg-amber-50 dark:bg-amber-950/40 dark:ring-amber-800 dark:hover:bg-amber-950/40",
                )
              }
              after={
                <Show when={editing()?.kind === "new"}>
                  <TableRow>
                    <TableCell class="align-top whitespace-normal">
                      <TextField
                        value={name()}
                        onChange={setName}
                        validationState={taken() ? "invalid" : "valid"}
                      >
                        <TextFieldLabel class="sr-only">Folder name</TextFieldLabel>
                        <TextFieldInput class="h-8" placeholder="name, the same on every node" required />
                        <TextFieldErrorMessage class="max-w-56">
                          <Show
                            when={taken()?.kept}
                            fallback={
                              <>
                                The group has a folder {taken()?.name} (highlighted).{" "}
                                <button
                                  type="button"
                                  class="underline underline-offset-2"
                                  onClick={() => edit({ kind: "map", name: taken()?.name ?? "" })}
                                >
                                  Keep it here
                                </button>{" "}
                                instead.
                              </>
                            }
                          >
                            This node keeps {taken()?.name} already (highlighted).
                          </Show>
                        </TextFieldErrorMessage>
                      </TextField>
                    </TableCell>
                    <TableCell colSpan={4} class="align-top">
                      <PathInput value={path()} onChange={setPath} />
                    </TableCell>
                    <TableCell class="align-top">
                      <EditButtons
                        busy={added().busy}
                        disabled={taken() !== undefined}
                        onCancel={() => setEditing(undefined)}
                      />
                    </TableCell>
                  </TableRow>
                </Show>
              }
            />
          </Form>
        </Show>
        <Failed state={added()} />
        <RemoveFolderDialog
          open={removing() !== undefined}
          folder={props.group.folders.find((f) => f.name === lastRemoved())}
          onClose={() => setRemoving(undefined)}
        />
      </SectionCard>
    </div>
  );
}

/** A kept folder's state in words, to sort by. */
function folderStateText(folder: FolderInfo): string {
  if (folder.disabled) return "disabled";
  if (folder.paused) return "paused";
  return "synced";
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

function EditButtons(props: { busy: boolean; disabled?: boolean; onCancel: () => void }) {
  return (
    <div class="flex justify-end gap-1">
      <Button size="sm" type="submit" disabled={props.busy || props.disabled}>
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

/** Direct first, then relay, then not connected, then never connected: to sort by. */
function connectionRank(peer: PeerInfo | null): number {
  if (!peer) return 3;
  if (peer.connections === 0) return 2;
  return peer.kind === "Direct" ? 0 : 1;
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
