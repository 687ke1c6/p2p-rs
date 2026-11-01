import ArrowDown from "lucide-solid/icons/arrow-down";
import ArrowUp from "lucide-solid/icons/arrow-up";
import { createMemo, createSignal, For, type JSX, onCleanup, Show } from "solid-js";
import type { LinkKind } from "@/api/LinkKind";
import type { SwarmPeerKind } from "@/api/SwarmPeerKind";
import type { TransferDirection } from "@/api/TransferDirection";
import type { TransferInfo } from "@/api/TransferInfo";
import type { TransferState } from "@/api/TransferState";
import type { UploadInfo } from "@/api/UploadInfo";
import { Badge, type BadgeProps } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  HoverCard,
  HoverCardContent,
  HoverCardPortal,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import { Progress } from "@/components/ui/progress";
import { bytes, duration } from "@/lib/format";
import { live } from "@/lib/socket";
import { columnsOf, DataTable, dense } from "./DataTable";
import { LiveError } from "./LiveError";

/** How this node reaches a peer now: a path's kind, or not connected (`null`). */
type Reach = (id: string) => LinkKind | null;

/**
 * The developer view: a log of this node's last downloads and uploads, the
 * running ones live, with their swarms and choking on hover.
 */
export function Transfers() {
  const transfers = live("transfers", "Transfers");
  const status = live("status", "Status");
  const [direction, setDirection] = createSignal<TransferDirection>();
  // Ticks for the running transfers' durations.
  const [now, setNow] = createSignal(Date.now());
  const clock = setInterval(() => setNow(Date.now()), 1000);
  onCleanup(() => clearInterval(clock));
  const reach: Reach = (id) => {
    const peer = status().value?.group?.members.find((m) => m.id === id)?.peer;
    return peer && peer.connections > 0 ? peer.kind : null;
  };
  const upload = (id: string) => transfers().value?.uploads.find((u) => u.id === id);
  // Reads only each row's direction: a new array only when rows come or go.
  const shown = createMemo(() =>
    (transfers().value?.transfers ?? []).filter((t) => !direction() || t.direction === direction()),
  );
  const running = (dir: TransferDirection) =>
    (transfers().value?.transfers ?? []).filter((t) => t.state === "Running" && t.direction === dir);
  const rate = (dir: TransferDirection) => running(dir).reduce((sum, t) => sum + t.rate, 0);
  return (
    <div class="space-y-4">
      <LiveError error={transfers().error} />
      <Show when={transfers().value}>
        <Card>
          <CardHeader>
            <div class="flex flex-wrap items-center justify-between gap-2">
              <div class="space-y-1">
                <CardTitle>Transfers</CardTitle>
                <CardDescription>
                  This node's last 500 downloads and uploads, newest first. Each download try of a
                  file is one row, and each upload of a file to one peer. Hover over the progress for
                  the swarm.
                </CardDescription>
              </div>
              <div class="flex gap-1">
                <For each={[undefined, "Download", "Upload"] as const}>
                  {(dir) => (
                    <Button
                      size="sm"
                      variant={direction() === dir ? "secondary" : "ghost"}
                      onClick={() => setDirection(dir)}
                    >
                      {dir === undefined ? "All" : dir === "Download" ? "Downloads" : "Uploads"}
                    </Button>
                  )}
                </For>
              </div>
            </div>
            <p class="text-muted-foreground flex flex-wrap gap-x-4 text-sm">
              <span class="inline-flex items-center gap-1">
                <ArrowDown class="size-4" />
                {running("Download").length} running, {bytes(rate("Download"))}/s
              </span>
              <span class="inline-flex items-center gap-1">
                <ArrowUp class="size-4" />
                {running("Upload").length} running, {bytes(rate("Upload"))}/s
              </span>
            </p>
          </CardHeader>
          <CardContent>
            <DataTable
              class={dense}
              data={shown()}
              columns={transferColumns(now, reach, upload)}
              rowId={(t) => String(t.id)}
              filter="Filter by file, folder or peer"
              pageSize={50}
              empty="No transfer yet."
            />
          </CardContent>
        </Card>
        <Uploads uploads={transfers().value?.uploads ?? []} />
      </Show>
    </div>
  );
}

const stateText: Record<TransferState, string> = {
  Running: "running",
  Done: "done",
  GaveUp: "gave up",
  Failed: "failed",
  Stopped: "stopped",
};

const stateVariant: Record<TransferState, BadgeProps["variant"]> = {
  Running: "default",
  Done: "secondary",
  GaveUp: "outline",
  Failed: "destructive",
  Stopped: "outline",
};

/** Seconds it has run: until now, or until it ended. */
function seconds(t: TransferInfo, now: number): number {
  return ((t.ended_ms ?? now) - t.started_ms) / 1000;
}

/** Its progress, 0 to 1: this node's pieces (a download), the peer's (an upload). */
function progress(t: TransferInfo): number | null {
  const have = t.direction === "Download" ? t.have : t.remote_have;
  return have === null ? null : have / Math.max(t.pieces, 1);
}

/** A running download's time to go at its rate now, or why there is none. */
function eta(t: TransferInfo): string {
  if (t.state !== "Running" || t.direction !== "Download") return "–";
  if (t.swarm?.stuck) return "stuck";
  if (t.rate === 0) return "–";
  const left = Math.min(t.size, (t.pieces - (t.have ?? 0)) * t.piece_length);
  return duration(left / t.rate);
}

/** Its peers: a running download's in its swarm, else those that sent or got bytes. */
function peersOf(t: TransferInfo): { id: string; label: string }[] {
  const swarm = t.swarm?.peers.filter((p) => p.state === "In");
  return swarm && swarm.length > 0 ? swarm : t.peers;
}

function transferColumns(
  now: () => number,
  reach: Reach,
  upload: (id: string) => UploadInfo | undefined,
) {
  const helper = columnsOf<TransferInfo>();
  return helper.columns([
    helper.accessor((t) => t.direction, {
      id: "direction",
      header: "",
      cell: (info) => (
        <Show
          when={info.getValue() === "Download"}
          fallback={<ArrowUp class="size-4" aria-label="upload" />}
        >
          <ArrowDown class="size-4" aria-label="download" />
        </Show>
      ),
    }),
    helper.accessor((t) => `${t.folder}/${t.path}`, {
      id: "file",
      header: "File",
      cell: (info) => (
        <span class="block max-w-xs truncate font-mono" title={`${info.getValue()}\n${info.row.original.content}`}>
          {info.getValue()}
        </span>
      ),
    }),
    helper.accessor((t) => stateText[t.state], {
      id: "state",
      header: "State",
      cell: (info) => {
        const t = info.row.original;
        return (
          <Show
            when={!(t.state === "Running" && t.swarm?.stuck)}
            fallback={<Badge variant="destructive">stuck</Badge>}
          >
            <Badge variant={stateVariant[t.state]} title={t.error ?? undefined}>
              {stateText[t.state]}
            </Badge>
          </Show>
        );
      },
    }),
    helper.display({
      id: "progress",
      header: "Progress",
      cell: (info) => <ProgressCell transfer={info.row.original} reach={reach} upload={upload} />,
    }),
    helper.accessor((t) => t.rate, {
      id: "rate",
      header: "Rate",
      meta: { class: "text-right tabular-nums" },
      cell: (info) => {
        const t = info.row.original;
        return (
          <Show
            when={t.state === "Running"}
            fallback={
              <span class="text-muted-foreground" title="average">
                {bytes(t.bytes / Math.max(seconds(t, now()), 1))}/s
              </span>
            }
          >
            {bytes(t.rate)}/s
          </Show>
        );
      },
    }),
    helper.display({
      id: "eta",
      header: "ETA",
      meta: { class: "text-right tabular-nums" },
      cell: (info) => eta(info.row.original),
    }),
    helper.accessor((t) => t.peers.map((p) => p.label).join(" "), {
      id: "peers",
      header: "Peers",
      cell: (info) => (
        <span class="flex flex-wrap gap-1">
          <For each={peersOf(info.row.original)} fallback={<span class="text-muted-foreground">–</span>}>
            {(peer) => <PeerBadge id={peer.id} label={peer.label} reach={reach} />}
          </For>
        </span>
      ),
    }),
    helper.accessor((t) => t.size, {
      id: "size",
      header: "Size",
      meta: { class: "text-right tabular-nums" },
      cell: (info) => bytes(info.getValue()),
    }),
    helper.accessor((t) => t.started_ms, {
      id: "started",
      header: "Started",
      meta: { class: "tabular-nums" },
      cell: (info) => (
        <span title={new Date(info.getValue()).toLocaleString()}>
          {new Date(info.getValue()).toLocaleTimeString()}
        </span>
      ),
    }),
    helper.display({
      id: "duration",
      header: "Took",
      meta: { class: "text-right tabular-nums" },
      cell: (info) => duration(seconds(info.row.original, now())),
    }),
  ]);
}

/** A peer, styled by how this node reaches it now. */
function PeerBadge(props: { id: string; label: string; reach: Reach }) {
  const kind = () => props.reach(props.id);
  return (
    <Badge
      variant={kind() === "Direct" ? "secondary" : kind() === "Relay" ? "outline" : "destructive"}
      title={`${props.id}\n${kind() === null ? "not connected" : kind() === "Direct" ? "direct" : "relay"}`}
    >
      {props.label}
    </Badge>
  );
}

/** Its progress bar, with its pieces, swarm and choking on hover. */
function ProgressCell(props: {
  transfer: TransferInfo;
  reach: Reach;
  upload: (id: string) => UploadInfo | undefined;
}) {
  const t = () => props.transfer;
  const label = () => {
    const p = progress(t());
    if (p === null) return "–";
    const percent = `${Math.floor(p * 100)}%`;
    return t().direction === "Upload" ? `peer has ${percent}` : percent;
  };
  return (
    <HoverCard openDelay={150} closeDelay={100}>
      <HoverCardTrigger as="div" class="flex min-w-32 cursor-default items-center gap-2">
        <Progress
          class="w-20"
          value={(progress(t()) ?? 0) * 100}
          maxValue={100}
          getValueLabel={() => label()}
        />
        <span class="text-muted-foreground text-xs tabular-nums">{label()}</span>
      </HoverCardTrigger>
      <HoverCardPortal>
        <HoverCardContent class="w-[32rem] space-y-3 p-3 text-sm">
          <Show
            when={t().direction === "Download"}
            fallback={<UploadDetails transfer={t()} upload={props.upload} />}
          >
            <DownloadDetails transfer={t()} reach={props.reach} />
          </Show>
        </HoverCardContent>
      </HoverCardPortal>
    </HoverCard>
  );
}

function Heading(props: { transfer: TransferInfo }) {
  const t = () => props.transfer;
  return (
    <div>
      <p class="font-mono font-medium break-all">
        {t().folder}/{t().path}
      </p>
      <p class="text-muted-foreground text-xs">
        {bytes(t().size)} in {t().pieces} pieces of {bytes(t().piece_length)}, version{" "}
        <span class="font-mono">{t().content.slice(0, 10)}</span>
      </p>
      <Show when={t().error}>
        <p class="text-destructive text-xs">{t().error}</p>
      </Show>
    </div>
  );
}

/** Two columns of label and value. */
function Facts(props: { children: JSX.Element }) {
  return <dl class="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-xs">{props.children}</dl>;
}

function Fact(props: { label: string; children: JSX.Element }) {
  return (
    <>
      <dt class="text-muted-foreground">{props.label}</dt>
      <dd class="tabular-nums">{props.children}</dd>
    </>
  );
}

const peerText: Record<SwarmPeerKind, string> = {
  In: "in",
  Joining: "joining",
  Out: "out, asked again",
  Banned: "dropped",
};

const peerVariant: Record<SwarmPeerKind, BadgeProps["variant"]> = {
  In: "secondary",
  Joining: "outline",
  Out: "outline",
  Banned: "destructive",
};

function DownloadDetails(props: { transfer: TransferInfo; reach: Reach }) {
  const t = () => props.transfer;
  const sent = (id: string) => t().peers.find((p) => p.id === id);
  return (
    <>
      <Heading transfer={t()} />
      <Facts>
        <Fact label="Pieces">
          {t().have ?? 0} of {t().pieces} verified
          <Show when={t().swarm}>{(swarm) => <>, {swarm().active} in progress</>}</Show>
        </Fact>
        <Fact label="Fetched">{bytes(t().bytes)} in this try</Fact>
        <Show when={t().bad_pieces > 0}>
          <Fact label="Bad pieces">{t().bad_pieces}, their senders dropped</Fact>
        </Show>
      </Facts>
      <Show
        when={t().swarm}
        fallback={
          <Show when={t().peers.length > 0}>
            <table class="w-full text-xs">
              <thead class="text-muted-foreground">
                <tr>
                  <th class="text-left font-normal">Sent by</th>
                  <th class="text-right font-normal">Bytes</th>
                  <th class="text-right font-normal">Pieces</th>
                </tr>
              </thead>
              <tbody>
                <For each={t().peers}>
                  {(peer) => (
                    <tr>
                      <td>
                        <PeerBadge id={peer.id} label={peer.label} reach={props.reach} />
                      </td>
                      <td class="text-right tabular-nums">{bytes(peer.bytes)}</td>
                      <td class="text-right tabular-nums">{peer.pieces}</td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </Show>
        }
      >
        {(swarm) => (
          <table class="w-full text-xs">
            <thead class="text-muted-foreground">
              <tr>
                <th class="text-left font-normal">Peer</th>
                <th class="text-left font-normal">Swarm</th>
                <th class="text-right font-normal">Rate ↓</th>
                <th class="text-right font-normal">Open</th>
                <th class="text-right font-normal">Its pieces</th>
                <th class="text-right font-normal">Sent here</th>
              </tr>
            </thead>
            <tbody>
              <For each={swarm().peers}>
                {(peer) => (
                  <tr>
                    <td>
                      <PeerBadge id={peer.id} label={peer.label} reach={props.reach} />
                    </td>
                    <td class="space-x-1">
                      <Badge variant={peerVariant[peer.state]}>{peerText[peer.state]}</Badge>
                      <Show when={peer.in_swarm}>
                        {(wire) => (
                          <>
                            <Show when={!wire().unchoked}>
                              <Badge variant="outline">choked</Badge>
                            </Show>
                            <Show when={wire().snubbed}>
                              <Badge variant="destructive">snubbed</Badge>
                            </Show>
                          </>
                        )}
                      </Show>
                    </td>
                    <td class="text-right tabular-nums">
                      {peer.in_swarm ? `${bytes(peer.in_swarm.rate)}/s` : "–"}
                    </td>
                    <td class="text-right tabular-nums">{peer.in_swarm?.open ?? "–"}</td>
                    <td class="text-right tabular-nums">{peer.in_swarm?.pieces ?? "–"}</td>
                    <td class="text-right tabular-nums">{bytes(sent(peer.id)?.bytes ?? 0)}</td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        )}
      </Show>
    </>
  );
}

function UploadDetails(props: {
  transfer: TransferInfo;
  upload: (id: string) => UploadInfo | undefined;
}) {
  const t = () => props.transfer;
  const peer = () => t().peers[0];
  // The choker's view of the peer, over all its swarms, while this runs.
  const choker = () => (t().state === "Running" && peer() ? props.upload(peer().id) : undefined);
  return (
    <>
      <Heading transfer={t()} />
      <Facts>
        <Fact label="To">{peer()?.label ?? "–"}</Fact>
        <Fact label="Sent">{bytes(t().bytes)}</Fact>
        <Fact label="Peer has">
          {t().remote_have === null ? "–" : `${t().remote_have} of ${t().pieces} pieces`}
        </Fact>
        <Show when={choker()}>
          {(u) => (
            <Fact label="Choker">
              <span class="space-x-1">
                <Show when={u().unchoked} fallback={<Badge variant="outline">choked</Badge>}>
                  <Badge variant="secondary">unchoked</Badge>
                </Show>
                <Show when={u().optimistic}>
                  <Badge>optimistic</Badge>
                </Show>
                <Show when={!u().interested}>
                  <Badge variant="outline">not interested</Badge>
                </Show>
              </span>
            </Fact>
          )}
        </Show>
      </Facts>
    </>
  );
}

/** The choker's view: each peer with swarms open to this node now. */
function Uploads(props: { uploads: UploadInfo[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Upload slots</CardTitle>
        <CardDescription>
          <Show when={props.uploads.length > 0} fallback="No peer has a swarm open to this node.">
            Each peer with swarms open to this node. The choker uploads to the fastest 4 and one
            optimistic unchoke.
          </Show>
        </CardDescription>
      </CardHeader>
      <Show when={props.uploads.length > 0}>
        <CardContent>
          <DataTable
            class={dense}
            data={props.uploads}
            columns={uploadColumns}
            rowId={(upload) => upload.id}
          />
        </CardContent>
      </Show>
    </Card>
  );
}

const uploadHelper = columnsOf<UploadInfo>();

const uploadColumns = uploadHelper.columns([
  uploadHelper.accessor((upload) => upload.label, {
    id: "peer",
    header: "Peer",
    cell: (info) => <span title={info.row.original.id}>{info.getValue()}</span>,
  }),
  uploadHelper.accessor((upload) => upload.streams, {
    id: "swarms",
    header: "Swarms",
    meta: { class: "text-right tabular-nums" },
  }),
  // Unchoked first, the optimistic unchoke among them.
  uploadHelper.accessor((upload) => (upload.unchoked ? 2 : 0) + (upload.optimistic ? 1 : 0), {
    id: "choker",
    header: "Choker",
    cell: (info) => {
      const upload = info.row.original;
      return (
        <span class="space-x-1">
          <Show when={upload.unchoked} fallback={<Badge variant="outline">choked</Badge>}>
            <Badge variant="secondary">unchoked</Badge>
          </Show>
          <Show when={upload.optimistic}>
            <Badge>optimistic</Badge>
          </Show>
          <Show when={!upload.interested}>
            <Badge variant="outline">not interested</Badge>
          </Show>
        </span>
      );
    },
  }),
  uploadHelper.accessor((upload) => upload.rate, {
    id: "rate",
    header: "Rate ↑",
    meta: { class: "text-right tabular-nums" },
    cell: (info) => `${bytes(info.getValue())}/s`,
  }),
]);
