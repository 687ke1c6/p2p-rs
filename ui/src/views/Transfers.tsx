import { For, Show } from "solid-js";
import type { SwarmInfo } from "@/api/SwarmInfo";
import type { SwarmPeerInfo } from "@/api/SwarmPeerInfo";
import type { SwarmPeerKind } from "@/api/SwarmPeerKind";
import type { UploadInfo } from "@/api/UploadInfo";
import { Badge, type BadgeProps } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress, ProgressGroup, ProgressLabel, ProgressValueLabel } from "@/components/ui/progress";
import { bytes } from "@/lib/format";
import { live } from "@/lib/socket";
import { columnsOf, DataTable, dense } from "./DataTable";
import { LiveError } from "./LiveError";

/** The developer view: this node's downloads (swarms, pieces, peers) and uploads (choking, rates). */
export function Transfers() {
  const transfers = live("transfers", "Transfers");
  return (
    <div class="space-y-4">
      <LiveError error={transfers().error} />
      <Show when={transfers().value}>
        {(answer) => (
          <>
            <Card>
              <CardHeader>
                <CardTitle>Downloads</CardTitle>
                <CardDescription>
                  <Show when={answer().swarms.length > 0} fallback="This node downloads nothing now.">
                    One swarm per version of a file this node fetches.
                  </Show>
                </CardDescription>
              </CardHeader>
            </Card>
            <For each={answer().swarms}>{(swarm) => <Swarm swarm={swarm} />}</For>
            <Uploads uploads={answer().uploads} />
          </>
        )}
      </Show>
    </div>
  );
}

function Swarm(props: { swarm: SwarmInfo }) {
  const swarm = () => props.swarm;
  return (
    <Card>
      <CardHeader>
        <CardTitle class="flex items-center gap-2">
          <span class="font-mono break-all">
            {swarm().folder}/{swarm().path}
          </span>
          <Show when={swarm().stuck}>
            <Badge variant="destructive">stuck: no peer sends</Badge>
          </Show>
        </CardTitle>
        <CardDescription>
          {bytes(swarm().size)} in {swarm().pieces} pieces of {bytes(swarm().piece_length)}, version{" "}
          <span class="font-mono" title={swarm().content}>
            {swarm().content.slice(0, 10)}
          </span>
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-4">
        <Progress
          value={swarm().have}
          maxValue={Math.max(swarm().pieces, 1)}
          getValueLabel={({ value, max }) => `${value} of ${max} pieces, ${swarm().active} in progress`}
        >
          <ProgressGroup>
            <ProgressLabel>Pieces verified</ProgressLabel>
            <ProgressValueLabel />
          </ProgressGroup>
        </Progress>
        <DataTable
          class={dense}
          data={swarm().peers}
          columns={peerColumns(swarm().pieces)}
          rowId={(peer) => peer.id}
          empty="No node to ask yet."
        />
      </CardContent>
    </Card>
  );
}

/** The peers of a swarm of `pieces` pieces. */
function peerColumns(pieces: number) {
  const helper = columnsOf<SwarmPeerInfo>();
  return helper.columns([
    helper.accessor((peer) => peer.label, {
      id: "peer",
      header: "Peer",
      cell: (info) => <span title={info.row.original.id}>{info.getValue()}</span>,
    }),
    helper.accessor((peer) => peerText[peer.state], {
      id: "swarm",
      header: "Swarm",
      cell: (info) => (
        <Badge variant={peerVariant[info.row.original.state]}>{info.getValue()}</Badge>
      ),
    }),
    helper.display({
      id: "choking",
      header: "Choking",
      cell: (info) => (
        <Show when={info.row.original.in_swarm} fallback="–">
          {(wire) => (
            <span class="space-x-1">
              <Show when={wire().unchoked} fallback={<Badge variant="outline">choked</Badge>}>
                <Badge variant="secondary">unchoked</Badge>
              </Show>
              <Show when={!wire().interested}>
                <Badge variant="outline">not interested</Badge>
              </Show>
              <Show when={wire().snubbed}>
                <Badge variant="destructive">snubbed</Badge>
              </Show>
            </span>
          )}
        </Show>
      ),
    }),
    helper.accessor((peer) => peer.in_swarm?.rate ?? -1, {
      id: "rate",
      header: "Rate ↓",
      meta: { class: "text-right tabular-nums" },
      cell: (info) => (info.getValue() < 0 ? "–" : `${bytes(info.getValue())}/s`),
    }),
    helper.accessor((peer) => peer.in_swarm?.open ?? -1, {
      id: "open",
      header: "Open requests",
      meta: { class: "text-right tabular-nums" },
      cell: (info) => (info.getValue() < 0 ? "–" : info.getValue()),
    }),
    helper.accessor((peer) => peer.in_swarm?.pieces ?? -1, {
      id: "pieces",
      header: "Its pieces",
      meta: { class: "text-right tabular-nums" },
      cell: (info) => (info.getValue() < 0 ? "–" : `${info.getValue()} of ${pieces}`),
    }),
  ]);
}

const peerText: Record<SwarmPeerKind, string> = {
  In: "in",
  Joining: "joining",
  Out: "out, asked again",
  Banned: "dropped for this download",
};

const peerVariant: Record<SwarmPeerKind, BadgeProps["variant"]> = {
  In: "secondary",
  Joining: "outline",
  Out: "outline",
  Banned: "destructive",
};

function Uploads(props: { uploads: UploadInfo[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Uploads</CardTitle>
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
