import { For, Show } from "solid-js";
import type { SwarmInfo } from "@/api/SwarmInfo";
import type { SwarmPeerInfo } from "@/api/SwarmPeerInfo";
import type { SwarmPeerKind } from "@/api/SwarmPeerKind";
import type { UploadInfo } from "@/api/UploadInfo";
import { Badge, type BadgeProps } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress, ProgressGroup, ProgressLabel, ProgressValueLabel } from "@/components/ui/progress";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { bytes } from "@/lib/format";
import { live } from "@/lib/socket";
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
        <Show
          when={swarm().peers.length > 0}
          fallback={<p class="text-sm text-muted-foreground">No node to ask yet.</p>}
        >
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Peer</TableHead>
                <TableHead>Swarm</TableHead>
                <TableHead>Choking</TableHead>
                <TableHead class="text-right">Rate ↓</TableHead>
                <TableHead class="text-right">Open requests</TableHead>
                <TableHead class="text-right">Its pieces</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <For each={swarm().peers}>{(peer) => <SwarmPeer peer={peer} pieces={swarm().pieces} />}</For>
            </TableBody>
          </Table>
        </Show>
      </CardContent>
    </Card>
  );
}

function SwarmPeer(props: { peer: SwarmPeerInfo; pieces: number }) {
  return (
    <TableRow>
      <TableCell title={props.peer.id}>{props.peer.label}</TableCell>
      <TableCell>
        <Badge variant={peerVariant[props.peer.state]}>{peerText[props.peer.state]}</Badge>
      </TableCell>
      <Show
        when={props.peer.in_swarm}
        fallback={
          <>
            <TableCell>–</TableCell>
            <TableCell class="text-right">–</TableCell>
            <TableCell class="text-right">–</TableCell>
            <TableCell class="text-right">–</TableCell>
          </>
        }
      >
        {(wire) => (
          <>
            <TableCell class="space-x-1">
              <Show when={wire().unchoked} fallback={<Badge variant="outline">choked</Badge>}>
                <Badge variant="secondary">unchoked</Badge>
              </Show>
              <Show when={!wire().interested}>
                <Badge variant="outline">not interested</Badge>
              </Show>
              <Show when={wire().snubbed}>
                <Badge variant="destructive">snubbed</Badge>
              </Show>
            </TableCell>
            <TableCell class="text-right">{bytes(wire().rate)}/s</TableCell>
            <TableCell class="text-right">{wire().open}</TableCell>
            <TableCell class="text-right">
              {wire().pieces} of {props.pieces}
            </TableCell>
          </>
        )}
      </Show>
    </TableRow>
  );
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
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Peer</TableHead>
                <TableHead class="text-right">Swarms</TableHead>
                <TableHead>Choker</TableHead>
                <TableHead class="text-right">Rate ↑</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <For each={props.uploads}>
                {(upload) => (
                  <TableRow>
                    <TableCell title={upload.id}>{upload.label}</TableCell>
                    <TableCell class="text-right">{upload.streams}</TableCell>
                    <TableCell class="space-x-1">
                      <Show when={upload.unchoked} fallback={<Badge variant="outline">choked</Badge>}>
                        <Badge variant="secondary">unchoked</Badge>
                      </Show>
                      <Show when={upload.optimistic}>
                        <Badge>optimistic</Badge>
                      </Show>
                      <Show when={!upload.interested}>
                        <Badge variant="outline">not interested</Badge>
                      </Show>
                    </TableCell>
                    <TableCell class="text-right">{bytes(upload.rate)}/s</TableCell>
                  </TableRow>
                )}
              </For>
            </TableBody>
          </Table>
        </CardContent>
      </Show>
    </Card>
  );
}
