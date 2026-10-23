import { Link } from "@tanstack/solid-router";
import { For, Show } from "solid-js";
import type { GroupStatus } from "@/api/GroupStatus";
import type { LinkState } from "@/api/LinkState";
import type { MemberInfo } from "@/api/MemberInfo";
import type { PeerInfo } from "@/api/PeerInfo";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge, type BadgeProps } from "@/components/ui/badge";
import { buttonVariants } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
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

/** The data of `sync-node status`. */
export function Group() {
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
                      Create or join one
                    </Link>
                  </p>
                </AlertDescription>
              </Alert>
            }
          >
            {(group) => <GroupDetails node={answer().node} group={group()} />}
          </Show>
        )}
      </Show>
    </div>
  );
}

function GroupDetails(props: { node: string; group: GroupStatus }) {
  const label = (id: string) => props.group.members.find((m) => m.id === id)?.label ?? id.slice(0, 10);
  const online = () => props.group.members.filter((m) => m.online).length;
  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>Group</CardTitle>
          <CardDescription>Founded by {label(props.group.founder)}</CardDescription>
        </CardHeader>
        <CardContent class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
          <span class="text-muted-foreground">node</span>
          <span class="font-mono break-all">{props.node}</span>
          <span class="text-muted-foreground">group</span>
          <span class="font-mono break-all">{props.group.id}</span>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Members</CardTitle>
          <CardDescription>
            {props.group.members.length} members, {online()} online. Traffic is this node's, since it
            started.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Member</TableHead>
                <TableHead>State</TableHead>
                <TableHead>Added by</TableHead>
                <TableHead>Connection</TableHead>
                <TableHead class="text-right">Direct ↑ / ↓</TableHead>
                <TableHead class="text-right">Relay ↑ / ↓</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <For each={props.group.members}>
                {(member) => (
                  <TableRow>
                    <TableCell title={member.id}>{member.label}</TableCell>
                    <TableCell>
                      <MemberState member={member} />
                    </TableCell>
                    <TableCell>
                      {member.id === props.group.founder ? "founder" : label(member.added_by)}
                    </TableCell>
                    <TableCell>
                      <Show when={member.peer} fallback="–">
                        {(peer) => connection(peer())}
                      </Show>
                    </TableCell>
                    <TableCell class="text-right">
                      <Show when={member.peer} fallback="–">
                        {(peer) =>
                          `${bytes(peer().traffic.direct_sent)} / ${bytes(peer().traffic.direct_recv)}`
                        }
                      </Show>
                    </TableCell>
                    <TableCell class="text-right">
                      <Show when={member.peer} fallback="–">
                        {(peer) =>
                          `${bytes(peer().traffic.relay_sent)} / ${bytes(peer().traffic.relay_recv)}`
                        }
                      </Show>
                    </TableCell>
                  </TableRow>
                )}
              </For>
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Show when={props.group.links.length > 0}>
        <Card>
          <CardHeader>
            <CardTitle>Links</CardTitle>
            <CardDescription>Each pair's connection, as the group reports it.</CardDescription>
          </CardHeader>
          <CardContent>
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
          </CardContent>
        </Card>
      </Show>
    </>
  );
}

function MemberState(props: { member: MemberInfo }) {
  return (
    <Show
      when={!props.member.this_node}
      fallback={<Badge>this node</Badge>}
    >
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

/** This node's connection to a peer, as `status` shows it. */
function connection(peer: PeerInfo): string {
  if (peer.connections === 0) return "not connected";
  // The path names its kind already: `direct 1.2.3.4:5`, `relay https://…`.
  const path = peer.path ?? "no path selected";
  const rtt = peer.rtt_ms === null ? "" : `, rtt ${peer.rtt_ms} ms`;
  return `${path}, ${peer.connections} conn${rtt}`;
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
