import { Link } from "@tanstack/solid-router";
import ArrowLeft from "lucide-solid/icons/arrow-left";
import { createSignal, Show } from "solid-js";
import type { FolderViewAnswer } from "@/api/FolderViewAnswer";
import type { PathClass } from "@/api/PathClass";
import type { PathInfo } from "@/api/PathInfo";
import { Badge, type BadgeProps } from "@/components/ui/badge";
import { buttonVariants } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Switch,
  SwitchControl,
  SwitchInput,
  SwitchLabel,
  SwitchThumb,
} from "@/components/ui/switch";
import { bytes, secondsSince } from "@/lib/format";
import { live } from "@/lib/socket";
import { columnsOf, DataTable, dense } from "./DataTable";
import { LiveError } from "./LiveError";

/** A folder's merged view: the data of `sync-node folder show`. */
export function Folder(props: { name: string }) {
  // The route mounts a new view for another name, so reading it once is right.
  // eslint-disable-next-line solid/reactivity
  const view = live(`folders/${props.name}`, "Folder");
  return (
    <div class="space-y-4">
      <Link to="/" class={buttonVariants({ variant: "ghost", size: "sm" })}>
        <ArrowLeft class="size-4" />
        Dashboard
      </Link>
      <LiveError error={view().error} />
      <Show when={view().value}>{(answer) => <FolderDetails view={answer()} />}</Show>
    </div>
  );
}

function FolderDetails(props: { view: FolderViewAnswer }) {
  const [all, setAll] = createSignal(false);
  const label = (id: string) => props.view.nodes.find((n) => n.id === id)?.label ?? id.slice(0, 10);
  const shown = () =>
    props.view.paths.filter(
      (p) => all() || (p.class !== "InSync" && p.class !== "DeletedEverywhere"),
    );
  const c = () => props.view.counts;
  const helper = columnsOf<PathInfo>();
  const columns = helper.columns([
    helper.accessor((p) => p.path, {
      id: "path",
      header: "Path",
      meta: { class: "font-mono" },
    }),
    helper.accessor((p) => classText[p.class], {
      id: "state",
      header: "State",
      cell: (info) => (
        <Badge variant={classVariant[info.row.original.class]}>{info.getValue()}</Badge>
      ),
    }),
    helper.accessor((p) => p.size ?? -1, {
      id: "size",
      header: "Size",
      meta: { class: "text-right tabular-nums" },
      cell: (info) => (info.getValue() < 0 ? "–" : bytes(info.getValue())),
    }),
    // The value sorts and filters; the cell reads the names again, so a
    // rename shows.
    // eslint-disable-next-line solid/reactivity
    helper.accessor((p) => detail(p, label), {
      id: "detail",
      header: "Detail",
      meta: { class: "whitespace-normal" },
      cell: (info) => detail(info.row.original, label),
    }),
  ]);
  return (
    <Card>
      <CardHeader>
        <CardTitle>{props.view.name}</CardTitle>
        <CardDescription>
          {props.view.nodes.length} nodes: {props.view.nodes.map((n) => n.label).join(", ")}. As of{" "}
          {secondsSince(props.view.written_at)}s ago.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="flex flex-wrap gap-2">
          <Badge variant="secondary">{c().in_sync} in sync</Badge>
          <Badge>{c().to_fetch} to fetch</Badge>
          <Badge variant="outline">{c().to_delete} to delete</Badge>
          <Badge variant="destructive">{c().conflicts} conflicts</Badge>
          <Badge variant="outline">{c().deleted_everywhere} deleted everywhere</Badge>
        </div>
        <Switch class="flex items-center gap-x-2" checked={all()} onChange={setAll}>
          <SwitchInput />
          <SwitchControl>
            <SwitchThumb />
          </SwitchControl>
          <SwitchLabel>Show files in sync</SwitchLabel>
        </Switch>
        <Show
          when={shown().length > 0}
          fallback={<p class="text-muted-foreground text-sm">Every file is in sync.</p>}
        >
          <DataTable
            class={dense}
            data={shown()}
            columns={columns}
            rowId={(p) => p.path}
            filter="Filter paths and details"
            pageSize={50}
            empty="No path matches."
          />
        </Show>
      </CardContent>
    </Card>
  );
}

/** What happens to a path, as `folder show` says it. */
function detail(path: PathInfo, label: (id: string) => string): string {
  const list = (ids: string[]) => ids.map(label).join(", ");
  const withState = (state: string) => path.nodes.filter((n) => n.state === state).map((n) => n.id);
  switch (path.class) {
    case "InSync":
      return "in sync";
    case "DeletedEverywhere":
      return "deleted everywhere";
    case "ToDelete":
      return `deleted; ${list(withState("WillDelete"))} will delete it`;
    case "ToFetch": {
      const behind = path.nodes.filter((n) => n.state !== "Current").map((n) => n.id);
      const has = withState("Current");
      if (has.length === 0 && path.source) has.push(path.source);
      return `from ${list(has)}; ${list(behind)} will fetch it`;
    }
    case "Conflict": {
      const copies = path.copies.map((copy) => `; ${label(copy.node)}'s becomes ${copy.name}`);
      return `${label(path.source ?? "")}'s version keeps the name${copies.join("")}`;
    }
  }
}

const classText: Record<PathClass, string> = {
  InSync: "in sync",
  DeletedEverywhere: "deleted everywhere",
  ToFetch: "to fetch",
  ToDelete: "to delete",
  Conflict: "conflict",
};

const classVariant: Record<PathClass, BadgeProps["variant"]> = {
  InSync: "secondary",
  DeletedEverywhere: "outline",
  ToFetch: "default",
  ToDelete: "outline",
  Conflict: "destructive",
};
