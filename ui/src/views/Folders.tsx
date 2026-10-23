import { Link } from "@tanstack/solid-router";
import { For, Show } from "solid-js";
import { Badge } from "@/components/ui/badge";
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

/** The folders this node keeps. Open one for its merged view. */
export function Folders() {
  const folders = live("folders", "Folders");
  return (
    <div class="space-y-4">
      <LiveError error={folders().error} />
      <Show when={folders().value}>
        {(list) => (
          <Card>
            <CardHeader>
              <CardTitle>Folders</CardTitle>
              <CardDescription>
                <Show when={list().length > 0} fallback="This node keeps no folders.">
                  The folders this node keeps. Open one to see what it will look like once synced.
                </Show>
              </CardDescription>
            </CardHeader>
            <Show when={list().length > 0}>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Folder</TableHead>
                      <TableHead>Path</TableHead>
                      <TableHead class="text-right">Files</TableHead>
                      <TableHead class="text-right">Size</TableHead>
                      <TableHead>State</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    <For each={list()}>
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
                          <TableCell class="text-right">{folder.files}</TableCell>
                          <TableCell class="text-right">{bytes(folder.bytes)}</TableCell>
                          <TableCell>
                            <Show
                              when={!folder.paused}
                              fallback={
                                <Badge variant="destructive">paused: root or its marker missing</Badge>
                              }
                            >
                              <Badge variant="secondary">synced with {folder.peers} nodes</Badge>
                            </Show>
                          </TableCell>
                        </TableRow>
                      )}
                    </For>
                  </TableBody>
                </Table>
              </CardContent>
            </Show>
          </Card>
        )}
      </Show>
    </div>
  );
}
