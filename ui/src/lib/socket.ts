import { type Accessor, createSignal, onCleanup } from "solid-js";
import { firstValueFrom, map, type Observable, repeat, retry, timer } from "rxjs";
import { webSocket } from "rxjs/webSocket";
import type { ClientMessage } from "@/api/ClientMessage";
import type { ServerMessage } from "@/api/ServerMessage";
import type { Update } from "@/api/Update";

/** How long the page waits before it connects again. */
const RECONNECT_MS = 2000;

/** The page's one WebSocket (ADR 0005). It opens with the first UI topic and closes after the last. */
const socket = webSocket<ServerMessage>(
  `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/api/ws`,
);

/** UI topic `name`'s updates: subscribing subscribes to the topic on the socket. */
export function updates(name: string): Observable<Update> {
  const send = (message: ClientMessage) => message;
  return socket
    .multiplex(
      () => send({ subscribe: name }),
      () => send({ unsubscribe: name }),
      (message) => message.topic === name,
    )
    .pipe(map((message) => message.update));
}

/** The latest data of a UI topic, and the error since, if any. */
export type Live<T> = { value?: T; error?: string };

type Kind = Exclude<Update["kind"], "Error">;
type DataOf<K extends Kind> = Extract<Update, { kind: K }>["data"];

/**
 * UI topic `name` while the calling view is shown. When the socket closes or
 * fails, the last data stays beside the error and the page connects again.
 */
export function live<K extends Kind>(name: string, kind: K): Accessor<Live<DataOf<K>>> {
  const [state, setState] = createSignal<Live<unknown>>({});
  const fail = (error: string) => setState((last) => ({ value: last.value, error }));
  const again = () => {
    fail("can't reach the node");
    return timer(RECONNECT_MS);
  };
  // A closed socket (the node stopping) completes the stream; a failed one errors it.
  const subscription = updates(name)
    .pipe(repeat({ delay: again }), retry({ delay: again }))
    .subscribe((update) => {
      if (update.kind === "Error") fail(update.data.error);
      else if (update.kind === kind) setState({ value: update.data });
    });
  onCleanup(() => subscription.unsubscribe());
  // Only data of `kind` is set.
  return state as Accessor<Live<DataOf<K>>>;
}

/** UI topic `name`'s first data, once. */
export async function once<K extends Kind>(name: string, kind: K): Promise<DataOf<K>> {
  const update = await firstValueFrom(updates(name)).catch(() => {
    throw new Error("can't reach the node");
  });
  if (update.kind === "Error") throw new Error(update.data.error);
  if (update.kind !== kind) throw new Error(`unexpected ${update.kind} on UI topic ${name}`);
  return update.data as DataOf<K>;
}
