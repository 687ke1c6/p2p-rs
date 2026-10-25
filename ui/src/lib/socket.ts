import { type Accessor, onCleanup } from "solid-js";
import { createStore, reconcile } from "solid-js/store";
import {
  firstValueFrom,
  map,
  type Observable,
  ReplaySubject,
  repeat,
  retry,
  share,
  timer,
} from "rxjs";
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

/** Each UI topic's shared updates, by name. */
const topics = new Map<string, Observable<Update>>();

/**
 * UI topic `name`'s updates: the first subscriber subscribes to the topic on
 * the socket, and the last one to go unsubscribes. Views share it, so a view
 * that mounts before the one it replaces goes doesn't subscribe twice (the
 * server keeps one subscription per topic). A later subscriber gets the last
 * update at once: the server sends only changes.
 */
export function updates(name: string): Observable<Update> {
  let topic = topics.get(name);
  if (!topic) {
    const send = (message: ClientMessage) => message;
    topic = socket
      .multiplex(
        () => send({ subscribe: name }),
        () => send({ unsubscribe: name }),
        (message) => message.topic === name,
      )
      .pipe(
        map((message) => message.update),
        // Reset on every end, so a new subscriber starts afresh on the socket
        // rather than getting old data from a closed one.
        share({
          connector: () => new ReplaySubject<Update>(1),
          resetOnError: true,
          resetOnComplete: true,
          resetOnRefCountZero: true,
        }),
      );
    topics.set(name, topic);
  }
  return topic;
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
  // A store, updated in place: rows keep their identity from one update to
  // the next (matched by `id`), so a view's `<For>` doesn't remake them and
  // lose what they hold, such as a half-typed input.
  const [state, setState] = createStore<Live<unknown>>({});
  const fail = (error: string) => setState("error", error);
  const again = () => {
    fail("can't reach the node");
    return timer(RECONNECT_MS);
  };
  // A closed socket (the node stopping) completes the stream; a failed one errors it.
  const subscription = updates(name)
    .pipe(repeat({ delay: again }), retry({ delay: again }))
    .subscribe((update) => {
      if (update.kind === "Error") fail(update.data.error);
      else if (update.kind === kind) {
        setState("error", undefined);
        setState("value", reconcile(update.data, { key: "id" }));
      }
    });
  onCleanup(() => subscription.unsubscribe());
  // Only data of `kind` is set.
  return (() => state) as Accessor<Live<DataOf<K>>>;
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
