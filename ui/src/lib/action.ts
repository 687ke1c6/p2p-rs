import { type Accessor, createSignal } from "solid-js";

/** An action's state: running, or its last answer or error. */
export type ActionState<T> = { busy: boolean; answer?: T; error?: string };

/** Wraps `run` so a view can show whether it runs, and its answer or error. */
export function action<A extends unknown[], T>(
  run: (...args: A) => Promise<T>,
): [Accessor<ActionState<T>>, (...args: A) => Promise<void>] {
  const [state, setState] = createSignal<ActionState<T>>({ busy: false });
  const start = async (...args: A) => {
    setState({ busy: true });
    try {
      setState({ busy: false, answer: await run(...args) });
    } catch (err) {
      setState({ busy: false, error: err instanceof Error ? err.message : String(err) });
    }
  };
  return [state, start];
}
