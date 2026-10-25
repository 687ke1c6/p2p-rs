import { createSignal } from "solid-js";

/**
 * How this page just got the node into a group, for the dashboard to say
 * once: the group was created, or joined. Cleared when the banner closes.
 */
export type Welcome = "created" | "joined";

const [welcome, setWelcome] = createSignal<Welcome>();

export { setWelcome, welcome };
