import X from "lucide-solid/icons/x";
import type { JSX } from "solid-js";
import { Show } from "solid-js";
import { Button } from "@/components/ui/button";
import { cx } from "@/lib/cva";

/** A banner's colour: news of something done, or of something on its way. */
export type Tone = "success" | "info";

const tones: Record<Tone, string> = {
  success:
    "border-emerald-300 bg-emerald-50 text-emerald-950 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-50",
  info: "border-sky-300 bg-sky-50 text-sky-950 dark:border-sky-800 dark:bg-sky-950 dark:text-sky-50",
};

/**
 * A notice over the page's content, not a part of it: one coloured strip
 * with an icon, a line of text, actions beside it, and a close button.
 */
export function Banner(props: {
  tone: Tone;
  icon: JSX.Element;
  title: string;
  children?: JSX.Element;
  actions?: JSX.Element;
  onClose: () => void;
}) {
  return (
    <div
      role="status"
      class={cx(
        "flex flex-wrap items-center gap-x-3 gap-y-2 rounded-lg border py-2 pr-1.5 pl-3 shadow-sm",
        "animate-in fade-in-0 slide-in-from-top-1 duration-200",
        tones[props.tone],
      )}
    >
      <span class="shrink-0 [&_svg]:size-4">{props.icon}</span>
      <p class="min-w-0 flex-1 text-sm">
        <span class="font-medium">{props.title}</span>
        <Show when={props.children}>
          <span class="opacity-80"> {props.children}</span>
        </Show>
      </p>
      <Show when={props.actions}>
        <div class="flex flex-wrap gap-2">{props.actions}</div>
      </Show>
      <Button
        size="icon-sm"
        variant="ghost"
        class="shrink-0 hover:bg-black/5 dark:hover:bg-white/10"
        aria-label="Close"
        onClick={() => props.onClose()}
      >
        <X class="size-4" />
      </Button>
    </div>
  );
}
