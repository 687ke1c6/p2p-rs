import { type JSX, Show } from "solid-js";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import type { ActionState } from "@/lib/action";

/** A form that runs `onSubmit` instead of sending itself. */
export function Form(props: { onSubmit: () => void; children: JSX.Element }) {
  return (
    <form
      class="grid gap-4"
      onSubmit={(event) => {
        event.preventDefault();
        props.onSubmit();
      }}
    >
      {props.children}
    </form>
  );
}

export function Failed(props: { state: ActionState<unknown> }) {
  return (
    <Show when={props.state.error}>
      {(error) => (
        <Alert variant="destructive">
          <AlertTitle>Not done</AlertTitle>
          <AlertDescription>{error()}</AlertDescription>
        </Alert>
      )}
    </Show>
  );
}

export function Queued(props: { within: number; children?: JSX.Element }) {
  return (
    <Alert>
      <AlertTitle>Queued</AlertTitle>
      <AlertDescription>
        <p>
          The node applies it within {props.within} s. {props.children}
        </p>
      </AlertDescription>
    </Alert>
  );
}
