import { Show } from "solid-js";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";

/** Why a UI topic isn't live, if it isn't. */
export function LiveError(props: { error?: string }) {
  return (
    <Show when={props.error}>
      {(error) => (
        <Alert variant="destructive">
          <AlertTitle>Not up to date</AlertTitle>
          <AlertDescription>{error()}</AlertDescription>
        </Alert>
      )}
    </Show>
  );
}
