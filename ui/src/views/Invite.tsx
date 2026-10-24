import { createSignal, Show } from "solid-js";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPortal,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  TextField,
  TextFieldDescription,
  TextFieldInput,
  TextFieldLabel,
  TextFieldTextArea,
} from "@/components/ui/text-field";
import { action } from "@/lib/action";
import { addMember, createInvite } from "@/lib/api";
import { Failed, Form, Queued } from "./Forms";

const optional = (text: string) => (text.trim() === "" ? undefined : text.trim());

/** Makes a one-time invite, as `invite create`, and shows its ticket to copy. */
export function InviteDialog(props: { open: boolean; onClose: () => void }) {
  return (
    <Dialog open={props.open} onOpenChange={(open) => !open && props.onClose()}>
      <DialogPortal>
        <DialogContent>
          <InviteForm onClose={() => props.onClose()} />
        </DialogContent>
      </DialogPortal>
    </Dialog>
  );
}

/** Mounted with the dialog, so each opening starts afresh. */
function InviteForm(props: { onClose: () => void }) {
  const [invite, start] = action(createInvite);
  const [hours, setHours] = createSignal("24");
  const [name, setName] = createSignal("");
  const [copied, setCopied] = createSignal(false);
  const copy = async (ticket: string) => {
    await navigator.clipboard.writeText(ticket);
    setCopied(true);
  };
  return (
    <Show
      when={invite().answer}
      fallback={
        <Form onSubmit={() => void start({ hours: Number(hours()), name: optional(name()) })}>
          <DialogHeader>
            <DialogTitle>Invite a node</DialogTitle>
            <DialogDescription>
              A one-time ticket. The new node joins with it while this node runs.
            </DialogDescription>
          </DialogHeader>
          <div class="grid gap-4 sm:grid-cols-2">
            <TextField value={hours()} onChange={setHours}>
              <TextFieldLabel>Valid for (hours)</TextFieldLabel>
              <TextFieldInput type="number" min="1" required />
            </TextField>
            <TextField value={name()} onChange={setName}>
              <TextFieldLabel>Name of the new member</TextFieldLabel>
              <TextFieldInput placeholder="optional" />
            </TextField>
          </div>
          <Failed state={invite()} />
          <DialogFooter>
            <Button variant="outline" type="button" onClick={() => props.onClose()}>
              Cancel
            </Button>
            <Button type="submit" disabled={invite().busy}>
              Create invite
            </Button>
          </DialogFooter>
        </Form>
      }
    >
      {(answer) => (
        <div class="grid gap-4">
          <DialogHeader>
            <DialogTitle>Invite ready</DialogTitle>
            <DialogDescription>
              On the new node, join with this ticket: on its Get started page, or with{" "}
              <span class="font-mono">sync-node join &lt;ticket&gt;</span>. It works once.
            </DialogDescription>
          </DialogHeader>
          <TextField value={answer().ticket} readOnly>
            <TextFieldLabel>Ticket</TextFieldLabel>
            <TextFieldTextArea class="font-mono text-xs" rows={4} />
          </TextField>
          <DialogFooter>
            <Button variant="outline" onClick={() => void copy(answer().ticket)}>
              {copied() ? "Copied" : "Copy ticket"}
            </Button>
            <Button onClick={() => props.onClose()}>Done</Button>
          </DialogFooter>
        </div>
      )}
    </Show>
  );
}

/** Adds a node by its ID, as `member add`; the node then joins with this node's ID. */
export function AddMemberDialog(props: { open: boolean; node: string; onClose: () => void }) {
  return (
    <Dialog open={props.open} onOpenChange={(open) => !open && props.onClose()}>
      <DialogPortal>
        <DialogContent>
          <AddMemberForm node={props.node} onClose={() => props.onClose()} />
        </DialogContent>
      </DialogPortal>
    </Dialog>
  );
}

function AddMemberForm(props: { node: string; onClose: () => void }) {
  const [added, start] = action(addMember);
  const [id, setId] = createSignal("");
  const [name, setName] = createSignal("");
  return (
    <Show
      when={added().answer}
      fallback={
        <Form onSubmit={() => void start({ id: id(), name: optional(name()) })}>
          <DialogHeader>
            <DialogTitle>Add a node by ID</DialogTitle>
            <DialogDescription>
              For a node that can't take a ticket. <span class="font-mono">sync-node id</span>{" "}
              prints its ID.
            </DialogDescription>
          </DialogHeader>
          <TextField value={id()} onChange={setId}>
            <TextFieldLabel>Node ID</TextFieldLabel>
            <TextFieldInput class="font-mono" required />
          </TextField>
          <TextField value={name()} onChange={setName}>
            <TextFieldLabel>Name</TextFieldLabel>
            <TextFieldInput placeholder="optional" />
            <TextFieldDescription>Shown instead of its ID, on every node.</TextFieldDescription>
          </TextField>
          <Failed state={added()} />
          <DialogFooter>
            <Button variant="outline" type="button" onClick={() => props.onClose()}>
              Cancel
            </Button>
            <Button type="submit" disabled={added().busy}>
              Add node
            </Button>
          </DialogFooter>
        </Form>
      }
    >
      {(answer) => (
        <div class="grid gap-4">
          <DialogHeader>
            <DialogTitle>Node added</DialogTitle>
          </DialogHeader>
          <Queued within={answer().within_secs}>
            The new node then joins with this node's ID, on its Get started page or with{" "}
            <span class="font-mono break-all select-all">sync-node join {props.node}</span>
          </Queued>
          <DialogFooter>
            <Button onClick={() => props.onClose()}>Done</Button>
          </DialogFooter>
        </div>
      )}
    </Show>
  );
}
