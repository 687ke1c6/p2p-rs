import { Link } from "@tanstack/solid-router";
import { createSignal, type JSX, Show } from "solid-js";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button, buttonVariants } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  TextField,
  TextFieldDescription,
  TextFieldInput,
  TextFieldLabel,
  TextFieldTextArea,
} from "@/components/ui/text-field";
import { type ActionState, action } from "@/lib/action";
import { addFolder, addMember, createGroup, createInvite, join } from "@/lib/api";
import { live } from "@/lib/socket";
import { LiveError } from "./LiveError";

/** The setup the CLI does: create or join a group; then invites, members and folders. */
export function Setup() {
  const status = live("status", "Status");
  return (
    <div class="max-w-2xl space-y-4">
      <LiveError error={status().error} />
      <Show when={status().value}>
        {(answer) => (
          <Show when={answer().group} fallback={<NoGroup node={answer().node} />}>
            <InGroup node={answer().node} />
          </Show>
        )}
      </Show>
    </div>
  );
}

function NoGroup(props: { node: string }) {
  const [created, create] = action(createGroup);
  const [joined, startJoin] = action(join);
  const [ticket, setTicket] = createSignal("");
  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>Create a group</CardTitle>
          <CardDescription>This node founds a new group and is its only member.</CardDescription>
        </CardHeader>
        <CardContent class="space-y-4">
          <Button disabled={created().busy} onClick={() => void create()}>
            {created().busy ? "Creating…" : "Create group"}
          </Button>
          <Failed state={created()} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Join a group</CardTitle>
          <CardDescription>
            With an invite ticket, or with the ID of a member that added this node. That member must
            be running.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Form onSubmit={() => void startJoin({ ticket: ticket() })}>
            <TextField value={ticket()} onChange={setTicket}>
              <TextFieldLabel>Invite ticket or member ID</TextFieldLabel>
              <TextFieldInput class="font-mono" required />
              <TextFieldDescription>
                To be added by a member, give it this node's ID:{" "}
                <span class="font-mono break-all">{props.node}</span>
              </TextFieldDescription>
            </TextField>
            <Button type="submit" class="w-fit" disabled={joined().busy}>
              {joined().busy ? "Joining… (up to 60 s)" : "Join"}
            </Button>
            <Failed state={joined()} />
          </Form>
        </CardContent>
      </Card>
    </>
  );
}

function InGroup(props: { node: string }) {
  const [invite, startInvite] = action(createInvite);
  const [member, startMember] = action(addMember);
  const [folder, startFolder] = action(addFolder);
  const [hours, setHours] = createSignal("24");
  const [inviteName, setInviteName] = createSignal("");
  const [memberId, setMemberId] = createSignal("");
  const [memberName, setMemberName] = createSignal("");
  const [folderName, setFolderName] = createSignal("");
  const [folderPath, setFolderPath] = createSignal("");
  const [copied, setCopied] = createSignal(false);
  const optional = (text: string) => (text.trim() === "" ? undefined : text.trim());
  const copy = async (ticket: string) => {
    await navigator.clipboard.writeText(ticket);
    setCopied(true);
  };
  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>Invite a node</CardTitle>
          <CardDescription>
            A one-time ticket. The new node joins with it while this node runs.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Form
            onSubmit={() => {
              setCopied(false);
              void startInvite({ hours: Number(hours()), name: optional(inviteName()) });
            }}
          >
            <div class="grid gap-4 sm:grid-cols-2">
              <TextField value={hours()} onChange={setHours}>
                <TextFieldLabel>Valid for (hours)</TextFieldLabel>
                <TextFieldInput type="number" min="1" required />
              </TextField>
              <TextField value={inviteName()} onChange={setInviteName}>
                <TextFieldLabel>Name of the new member</TextFieldLabel>
                <TextFieldInput placeholder="optional" />
              </TextField>
            </div>
            <Button type="submit" class="w-fit" disabled={invite().busy}>
              Create invite
            </Button>
            <Failed state={invite()} />
            <Show when={invite().answer}>
              {(answer) => (
                <div class="space-y-2">
                  <TextField value={answer().ticket} readOnly>
                    <TextFieldLabel>Ticket</TextFieldLabel>
                    <TextFieldTextArea class="font-mono text-xs" rows={4} />
                  </TextField>
                  <Button variant="outline" onClick={() => void copy(answer().ticket)}>
                    {copied() ? "Copied" : "Copy ticket"}
                  </Button>
                </div>
              )}
            </Show>
          </Form>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Add a member</CardTitle>
          <CardDescription>
            Adds a node by its ID (<span class="font-mono">sync-node id</span> prints it), or
            renames a member.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Form
            onSubmit={() => void startMember({ id: memberId(), name: optional(memberName()) })}
          >
            <TextField value={memberId()} onChange={setMemberId}>
              <TextFieldLabel>Node ID</TextFieldLabel>
              <TextFieldInput class="font-mono" required />
            </TextField>
            <TextField value={memberName()} onChange={setMemberName}>
              <TextFieldLabel>Name</TextFieldLabel>
              <TextFieldInput placeholder="optional" />
            </TextField>
            <Button type="submit" class="w-fit" disabled={member().busy}>
              Add member
            </Button>
            <Failed state={member()} />
            <Show when={member().answer}>
              {(answer) => (
                <Queued within={answer().within_secs}>
                  The new node then joins with this node's ID:{" "}
                  <span class="font-mono break-all">{props.node}</span>
                </Queued>
              )}
            </Show>
          </Form>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Add a folder</CardTitle>
          <CardDescription>
            Keeps a folder on this node: joins the group's folder of that name, or creates it.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Form
            onSubmit={() => void startFolder({ name: folderName(), path: folderPath() })}
          >
            <TextField value={folderName()} onChange={setFolderName}>
              <TextFieldLabel>Folder name</TextFieldLabel>
              <TextFieldInput required />
              <TextFieldDescription>The same on every node.</TextFieldDescription>
            </TextField>
            <TextField value={folderPath()} onChange={setFolderPath}>
              <TextFieldLabel>Directory on this node</TextFieldLabel>
              <TextFieldInput class="font-mono" placeholder="/home/…" required />
            </TextField>
            <Button type="submit" class="w-fit" disabled={folder().busy}>
              Add folder
            </Button>
            <Failed state={folder()} />
            <Show when={folder().answer}>
              {(answer) => (
                <Queued within={answer().within_secs}>
                  See it under{" "}
                  <Link to="/folders" class={buttonVariants({ variant: "link", class: "h-auto p-0" })}>
                    Folders
                  </Link>
                  .
                </Queued>
              )}
            </Show>
          </Form>
        </CardContent>
      </Card>
    </>
  );
}

/** A form that runs `onSubmit` instead of sending itself. */
function Form(props: { onSubmit: () => void; children: JSX.Element }) {
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

function Failed(props: { state: ActionState<unknown> }) {
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

function Queued(props: { within: number; children: JSX.Element }) {
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
