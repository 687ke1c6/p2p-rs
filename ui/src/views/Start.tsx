import { Navigate, useNavigate } from "@tanstack/solid-router";
import { createSignal, Show } from "solid-js";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  TextField,
  TextFieldDescription,
  TextFieldInput,
  TextFieldLabel,
} from "@/components/ui/text-field";
import { action } from "@/lib/action";
import { cx } from "@/lib/cva";
import { createGroup, join } from "@/lib/api";
import { live } from "@/lib/socket";
import { setWelcome } from "@/lib/welcome";
import { Failed, Form } from "./Forms";
import { LiveError } from "./LiveError";

/**
 * Getting into a group, as `create` and `join` do. Only for a node in no
 * group: once in one, the dashboard is the place, and this view goes there.
 */
export function Start() {
  const status = live("status", "Status");
  return (
    <div class="mx-auto max-w-lg space-y-3 pt-4">
      <LiveError error={status().error} />
      <Show when={status().value}>
        {(answer) => (
          <Show when={!answer().group} fallback={<Navigate to="/" />}>
            <GetStarted node={answer().node} />
          </Show>
        )}
      </Show>
    </div>
  );
}

/**
 * One step of the page's entrance: it rises a little and fades in, `delay`
 * ms after the page, so the parts arrive one after another.
 */
const rise = (delay: 0 | 90 | 180 | 270) =>
  cx(
    "motion-safe:animate-in fade-in-0 slide-in-from-bottom-4 animation-duration-500 fill-mode-both",
    "ease-[cubic-bezier(0.22,1,0.36,1)]",
    { 0: "delay-0", 90: "delay-90", 180: "delay-180", 270: "delay-270" }[delay],
  );

function GetStarted(props: { node: string }) {
  const navigate = useNavigate();
  const [created, startCreate] = action(createGroup);
  const [joined, startJoin] = action(join);
  const [name, setName] = createSignal("");
  const [ticket, setTicket] = createSignal("");
  const create = async () => {
    const trimmed = name().trim();
    await startCreate({ name: trimmed === "" ? undefined : trimmed });
    if (created().answer) {
      setWelcome("created");
      void navigate({ to: "/" });
    }
  };
  const enter = async () => {
    await startJoin({ ticket: ticket() });
    if (joined().answer) {
      setWelcome("joined");
      void navigate({ to: "/" });
    }
  };
  return (
    <>
      <div class={cx("space-y-1", rise(0))}>
        <h1 class="text-lg font-semibold tracking-tight">Get started</h1>
        <p class="text-muted-foreground text-sm">
          This node is in no group yet. Create one, or join one that another node is in.
        </p>
      </div>

      <Card class={cx("gap-3 py-4", rise(90))}>
        <CardHeader class="px-4">
          <CardTitle>Create a group</CardTitle>
          <CardDescription>This node founds a new group and is its only member.</CardDescription>
        </CardHeader>
        <CardContent class="px-4">
          <Form onSubmit={() => void create()}>
            <TextField value={name()} onChange={setName}>
              <TextFieldLabel>Name</TextFieldLabel>
              <TextFieldInput placeholder="e.g. home (optional)" />
              <TextFieldDescription>Shown on every node. You can change it later.</TextFieldDescription>
            </TextField>
            <Button type="submit" class="w-fit" disabled={created().busy}>
              {created().busy ? "Creating…" : "Create group"}
            </Button>
            <Failed state={created()} />
          </Form>
        </CardContent>
      </Card>

      <div class={cx("text-muted-foreground flex items-center gap-3 text-xs", rise(180))}>
        <div class="h-px flex-1 bg-border" />
        or
        <div class="h-px flex-1 bg-border" />
      </div>

      <Card class={cx("gap-3 py-4", rise(270))}>
        <CardHeader class="px-4">
          <CardTitle>Join a group</CardTitle>
          <CardDescription>
            With an invite ticket, or with the ID of a member that added this node. That member must
            be running.
          </CardDescription>
        </CardHeader>
        <CardContent class="px-4">
          <Form onSubmit={() => void enter()}>
            <TextField value={ticket()} onChange={setTicket}>
              <TextFieldLabel>Invite ticket or member ID</TextFieldLabel>
              <TextFieldInput class="font-mono" required />
              <TextFieldDescription>
                To be added by a member, give it this node's ID:{" "}
                <span class="font-mono break-all select-all">{props.node}</span>
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
