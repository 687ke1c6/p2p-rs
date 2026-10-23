import {
  type ErrorComponentProps,
  Link,
  Outlet,
  createRootRoute,
  createRoute,
  createRouter,
  redirect,
} from "@tanstack/solid-router";
import { For, Show } from "solid-js";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { buttonVariants } from "@/components/ui/button";
import { once } from "@/lib/socket";
import { Folder } from "./views/Folder";
import { Folders } from "./views/Folders";
import { Group } from "./views/Group";
import { Setup } from "./views/Setup";
import { Transfers } from "./views/Transfers";

const views = [
  { path: "/group", label: "Group", component: Group },
  { path: "/folders", label: "Folders", component: Folders },
  { path: "/transfers", label: "Transfers", component: Transfers },
  { path: "/setup", label: "Setup", component: Setup },
] as const;

function Layout() {
  return (
    <div class="min-h-screen">
      <nav class="flex gap-1 border-b px-4 py-2">
        <For each={views}>
          {(view) => (
            <Link
              to={view.path}
              class={buttonVariants({
                variant: "ghost",
                class: "data-[status=active]:bg-accent data-[status=active]:text-accent-foreground",
              })}
            >
              {view.label}
            </Link>
          )}
        </For>
      </nav>
      <main class="p-4">
        <Outlet />
      </main>
    </div>
  );
}

function LoadError(props: ErrorComponentProps) {
  return (
    <Alert variant="destructive">
      <AlertTitle>Can't reach the node</AlertTitle>
      <AlertDescription>{props.error.message}</AlertDescription>
    </Alert>
  );
}

const rootRoute = createRootRoute({ component: Layout, errorComponent: LoadError });

// The node's state picks the first view (spec §6).
const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  beforeLoad: async () => {
    const status = await once("status", "Status");
    throw redirect({ to: status.group ? "/group" : "/setup" });
  },
});

const viewRoutes = views.map((view) =>
  createRoute({ getParentRoute: () => rootRoute, path: view.path, component: view.component }),
);

const folderRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/folders/$name",
  // Keyed, so another folder starts a new view and its own UI topic.
  component: () => {
    const params = folderRoute.useParams();
    return (
      <Show when={params().name} keyed>
        {(name) => <Folder name={name} />}
      </Show>
    );
  },
});

export const router = createRouter({
  routeTree: rootRoute.addChildren([indexRoute, ...viewRoutes, folderRoute]),
});

declare module "@tanstack/solid-router" {
  interface Register {
    router: typeof router;
  }
}
