import {
  Link,
  Outlet,
  createRootRoute,
  createRoute,
  createRouter,
  redirect,
} from "@tanstack/solid-router";
import { For } from "solid-js";
import { buttonVariants } from "@/components/ui/button";
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

const rootRoute = createRootRoute({ component: Layout });

// The node's state picks the first view (spec §6) once `/api/status` exists.
const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  beforeLoad: () => {
    throw redirect({ to: "/group" });
  },
});

const viewRoutes = views.map((view) =>
  createRoute({ getParentRoute: () => rootRoute, path: view.path, component: view.component }),
);

export const router = createRouter({
  routeTree: rootRoute.addChildren([indexRoute, ...viewRoutes]),
});

declare module "@tanstack/solid-router" {
  interface Register {
    router: typeof router;
  }
}
