import {
  type ErrorComponentProps,
  Link,
  Outlet,
  createRootRoute,
  createRoute,
  createRouter,
  useLocation,
} from "@tanstack/solid-router";
import ArrowDownUp from "lucide-solid/icons/arrow-down-up";
import FolderSync from "lucide-solid/icons/folder-sync";
import LayoutDashboard from "lucide-solid/icons/layout-dashboard";
import PanelLeftClose from "lucide-solid/icons/panel-left-close";
import PanelLeftOpen from "lucide-solid/icons/panel-left-open";
import Rocket from "lucide-solid/icons/rocket";
import { createSignal, For, Show } from "solid-js";
import { Dynamic } from "solid-js/web";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { cx } from "@/lib/cva";
import { live } from "@/lib/socket";
import { Dashboard } from "./views/Dashboard";
import { Folder } from "./views/Folder";
import { Start } from "./views/Start";
import { Transfers } from "./views/Transfers";

const views = [
  { path: "/", label: "Dashboard", icon: LayoutDashboard, component: Dashboard },
  { path: "/transfers", label: "Transfers", icon: ArrowDownUp, component: Transfers },
  // Only while the node is in no group.
  { path: "/start", label: "Get started", icon: Rocket, component: Start, start: true },
] as const;

/** Where the rail's state is kept, per browser. */
const RAIL_KEY = "sync-node.rail-open";

function railOpen(): boolean {
  try {
    return localStorage.getItem(RAIL_KEY) !== "false";
  } catch {
    return true;
  }
}

/** A page's entrance: a fade from a little smaller, with a soft ease-out. */
const enter =
  "motion-safe:animate-in fade-in-0 zoom-in-98 animation-duration-300 ease-[cubic-bezier(0.22,1,0.36,1)]";

function Layout() {
  const [open, setOpen] = createSignal(railOpen());
  const location = useLocation();
  const status = live("status", "Status");
  // "Get started" once the node is known to be in no group.
  const shown = () =>
    views.filter((view) => !("start" in view) || status().value?.group === null);
  const toggle = () => {
    const next = !open();
    setOpen(next);
    try {
      localStorage.setItem(RAIL_KEY, String(next));
    } catch {
      // Not kept: the rail opens again next time.
    }
  };
  return (
    <div class="min-h-screen">
      <header class="bg-background/90 sticky top-0 z-40 flex h-12 items-center gap-2 border-b px-3 backdrop-blur">
        <FolderSync class="text-primary size-5" />
        <span class="font-semibold tracking-tight">sync-node</span>
      </header>
      <div class="flex">
        <nav
          class={cx(
            "bg-sidebar text-sidebar-foreground sticky top-12 flex h-[calc(100vh-3rem)] shrink-0 flex-col gap-0.5 overflow-hidden border-r p-1.5",
            "transition-[width] duration-300 ease-in-out",
            open() ? "w-44" : "w-12",
          )}
        >
          <For each={shown()}>
            {(view) => (
              <Link
                to={view.path}
                activeOptions={{ exact: view.path === "/" }}
                title={open() ? undefined : view.label}
                class={cx(
                  "relative flex h-9 items-center gap-3 rounded-md px-3 text-sm transition-colors",
                  "hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                  // Selected: tinted, in the primary colour, with a bar at its left edge
                  // (seen with the rail collapsed too).
                  "data-[status=active]:bg-primary/10 data-[status=active]:text-primary data-[status=active]:font-medium",
                  "data-[status=active]:hover:bg-primary/15 data-[status=active]:hover:text-primary",
                  "before:absolute before:inset-y-2 before:left-0 before:w-0.5 before:rounded-full before:bg-primary before:opacity-0 before:transition-opacity",
                  "data-[status=active]:before:opacity-100",
                )}
              >
                <Dynamic component={view.icon} class="size-4 shrink-0" />
                <RailLabel open={open()}>{view.label}</RailLabel>
              </Link>
            )}
          </For>
          <button
            type="button"
            onClick={toggle}
            aria-label={open() ? "Collapse the menu" : "Expand the menu"}
            title={open() ? undefined : "Expand"}
            class="hover:bg-sidebar-accent text-muted-foreground mt-auto flex h-9 items-center gap-3 rounded-md px-3 text-sm transition-colors"
          >
            <Show when={open()} fallback={<PanelLeftOpen class="size-4 shrink-0" />}>
              <PanelLeftClose class="size-4 shrink-0" />
            </Show>
            <RailLabel open={open()}>Collapse</RailLabel>
          </button>
        </nav>
        <main class="min-w-0 flex-1 p-3">
          {/* Mounted again for each page, so each one fades in. */}
          <Show when={location().pathname} keyed>
            <div class={enter}>
              <Outlet />
            </div>
          </Show>
        </main>
      </div>
    </div>
  );
}

/** A rail item's label: fades and slides out as the rail closes. */
function RailLabel(props: { open: boolean; children: string }) {
  return (
    <span
      class={cx(
        "whitespace-nowrap transition-[opacity,translate] duration-300 ease-in-out",
        props.open ? "translate-x-0 opacity-100" : "pointer-events-none -translate-x-2 opacity-0",
      )}
    >
      {props.children}
    </span>
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
  routeTree: rootRoute.addChildren([...viewRoutes, folderRoute]),
});

declare module "@tanstack/solid-router" {
  interface Register {
    router: typeof router;
  }
}
