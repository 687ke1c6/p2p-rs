import { RouterProvider } from "@tanstack/solid-router";
import { render } from "solid-js/web";
import "./index.css";
import { router } from "./router";

render(() => <RouterProvider router={router} />, document.getElementById("root")!);
