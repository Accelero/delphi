import { createRootRoute, createRoute, createRouter, redirect } from "@tanstack/react-router";
import { App } from "./components/chat/App";

const rootRoute = createRootRoute({
  component: App
});

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  beforeLoad: () => {
    throw redirect({ to: "/chat", replace: true });
  }
});

const chatRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/chat"
});

const chatConversationRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/chat/$conversationId"
});

const routeTree = rootRoute.addChildren([indexRoute, chatRoute, chatConversationRoute]);

export const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
