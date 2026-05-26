import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider, createRouter } from "@tanstack/react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { ThemeProvider } from "./components/theme-provider";
import { TooltipProvider } from "./components/ui/tooltip";
import { routeTree } from "./routeTree.gen";
import { setUnauthorizedHandler, SIGN_IN_URL } from "./lib/api";
import "./styles/globals.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 30_000, refetchOnWindowFocus: false },
  },
});

const router = createRouter({
  routeTree,
  context: { queryClient },
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

// On 401 from any /api call, hard-navigate to oauth2-proxy's sign-in
// endpoint, which kicks off the OIDC redirect chain. In Tier 1 (dev) the
// dev injector means /api calls never 401, so this branch is dead — but
// pointing at a Tier 2 URL keeps the contract consistent across stacks.
setUnauthorizedHandler(() => {
  const rd = encodeURIComponent(window.location.pathname + window.location.search);
  window.location.href = `${SIGN_IN_URL}?rd=${rd}`;
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ThemeProvider>
      <TooltipProvider>
        <QueryClientProvider client={queryClient}>
          <RouterProvider router={router} />
        </QueryClientProvider>
      </TooltipProvider>
    </ThemeProvider>
  </StrictMode>,
);
