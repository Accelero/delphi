import {
  Outlet,
  createRootRouteWithContext,
  Link,
  redirect,
} from "@tanstack/react-router";
import type { QueryClient } from "@tanstack/react-query";

import { ThemeToggle } from "@/components/theme-toggle";
import { UserMenu } from "@/components/user-menu";
import { ApiError, api, SIGN_IN_URL } from "@/lib/api";
import { useSession } from "@/hooks/useSession";

interface RouterContext {
  queryClient: QueryClient;
}

export const Route = createRootRouteWithContext<RouterContext>()({
  beforeLoad: async ({ context, location }) => {
    // Don't gate API or BFF callback paths.
    if (location.pathname.startsWith("/api/")) return;
    if (location.pathname.startsWith("/oauth2/")) return;
    try {
      await context.queryClient.ensureQueryData({
        queryKey: ["session"],
        queryFn: api.session,
        staleTime: 5 * 60_000,
      });
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) {
        // Hard-navigate so the browser hits oauth2-proxy's OIDC redirect
        // chain instead of the SPA router.
        const rd = encodeURIComponent(location.pathname + location.search);
        window.location.href = `${SIGN_IN_URL}?rd=${rd}`;
        // Halt route resolution; the browser will navigate away.
        throw redirect({ to: "/" });
      }
      throw e;
    }
  },
  component: RootLayout,
});

function RootLayout() {
  const { dev } = useSession();
  return (
    <div className="flex h-full">
      <aside className="w-56 border-r border-[var(--border)] p-4 text-sm flex flex-col">
        <div className="font-semibold mb-4">delphi</div>
        <nav className="flex flex-col space-y-1">
          <Link to="/" className="hover:underline">
            Home
          </Link>
          <Link to="/feed" className="hover:underline">
            Feed
          </Link>
          <Link to="/corpus" className="hover:underline">
            Chat with corpus
          </Link>
        </nav>
        <div className="mt-auto pt-4 space-y-2">
          <UserMenu />
          <ThemeToggle />
        </div>
      </aside>
      <main className="flex-1 flex flex-col overflow-hidden">
        {dev && (
          <div className="bg-yellow-500/15 border-b border-yellow-500/40 px-4 py-1.5 text-xs text-yellow-700 dark:text-yellow-400">
            DEV AUTH MODE — auto-signed in. Real auth is bypassed.
          </div>
        )}
        <div id="app-scroll" className="flex-1 p-6 overflow-auto">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
