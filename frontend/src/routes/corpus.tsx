/**
 * `/corpus` — parent route for the corpus-chat surface. The bare URL
 * redirects to the most-recently-updated conversation when one exists;
 * when there are none it falls through to the index route
 * (`corpus.index.tsx`), which renders a **draft** chat. The draft mints a
 * conversation lazily on the first message, so visiting `/corpus` (or
 * deleting the last conversation) never leaves junk empty conversations
 * behind. For nested URLs like `/corpus/$sessionId` this route is a
 * transparent layout: its component is just `<Outlet />`.
 *
 * Per-conversation state lives at `/corpus/$sessionId` — see
 * `corpus.$sessionId.tsx`.
 */
import { Outlet, createFileRoute, redirect } from "@tanstack/react-router";

import { api, conversationKey } from "@/lib/api";
import { conversationsKey } from "@/hooks/useConversations";

export const Route = createFileRoute("/corpus")({
  beforeLoad: async ({ context, location }) => {
    // This route is also the parent of `/corpus/$sessionId`, so its
    // beforeLoad runs on every navigation under `/corpus/*`. Only the
    // bare `/corpus` URL should redirect; otherwise we'd bounce back to
    // ourselves and loop.
    if (location.pathname !== "/corpus" && location.pathname !== "/corpus/") {
      return;
    }
    // `fetchQuery` (not `ensureQueryData`) so a just-deleted conversation
    // can't linger in stale cache and bounce us to a 404 — the redirect
    // decision is made on the fresh list.
    const list = await context.queryClient.fetchQuery({
      queryKey: conversationsKey,
      queryFn: api.chat.listConversations,
    });
    if (list.length > 0) {
      throw redirect({
        to: "/corpus/$sessionId",
        params: { sessionId: conversationKey(list[0].id) },
      });
    }
    // No conversations — fall through to the index route's draft chat.
  },
  component: Outlet,
});
