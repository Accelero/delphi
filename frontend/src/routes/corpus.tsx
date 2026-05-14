/**
 * `/corpus` — parent route for the corpus-chat surface. The bare URL
 * redirects to the most-recently-updated conversation (or mints one).
 * For nested URLs like `/corpus/$sessionId` this route is a transparent
 * layout: its component is just `<Outlet />` so the child renders.
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
    // bare `/corpus` URL should trigger the redirect; otherwise we'd
    // bounce back to ourselves and loop.
    if (location.pathname !== "/corpus" && location.pathname !== "/corpus/") {
      return;
    }
    const list = await context.queryClient.ensureQueryData({
      queryKey: conversationsKey,
      queryFn: api.chat.listConversations,
    });
    if (list.length > 0) {
      throw redirect({
        to: "/corpus/$sessionId",
        params: { sessionId: conversationKey(list[0].id) },
      });
    }
    // No conversations yet — mint one and redirect. The mutation result
    // also seeds the list cache so the sidebar paints immediately.
    const created = await api.chat.createConversation();
    context.queryClient.setQueryData(conversationsKey, [created]);
    throw redirect({
      to: "/corpus/$sessionId",
      params: { sessionId: conversationKey(created.id) },
    });
  },
  component: Outlet,
});
