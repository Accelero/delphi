/**
 * `/corpus` — index route for the corpus-chat surface. Never renders;
 * redirects to the most-recently-updated conversation, or creates one
 * if the user has none yet.
 *
 * Per-conversation state lives at `/corpus/$sessionId` — see
 * `corpus.$sessionId.tsx`.
 */
import { createFileRoute, redirect } from "@tanstack/react-router";

import { api, conversationKey } from "@/lib/api";
import { conversationsKey } from "@/hooks/useConversations";

export const Route = createFileRoute("/corpus")({
  beforeLoad: async ({ context }) => {
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
  // Component is unreachable — beforeLoad always redirects — but TanStack
  // Router requires one.
  component: () => null,
});
