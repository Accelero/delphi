/**
 * `/corpus/$sessionId` — one persisted chat conversation.
 *
 * Layout: sidebar on the left listing every conversation, chat surface
 * on the right. Both share TanStack Query's `["conversations"]` cache;
 * the chat surface invalidates it after each turn so the auto-generated
 * title and freshly-persisted messages flow back into the sidebar.
 */
import { createFileRoute, useParams } from "@tanstack/react-router";

import { Chat } from "@/components/chat";
import { ConversationSidebar } from "@/components/chat/ConversationSidebar";
import {
  conversationKeyFor,
  useConversation,
} from "@/hooks/useConversations";
import { api } from "@/lib/api";

export const Route = createFileRoute("/corpus/$sessionId")({
  loader: async ({ context, params }) => {
    try {
      await context.queryClient.ensureQueryData({
        queryKey: conversationKeyFor(params.sessionId),
        queryFn: () => api.chat.getConversation(params.sessionId),
      });
    } catch {
      // 404 etc. — let the component render a friendly fallback rather
      // than throwing past the route boundary.
    }
    return null;
  },
  component: CorpusConversation,
});

function CorpusConversation() {
  const { sessionId } = useParams({ from: "/corpus/$sessionId" });
  const q = useConversation(sessionId);

  return (
    <div className="flex h-full">
      <ConversationSidebar activeKey={sessionId} />
      <div className="flex-1 flex flex-col min-w-0">
        {q.isError && (
          <div className="p-6 text-sm text-destructive">
            Conversation not found.
          </div>
        )}
        {q.isSuccess && (
          <div className="flex-1 min-h-0 flex flex-col max-w-3xl mx-auto w-full p-4">
            <Chat
              api={api.chat.messagesUrl(sessionId)}
              sessionKey={sessionId}
              initialMessages={q.data.messages.map((m) => ({
                id: m.id,
                role: m.role,
                content: m.content,
              }))}
              emptyTitle="Chat with corpus"
              emptyDescription="Ask anything. v1 is plain LLM passthrough — corpus retrieval comes next."
            />
          </div>
        )}
      </div>
    </div>
  );
}
