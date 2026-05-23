/**
 * `/corpus` (index) — the **draft** chat, shown when the user has no
 * conversations (first visit, or after deleting the last one). It renders
 * the same chat surface with no session attached; the first message mints
 * a conversation, sends the turn, and navigates to `/corpus/$sessionId`.
 *
 * Minting lazily (on first send) — rather than eagerly on navigation —
 * means an empty chat window never leaves a junk "Untitled" conversation
 * behind. After navigation the session view late-joins the in-flight turn
 * via the bus, so the streamed reply appears without special handoff.
 */
import { useQueryClient } from "@tanstack/react-query";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { ulid } from "ulid";

import { Chat } from "@/components/chat";
import { ConversationSidebar } from "@/components/chat/ConversationSidebar";
import { conversationKeyFor, conversationsKey } from "@/hooks/useConversations";
import { api, conversationKey, type Conversation } from "@/lib/api";

export const Route = createFileRoute("/corpus/")({
  component: DraftCorpus,
});

function DraftCorpus() {
  const navigate = useNavigate();
  const qc = useQueryClient();

  const handleDraftSubmit = async (text: string) => {
    const created = await api.chat.createConversation();
    const key = conversationKey(created.id);
    // Seed caches so the sidebar paints immediately and the session
    // route's loader resolves without a round-trip.
    qc.setQueryData<Conversation[]>(conversationsKey, (old) => [
      created,
      ...(old ?? []),
    ]);
    qc.setQueryData(conversationKeyFor(key), {
      conversation: created,
      messages: [],
    });
    // Fire the first turn, then navigate. The session view late-joins the
    // in-flight turn over SSE (v4 replay), so the streamed reply shows up.
    await api.chat.submitMessage(key, { id: ulid(), text, parent_id: null });
    navigate({ to: "/corpus/$sessionId", params: { sessionId: key } });
  };

  return (
    <div className="flex h-full">
      <ConversationSidebar activeKey="" />
      <div className="flex-1 flex flex-col min-w-0">
        <div className="flex-1 min-h-0 flex flex-col max-w-3xl mx-auto w-full p-4">
          <Chat
            onDraftSubmit={handleDraftSubmit}
            emptyTitle="Chat with corpus"
            emptyDescription="Ask anything. v1 is plain LLM passthrough — corpus retrieval comes next."
          />
        </div>
      </div>
    </div>
  );
}
