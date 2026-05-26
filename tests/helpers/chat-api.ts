import type { APIRequestContext } from "@playwright/test";

type ConversationSummary = {
  id: string;
  title: string;
  updated_at: string;
};

export async function deleteConversationsWithPrefix(
  request: APIRequestContext,
  titlePrefix: string
): Promise<void> {
  const response = await request.get("/api/chat/conversations");
  if (!response.ok()) return;

  const conversations = (await response.json()) as ConversationSummary[];
  await Promise.all(
    conversations
      .filter((conversation) => conversation.title.startsWith(titlePrefix))
      .map((conversation) => request.delete(`/api/chat/conversations/${conversation.id}`))
  );
}
