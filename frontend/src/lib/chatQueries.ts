export const conversationListQueryKey = ["chat", "conversations"] as const;

export function conversationQueryKey(conversationId: string) {
  return ["chat", "conversation", conversationId] as const;
}
