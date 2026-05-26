import type { ConversationDetail, ConversationSummary } from "./types";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: {
      "content-type": "application/json",
      ...init?.headers
    },
    ...init
  });

  if (response.status === 401) {
    window.location.assign("/oauth2/sign_in");
    throw new Error("unauthorized");
  }

  if (!response.ok) {
    const body = await response.json().catch(() => undefined);
    const message = body?.error?.message ?? `request failed with ${response.status}`;
    const error = new Error(message);
    (error as Error & { status?: number; code?: string }).status = response.status;
    (error as Error & { status?: number; code?: string }).code = body?.error?.code;
    throw error;
  }

  if (response.status === 204) {
    return undefined as T;
  }
  return response.json() as Promise<T>;
}

export const api = {
  me: () => request("/api/auth/me"),
  listConversations: () => request<ConversationSummary[]>("/api/chat/conversations"),
  createConversation: (title?: string) =>
    request<ConversationDetail>("/api/chat/conversations", {
      method: "POST",
      body: JSON.stringify({ title })
    }),
  getConversation: (conversationId: string) =>
    request<ConversationDetail>(`/api/chat/conversations/${conversationId}`),
  renameConversation: (conversationId: string, title: string) =>
    request<ConversationDetail>(`/api/chat/conversations/${conversationId}`, {
      method: "PATCH",
      body: JSON.stringify({ title })
    }),
  deleteConversation: (conversationId: string) =>
    request<void>(`/api/chat/conversations/${conversationId}`, { method: "DELETE" }),
  submitTurn: (
    conversationId: string,
    body: {
      user_message_id: string;
      turn_id: string;
      text: string;
      parent_message_id: string | null;
    }
  ) =>
    request<{ turn_id: string }>(`/api/chat/conversations/${conversationId}/turns`, {
      method: "POST",
      body: JSON.stringify(body)
    }),
  stopTurn: (conversationId: string) =>
    request<void>(`/api/chat/conversations/${conversationId}/stop`, { method: "POST" })
};
