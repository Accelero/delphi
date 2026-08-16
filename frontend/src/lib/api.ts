import type {
  CompleteUploadRequest,
  ConversationDetail,
  ConversationSummary,
  CreateUploadResponse,
  DocumentDto,
  DocumentListResponse,
  RenewUploadResponse,
  UploadedPartsResponse,
  UploadStatusResponse
} from "./types";

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
    window.location.assign(`/oauth2/start?rd=${encodeURIComponent(window.location.href)}`);
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
    request<void>(`/api/chat/conversations/${conversationId}/stop`, { method: "POST" }),
  // --- documents -----------------------------------------------------------
  //
  // Preflight. Must be called BEFORE the uploader is constructed: part size is
  // server-owned and Uppy fixes its chunk boundaries at construction time.
  createUpload: (body: {
    filename: string;
    size: number;
    content_type?: string | null;
    /** Omit to create a new document, supply to replace an existing one. */
    document_id?: string | null;
  }) =>
    request<CreateUploadResponse>("/api/uploads", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  // What storage already holds. An uploader resumes by skipping these parts and
  // taking their ETags from here — it never uploaded them, so it has no other
  // source, and /complete needs every ETag.
  listUploadedParts: (uploadId: string) =>
    request<UploadedPartsResponse>(`/api/uploads/${uploadId}/parts`),
  // Signs parts. Called once per part, immediately before that part is
  // uploaded — omit `from_part` instead to ask where an upload should resume.
  renewUploadParts: (
    uploadId: string,
    body: { from_part?: number; count?: number },
    signal?: AbortSignal
  ) =>
    request<RenewUploadResponse>(`/api/uploads/${uploadId}/renew`, {
      method: "POST",
      body: JSON.stringify(body),
      signal
    }),
  completeUpload: (uploadId: string, body: CompleteUploadRequest) =>
    request<{ state: string }>(`/api/uploads/${uploadId}/complete`, {
      method: "POST",
      body: JSON.stringify(body)
    }),
  getUploadStatus: (uploadId: string) =>
    request<UploadStatusResponse>(`/api/uploads/${uploadId}`),
  getDocument: (documentId: string) => request<DocumentDto>(`/api/documents/${documentId}`),
  // `cursor` is the previous page's `next`, passed back verbatim. It is opaque:
  // it encodes the whole ordering key, because `updated_at` alone is not unique
  // and paging on a bare timestamp drops rows that share one.
  listDocuments: (params?: { limit?: number; cursor?: string }) => {
    const query = new URLSearchParams();
    if (params?.limit != null) query.set("limit", String(params.limit));
    if (params?.cursor) query.set("cursor", params.cursor);
    const suffix = query.size > 0 ? `?${query.toString()}` : "";
    return request<DocumentListResponse>(`/api/documents${suffix}`);
  }
};
