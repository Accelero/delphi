/**
 * Thin fetch wrapper for the delphi backend.
 *
 * All paths are relative — Vite's dev server proxies /api, /v1, /healthz to
 * the axum backend; in the full prod-shape stack Traefik routes the same
 * paths to the backend directly.
 *
 * Auth model: an upstream BFF (Traefik + oauth2-proxy in front of the
 * backend) owns the session cookie. The browser sends the cookie, the BFF
 * verifies it and projects identity into `X-Auth-*` headers for the
 * backend. `credentials: "same-origin"` keeps the cookie attached.
 */

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

let onUnauthorized: (() => void) | null = null;
/** Wire a 401 handler from main.tsx — typically a hard navigation to
 *  `/oauth2/sign_in`, which kicks off the IdP redirect chain. */
export function setUnauthorizedHandler(fn: () => void): void {
  onUnauthorized = fn;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  if (res.status === 401 && onUnauthorized) onUnauthorized();
  if (!res.ok) {
    throw new ApiError(res.status, `${res.status} ${res.statusText}: ${path}`);
  }
  // 204 No Content → nothing to parse.
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

export type Session = {
  user: { id: string; email: string; name: string | null };
  tenant: { id: string };
  dev: boolean;
};

/** Wire shape returned by `GET /api/discovery/feed`. Mirrors the Rust
 *  `Document`. `id` is a SurrealDB record id stringified as
 *  `document:<key>`. */
export type FeedDocument = {
  id: string;
  /** Optional dedup key — `null` for manual uploads (identity is `id`). */
  canonical_id?: string | null;
  source_type: string;
  source_uri: string;
  storage_uri?: string | null;
  title?: string | null;
  authors: string[];
  published_at?: string | null;
  ingested_at?: string | null;
  language?: string | null;
  summary?: string | null;
  content_hash: string;
  version: number;
  metadata: Record<string, unknown>;
};

export type FeedPage = {
  items: FeedDocument[];
  /** Opaque cursor for the next page. `null` ⇒ end of feed. */
  next_cursor: string | null;
};

export type FeedSort = "recency";

/** Strip the `document:` table prefix from a FeedDocument.id. The
 *  backend's per-document routes are keyed on the record key alone. */
export function documentKey(id: string): string {
  const idx = id.indexOf(":");
  return idx >= 0 ? id.slice(idx + 1) : id;
}

/** `GET /api/documents/:key/view-url` response: a short-lived,
 *  direct-to-storage URL the browser fetches the original bytes from
 *  (PDF.js issues range requests against it directly — the backend is
 *  not in the byte path). See docs/architecture/object-access.md. */
export type ViewUrlResponse = {
  url: string;
  /** RFC3339 instant after which `url` stops working. */
  expires_at: string;
};

/** Wire shape returned by `/api/chat/conversations`. `id` is a
 *  SurrealDB record id stringified as `conversation:<key>`. */
export type Conversation = {
  id: string;
  title: string | null;
  created_at: string | null;
  updated_at: string | null;
};

export type ChatMessageWire = {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  created_at: string | null;
};

/** Wire shape returned by `GET /api/chat/conversations/{id}`. */
export type ConversationWithMessages = {
  conversation: Conversation;
  messages: ChatMessageWire[];
};

/** Strip the `conversation:` table prefix from a Conversation.id. The
 *  backend's per-resource routes are keyed on the record key alone. */
export function conversationKey(id: string): string {
  const idx = id.indexOf(":");
  return idx >= 0 ? id.slice(idx + 1) : id;
}

/** Strip the `chunk:` table prefix from a chunk id (`chunk:<key>`). */
export function chunkKey(id: string): string {
  const idx = id.indexOf(":");
  return idx >= 0 ? id.slice(idx + 1) : id;
}

/** URL for `GET /api/chunks/:id`. */
export function chunkUrl(id: string): string {
  return `/api/chunks/${encodeURIComponent(chunkKey(id))}`;
}

/** Wire shape returned by `GET /api/chunks/:id`. PDF-coordinate
 *  bboxes (origin bottom-left); the viewer flips to CSS at render. */
export type ChunkBbox = { page: number; x: number; y: number; w: number; h: number };
export type ChunkPayload = {
  id: string;
  doc_id: string;
  ordinal: number;
  text: string;
  bboxes?: ChunkBbox[] | null;
};


// ---------------------------------------------------------------------------
// Ingestion v2 — direct-to-S3 multipart upload
// ---------------------------------------------------------------------------

/** Single-file metadata prefill (multi-file uploads send none). All
 *  optional — whatever the user supplies wins; the rest is autofilled
 *  server-side. */
export type UploadPrefill = {
  title?: string;
  summary?: string;
  authors?: string[];
  language?: string;
};

/** `POST /api/ingestion/uploads` request. `source_type` is omitted —
 *  the server defaults it to "manual". `canonical_id` / `source_uri`
 *  are never sent for manual uploads. */
export type CreateUploadRequest = UploadPrefill & {
  filename: string;
  content_type: string;
  size: number;
};

export type CreateUploadResponse = {
  doc_id: string;
  key: string;
  upload_id: string;
  part_size_bytes: number;
  part_url_ttl_secs: number;
};

export type PartRef = { part_number: number; etag: string };

export type SignPartResponse = { url: string };

/** `POST /complete` — synchronous; returns `ready` on success or 422. */
export type CompleteResponse =
  | { result: "ready"; doc_id: string }
  | { result: "conflict"; state: string; existing_doc_id: string | null }
  | { result: "rejected"; reason: string };

/** `GET /api/ingestion/uploads/:id` status. */
export type UploadStatus =
  | { state: "uploading" | "validating" }
  | { state: "ready"; doc_id: string }
  | { state: "rejected"; reason: string };

export const api = {
  health: () => request<{ status: string }>("/healthz"),
  session: () => request<Session>("/api/auth/me"),
  ingestion: {
    createUpload: (req: CreateUploadRequest) =>
      request<CreateUploadResponse>("/api/ingestion/uploads", {
        method: "POST",
        body: JSON.stringify(req),
      }),
    signUploadPart: (docId: string, partNumber: number) =>
      request<SignPartResponse>(
        `/api/ingestion/uploads/${encodeURIComponent(docId)}/sign-part`,
        { method: "POST", body: JSON.stringify({ part_number: partNumber }) },
      ),
    completeUpload: (docId: string, parts: PartRef[]) =>
      request<CompleteResponse>(
        `/api/ingestion/uploads/${encodeURIComponent(docId)}/complete`,
        { method: "POST", body: JSON.stringify({ parts }) },
      ),
    uploadStatus: (docId: string) =>
      request<UploadStatus>(
        `/api/ingestion/uploads/${encodeURIComponent(docId)}`,
      ),
  },
  documents: {
    /** Mint a short-lived direct-to-storage URL for a document's stored
     *  original. The backend runs the tenant/doc authz check, then
     *  returns a presigned URL; the caller fetches the bytes directly. */
    viewUrl: (id: string) =>
      request<ViewUrlResponse>(
        `/api/documents/${encodeURIComponent(documentKey(id))}/view-url`,
      ),
  },
  chunks: {
    get: (id: string) => request<ChunkPayload>(chunkUrl(id)),
  },
  discovery: {
    feed: (params: {
      sort?: FeedSort;
      cursor?: string | null;
      limit?: number;
    }) => {
      const q = new URLSearchParams();
      if (params.sort) q.set("sort", params.sort);
      if (params.cursor) q.set("cursor", params.cursor);
      if (params.limit) q.set("limit", String(params.limit));
      const qs = q.toString();
      return request<FeedPage>(`/api/discovery/feed${qs ? `?${qs}` : ""}`);
    },
    /** URL for the SSE `EventSource`. Plain string because EventSource
     *  manages its own fetch, separate from `request()`. */
    eventsUrl: "/api/discovery/feed/events",
  },
  chat: {
    listConversations: () =>
      request<Conversation[]>("/api/chat/conversations"),
    createConversation: () =>
      request<Conversation>("/api/chat/conversations", {
        method: "POST",
        body: "{}",
      }),
    getConversation: (key: string) =>
      request<ConversationWithMessages>(
        `/api/chat/conversations/${encodeURIComponent(key)}`,
      ),
    renameConversation: (key: string, title: string) =>
      request<void>(`/api/chat/conversations/${encodeURIComponent(key)}`, {
        method: "PATCH",
        body: JSON.stringify({ title }),
      }),
    deleteConversation: (key: string) =>
      request<void>(`/api/chat/conversations/${encodeURIComponent(key)}`, {
        method: "DELETE",
      }),
    /** Submit one turn. Fire-and-forget — the live SSE stream
     *  delivers user_message + text + finish frames.
     *
     *  Body shape: `{ id, text, parent_id }`. `id` is a client-
     *  generated ULID, used verbatim as the message record key.
     *  `parent_id` is the last known assistant message id or `null`
     *  for the first turn.
     *
     *  Returns `{ok: true}` on 202 Accepted; `{ok: false, status}`
     *  on 409 (parent stale OR turn in flight) or any other failure.
     *  Caller inspects `status` to decide between a refetch toast
     *  and a generic error. */
    submitMessage: async (
      key: string,
      body: { id: string; text: string; parent_id: string | null },
    ): Promise<{ ok: true } | { ok: false; status: number }> => {
      const res = await fetch(
        `/api/chat/conversations/${encodeURIComponent(key)}/messages`,
        {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
      );
      if (res.status === 202) return { ok: true };
      return { ok: false, status: res.status };
    },
    /** Cancel the in-flight turn (if any) for a conversation.
     *  Idempotent — server returns 204 whether or not a turn was
     *  running. Fire-and-forget; the SSE `clear` event drives the
     *  UI rollback. **No task id** — v3's `/stop` is scoped to the
     *  conversation. */
    stopChat: (key: string) =>
      request<void>(
        `/api/chat/conversations/${encodeURIComponent(key)}/stop`,
        { method: "POST" },
      ),
  },
};

/** URL the browser hard-navigates to for sign-out. Stable and
 *  IdP-agnostic — Traefik's `signout-chain` middleware rewrites it to
 *  `/oauth2/sign_out?rd=<keycloak end-session URL>`, so oauth2-proxy
 *  clears the BFF session and the browser then hits Keycloak's
 *  RP-initiated logout endpoint to kill the SSO session. Without that
 *  second hop the next /api call silently re-authenticates against
 *  the still-valid IdP session. In dev (Tier 1) there is no BFF; the
 *  UI hides the sign-out control whenever `session.dev === true`. */
export const SIGN_OUT_URL = "/signout";

/** URL the browser hard-navigates to on a 401. oauth2-proxy starts the
 *  OIDC redirect chain and ?rd= brings the user back here afterwards. */
export const SIGN_IN_URL = "/oauth2/sign_in";
