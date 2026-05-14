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
  canonical_id: string;
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

/** URL for `GET /api/documents/:key/file` — the stored original
 *  (PDF) bytes. Plain string because callers feed it to `fetch` /
 *  react-pdf rather than going through the typed `request()`. */
export function documentFileUrl(id: string): string {
  return `/api/documents/${encodeURIComponent(documentKey(id))}/file`;
}

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

export const api = {
  health: () => request<{ status: string }>("/healthz"),
  session: () => request<Session>("/api/auth/me"),
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
    /** URL the `useChat()` hook POSTs to. Plain string because the AI SDK
     *  manages its own fetch. */
    messagesUrl: (key: string) =>
      `/api/chat/conversations/${encodeURIComponent(key)}/messages`,
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
