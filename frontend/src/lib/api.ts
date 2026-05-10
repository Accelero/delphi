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
 *  `FeedItem` (Document fields plus `read`). `id` is a SurrealDB record
 *  id stringified as `document:<key>`; `docKey()` strips the prefix for
 *  paths that take just the key. */
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
  read: boolean;
};

export type FeedPage = {
  items: FeedDocument[];
  /** Opaque cursor for the next page. `null` ⇒ end of feed. */
  next_cursor: string | null;
};

export type FeedSort = "recency";

/** Strip the `document:` table prefix from a record-id string. The mark-
 *  read endpoints take only the key portion in the path. */
export function docKey(id: string): string {
  return id.startsWith("document:") ? id.slice("document:".length) : id;
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
    markRead: (id: string) =>
      request<void>(`/api/discovery/items/${docKey(id)}/read`, { method: "POST" }),
    markUnread: (id: string) =>
      request<void>(`/api/discovery/items/${docKey(id)}/read`, { method: "DELETE" }),
    /** URL for the SSE `EventSource`. Plain string because EventSource
     *  manages its own fetch, separate from `request()`. */
    eventsUrl: "/api/discovery/feed/events",
  },
};

/** URL the browser hard-navigates to for sign-out. Owned by oauth2-proxy
 *  (the BFF), which clears the session cookie and redirects back to "/".
 *  In dev (Tier 1) this URL doesn't exist — the UI hides the sign-out
 *  control whenever `session.dev === true`. */
export const SIGN_OUT_URL = "/oauth2/sign_out";

/** URL the browser hard-navigates to on a 401. oauth2-proxy starts the
 *  OIDC redirect chain and ?rd= brings the user back here afterwards. */
export const SIGN_IN_URL = "/oauth2/sign_in";
