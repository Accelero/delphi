/**
 * Thin fetch wrapper for the delphi backend.
 *
 * All paths are relative — Vite's dev server proxies /api, /v1, /healthz to
 * the axum backend; in production the same paths are served by axum directly.
 *
 * Auth is cookie-based (BFF: backend owns OIDC tokens, browser only sees an
 * HTTP-only signed session cookie). `credentials: "same-origin"` is set on
 * every request so the cookie rides along.
 */

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

let onUnauthorized: (() => void) | null = null;
/** Wire a 401 handler from main.tsx — typically a hard navigation to
 *  `/api/auth/login`, which kicks off the IdP redirect chain. */
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

export const api = {
  health: () => request<{ status: string }>("/healthz"),
  session: () => request<Session>("/api/auth/me"),
  logout: () => request<void>("/api/auth/logout", { method: "POST" }),
};
