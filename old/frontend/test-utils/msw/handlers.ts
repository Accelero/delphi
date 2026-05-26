/**
 * Default MSW handlers — the "happy path" backend that most tests want.
 *
 * Override per-test with `server.use(http.get('/api/auth/me', () => …))`.
 * Anything not in this list AND not overridden in a test will fail with
 * `onUnhandledRequest: 'error'` — that's deliberate, so a missing mock is
 * a loud error rather than a silent network call.
 */

import { http, HttpResponse } from "msw";

import { fixtures } from "../fixtures";

export const handlers = [
  http.get("/healthz", () =>
    HttpResponse.json({ status: "ok" }),
  ),
  http.get("/api/auth/me", () => HttpResponse.json(fixtures.session)),
];
