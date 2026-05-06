/**
 * Unit tests for the fetch wrapper. Lives here (colocated) per the test
 * plan: tests sit next to the unit they test. MSW handles the mock backend
 * via `frontend/test-utils/msw/`.
 */

import { describe, it, expect } from "vitest";
import { http, HttpResponse } from "msw";

import { api, ApiError } from "./api";
import { server } from "../../test-utils/msw/server";
import { fixtures } from "../../test-utils/fixtures";

describe("api.session", () => {
  it("returns the session payload from /api/auth/me", async () => {
    const session = await api.session();
    expect(session.user.email).toBe(fixtures.session.user.email);
    expect(session.dev).toBe(false);
  });

  it("throws ApiError(401) when the backend says unauthenticated", async () => {
    server.use(
      http.get("/api/auth/me", () =>
        new HttpResponse("not authenticated", { status: 401 }),
      ),
    );

    await expect(api.session()).rejects.toThrow(ApiError);
    await expect(api.session()).rejects.toMatchObject({ status: 401 });
  });
});

describe("api.health", () => {
  it("returns the health payload", async () => {
    const h = await api.health();
    expect(h.status).toBe("ok");
  });
});
