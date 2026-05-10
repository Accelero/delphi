/**
 * Feed page integration test.
 *
 * Drives the route's `Feed` component directly (no TanStack Router) with
 * MSW mocking the backend. Covers: empty state, pagination via the
 * "Load more" button, mark-read mutation. SSE arrival is not tested
 * here — `EventSource` requires a heavier mock and is exercised
 * end-to-end in the Tier 1 Playwright run instead.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { http, HttpResponse } from "msw";
import userEvent from "@testing-library/user-event";
import { render as rtlRender, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { server } from "../../../test-utils/msw/server";
import { Feed } from "./Feed";
import type { FeedDocument, FeedPage } from "@/lib/api";

function render(ui: React.ReactElement) {
  const qc = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0, staleTime: 0 },
      mutations: { retry: false },
    },
  });
  return rtlRender(
    <QueryClientProvider client={qc}>{ui}</QueryClientProvider>,
  );
}

// jsdom doesn't ship EventSource. We don't exercise SSE here, but the
// hook still calls `new EventSource(...)` on mount; provide a no-op
// stub so that line doesn't throw.
beforeEach(() => {
  // @ts-expect-error — partial polyfill
  globalThis.EventSource = class {
    constructor() {}
    addEventListener() {}
    removeEventListener() {}
    close() {}
  };
  // jsdom's IntersectionObserver shim is also missing.
  // @ts-expect-error — partial polyfill
  globalThis.IntersectionObserver = class {
    constructor() {}
    observe() {}
    unobserve() {}
    disconnect() {}
  };
});

function fakeDoc(i: number, overrides: Partial<FeedDocument> = {}): FeedDocument {
  return {
    id: `document:doc-${i}`,
    canonical_id: `test:doc-${i}`,
    source_type: "arxiv",
    source_uri: `https://arxiv.org/abs/${i}`,
    authors: [`Author ${i}`],
    title: `Paper ${i}`,
    summary: `Abstract for paper ${i}.`,
    ingested_at: new Date(Date.now() - i * 1000).toISOString(),
    content_hash: `h${i}`,
    version: 1,
    metadata: {},
    read: false,
    ...overrides,
  };
}

describe("Feed", () => {
  it("renders the empty state when no documents", async () => {
    server.use(
      http.get("/api/discovery/feed", () =>
        HttpResponse.json<FeedPage>({ items: [], next_cursor: null }),
      ),
    );
    render(<Feed />);
    await waitFor(() =>
      expect(screen.getByText(/No papers yet/i)).toBeInTheDocument(),
    );
  });

  it("loads more pages via the cursor", async () => {
    const page1: FeedPage = {
      items: [fakeDoc(1), fakeDoc(2)],
      next_cursor: "cursor-to-page-2",
    };
    const page2: FeedPage = {
      items: [fakeDoc(3)],
      next_cursor: null,
    };
    server.use(
      http.get("/api/discovery/feed", ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.get("cursor") === "cursor-to-page-2") {
          return HttpResponse.json(page2);
        }
        return HttpResponse.json(page1);
      }),
    );

    render(<Feed />);
    await waitFor(() => expect(screen.getByText("Paper 1")).toBeInTheDocument());
    expect(screen.getByText("Paper 2")).toBeInTheDocument();
    expect(screen.queryByText("Paper 3")).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /load more/i }));
    await waitFor(() => expect(screen.getByText("Paper 3")).toBeInTheDocument());
    // After last page, the button is gone.
    expect(
      screen.queryByRole("button", { name: /load more/i }),
    ).not.toBeInTheDocument();
  });

  it("clicking a card POSTs the mark-read endpoint and shows the Read chip", async () => {
    const markReadCalls = vi.fn();
    server.use(
      http.get("/api/discovery/feed", () =>
        HttpResponse.json<FeedPage>({
          items: [fakeDoc(1)],
          next_cursor: null,
        }),
      ),
      http.post("/api/discovery/items/:key/read", ({ params }) => {
        markReadCalls(params.key);
        return new HttpResponse(null, { status: 204 });
      }),
    );

    render(<Feed />);
    await waitFor(() => expect(screen.getByText("Paper 1")).toBeInTheDocument());
    expect(screen.queryByText("Read")).not.toBeInTheDocument();

    await userEvent.click(screen.getByText("Abstract for paper 1."));
    await waitFor(() =>
      expect(markReadCalls).toHaveBeenCalledWith("doc-1"),
    );
    // Optimistic update: chip appears immediately.
    expect(screen.getByText("Read")).toBeInTheDocument();
  });
});
