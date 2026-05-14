/**
 * Discovery feed.
 *
 * Reverse-chronological infinite scroll over the user's corpus, with:
 *  - cursor pagination ("Load more" at the bottom)
 *  - SSE live-updates that prepend new arrivals to the top
 *  - per-session "new" highlight (chip + glow) that clears the first
 *    time the card sees a mouseover — pure cosmetic, not persisted
 *  - "N new above" floating pill when the user has scrolled away from
 *    the top
 *
 * Scroll-position preservation when prepending is delegated to the
 * browser's `overflow-anchor: auto` (default in modern browsers); no JS
 * scroll bookkeeping required.
 *
 * Lives outside `routes/` so the route file can stay tiny — TanStack
 * Router code-splits one chunk per route file, and any non-`Route`
 * exports there break the split.
 */
import { useEffect, useMemo, useState } from "react";
import {
  useInfiniteQuery,
  useQueryClient,
  type InfiniteData,
} from "@tanstack/react-query";
import { ArrowUp } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Spinner } from "@/components/ui/spinner";
import { DocumentCard } from "@/components/discovery/DocumentCard";
import { api, type FeedDocument, type FeedPage, type FeedSort } from "@/lib/api";
import { useFeedEvents } from "@/hooks/useFeedEvents";

const PAGE_LIMIT = 50;
const APP_SCROLL_ID = "app-scroll";

function feedQueryKey(sort: FeedSort) {
  return ["discovery-feed", { sort }] as const;
}

export function Feed() {
  const [sort, setSort] = useState<FeedSort>("recency");
  const queryClient = useQueryClient();
  const queryKey = feedQueryKey(sort);

  const query = useInfiniteQuery<FeedPage, Error>({
    queryKey,
    queryFn: ({ pageParam }) =>
      api.discovery.feed({
        sort,
        cursor: (pageParam as string | null) ?? null,
        limit: PAGE_LIMIT,
      }),
    initialPageParam: null,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  });

  const items = useMemo(
    () => query.data?.pages.flatMap((p) => p.items) ?? [],
    [query.data],
  );

  // Per-session "newly arrived via SSE" set. Cleared on first hover —
  // pure cosmetic; nothing about this is persisted.
  const [newSet, setNewSet] = useState<Set<string>>(new Set());
  const clearNew = (id: string) => {
    setNewSet((prev) => {
      if (!prev.has(id)) return prev;
      const next = new Set(prev);
      next.delete(id);
      return next;
    });
  };

  // SSE arrival → prepend the document directly into the cache. The
  // event payload IS a FeedDocument (same wire shape /api/discovery/feed
  // returns), so no refetch is needed. Dedup by id covers the rare race
  // where the initial page-fetch already includes the doc.
  useFeedEvents((item) => {
    setNewSet((prev) => new Set(prev).add(item.id));
    queryClient.setQueryData<InfiniteData<FeedPage>>(queryKey, (old) =>
      prependItem(old, item),
    );
  });

  const { showPill, scrollToTop } = usePillState(newSet.size);

  return (
    <div className="space-y-4 max-w-3xl mx-auto">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">Feed</h1>
        <Select value={sort} onValueChange={(v) => setSort(v as FeedSort)}>
          <SelectTrigger size="sm" aria-label="Sort">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="recency">Recency</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {showPill && (
        <button
          type="button"
          onClick={scrollToTop}
          className="fixed left-1/2 -translate-x-1/2 top-20 z-20 inline-flex items-center gap-2 rounded-full bg-primary text-primary-foreground px-4 py-1.5 text-sm shadow-lg hover:bg-primary/90"
        >
          <ArrowUp className="size-4" />
          {newSet.size} new {newSet.size === 1 ? "document" : "documents"} above
        </button>
      )}

      {query.isLoading && (
        <div className="flex justify-center py-12">
          <Spinner />
        </div>
      )}

      {query.isError && (
        <div className="text-sm text-destructive">
          Failed to load feed: {query.error.message}
        </div>
      )}

      {!query.isLoading && items.length === 0 && (
        <p className="text-sm text-muted-foreground">
          No documents yet. As your source adapters discover content, it will
          show up here.
        </p>
      )}

      <ul className="space-y-3">
        {items.map((item) => (
          <li key={item.id}>
            <DocumentCard
              item={item}
              isNew={newSet.has(item.id)}
              onClearNew={() => clearNew(item.id)}
            />
          </li>
        ))}
      </ul>

      {query.hasNextPage && (
        <div className="flex justify-center py-4">
          <Button
            variant="outline"
            disabled={query.isFetchingNextPage}
            onClick={() => query.fetchNextPage()}
          >
            {query.isFetchingNextPage ? <Spinner /> : "Load more"}
          </Button>
        </div>
      )}
    </div>
  );
}

// ─── helpers ────────────────────────────────────────────────────────────────

/** Prepend a freshly-arrived `FeedDocument` to the first page of the
 *  cached infinite-feed. Dedups by id so an SSE arrival that races the
 *  initial page-fetch doesn't double-render. Returns `old` unchanged if
 *  the cache hasn't been seeded yet (the initial query will then load
 *  the first page including this item, no prepend needed). */
function prependItem(
  old: InfiniteData<FeedPage> | undefined,
  item: FeedDocument,
): InfiniteData<FeedPage> | undefined {
  if (!old || old.pages.length === 0) return old;
  const [first, ...rest] = old.pages;
  if (first.items.some((it) => it.id === item.id)) return old;
  return {
    ...old,
    pages: [{ ...first, items: [item, ...first.items] }, ...rest],
  };
}

/** Tracks the app's scroll container (`#app-scroll` in __root.tsx).
 *  Returns `showPill` (true when scrolled away from top with >0 new
 *  arrivals) and a `scrollToTop` action. */
function usePillState(newCount: number) {
  const [scrollTop, setScrollTop] = useState(0);
  useEffect(() => {
    const el = document.getElementById(APP_SCROLL_ID);
    if (!el) return;
    const handler = () => setScrollTop(el.scrollTop);
    el.addEventListener("scroll", handler, { passive: true });
    handler();
    return () => el.removeEventListener("scroll", handler);
  }, []);
  return {
    showPill: scrollTop > 100 && newCount > 0,
    scrollToTop: () => {
      const el = document.getElementById(APP_SCROLL_ID);
      el?.scrollTo({ top: 0, behavior: "smooth" });
    },
  };
}
