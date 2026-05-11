/**
 * Discovery feed.
 *
 * Reverse-chronological infinite scroll over the user's corpus, with:
 *  - cursor pagination ("Load more" at the bottom)
 *  - SSE live-updates that prepend new arrivals to the top
 *  - per-session "new" highlight (chip + glow) that fades 1s after the
 *    card has been ≥50% in view
 *  - "N new above" floating pill when the user has scrolled away from
 *    the top
 *  - per-card mark-read / mark-unread with optimistic updates
 *
 * Scroll-position preservation when prepending is delegated to the
 * browser's `overflow-anchor: auto` (default in modern browsers); no JS
 * scroll bookkeeping required.
 *
 * Lives outside `routes/` so the route file can stay tiny — TanStack
 * Router code-splits one chunk per route file, and any non-`Route`
 * exports there break the split.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import {
  useInfiniteQuery,
  useMutation,
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
import { PaperCard } from "@/components/discovery/PaperCard";
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

  // Per-session "newly arrived via SSE" set. The IO fader removes ids
  // after the card has been visible for 1s.
  const [newSet, setNewSet] = useState<Set<string>>(new Set());
  const removeFromNewSet = (id: string) => {
    setNewSet((prev) => {
      if (!prev.has(id)) return prev;
      const next = new Set(prev);
      next.delete(id);
      return next;
    });
  };

  // SSE arrival → prepend the FeedItem directly into the cache. The
  // event payload IS a FeedDocument (same wire shape /api/discovery/feed
  // returns), so no refetch is needed. Dedup by id covers the rare race
  // where the initial page-fetch already includes the doc.
  useFeedEvents((item) => {
    setNewSet((prev) => new Set(prev).add(item.id));
    queryClient.setQueryData<InfiniteData<FeedPage>>(queryKey, (old) =>
      prependItem(old, item),
    );
  });

  // Mark-read / unread mutations: optimistic, with rollback on error.
  const markRead = useMutation({
    mutationFn: (id: string) => api.discovery.markRead(id),
    onMutate: (id) => optimisticReadFlip(queryClient, queryKey, id, true),
    onError: (_e, _id, ctx) => rollbackReadFlip(queryClient, queryKey, ctx),
  });
  const markUnread = useMutation({
    mutationFn: (id: string) => api.discovery.markUnread(id),
    onMutate: (id) => optimisticReadFlip(queryClient, queryKey, id, false),
    onError: (_e, _id, ctx) => rollbackReadFlip(queryClient, queryKey, ctx),
  });

  // Newness-fade observer + scroll-tracking for the pill.
  const { showPill, scrollToTop } = usePillState(newSet.size);
  useNewnessFade({ items, removeFromNewSet });

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
          {newSet.size} new {newSet.size === 1 ? "paper" : "papers"} above
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
          No papers yet. As your source adapters discover content, it will
          show up here.
        </p>
      )}

      <ul className="space-y-3">
        {items.map((item) => (
          <li key={item.id}>
            <PaperCard
              item={item}
              isNew={newSet.has(item.id)}
              onMarkRead={(id) => markRead.mutate(id)}
              onMarkUnread={(id) => markUnread.mutate(id)}
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

type ReadFlipCtx = { prev: InfiniteData<FeedPage> | undefined };

async function optimisticReadFlip(
  qc: ReturnType<typeof useQueryClient>,
  queryKey: readonly unknown[],
  id: string,
  read: boolean,
): Promise<ReadFlipCtx> {
  await qc.cancelQueries({ queryKey });
  const prev = qc.getQueryData<InfiniteData<FeedPage>>(queryKey);
  qc.setQueryData<InfiniteData<FeedPage>>(queryKey, (old) =>
    old
      ? {
          ...old,
          pages: old.pages.map((p) => ({
            ...p,
            items: p.items.map((it) =>
              it.id === id ? { ...it, read } : it,
            ),
          })),
        }
      : old,
  );
  return { prev };
}

function rollbackReadFlip(
  qc: ReturnType<typeof useQueryClient>,
  queryKey: readonly unknown[],
  ctx: ReadFlipCtx | undefined,
) {
  if (ctx?.prev) qc.setQueryData(queryKey, ctx.prev);
}

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

/** Watches for `[data-new="true"]` cards entering the viewport at ≥50%
 *  visibility, then removes them from `newSet` 1s later (cancelled if
 *  they leave the viewport before the timer fires). */
function useNewnessFade({
  items,
  removeFromNewSet,
}: {
  items: { id: string }[];
  removeFromNewSet: (id: string) => void;
}) {
  // Stable reference for the IO callback so we don't tear it down on
  // every items change.
  const removeRef = useRef(removeFromNewSet);
  removeRef.current = removeFromNewSet;

  useEffect(() => {
    const root = document.getElementById(APP_SCROLL_ID);
    if (!root) return;
    const timers = new Map<Element, ReturnType<typeof setTimeout>>();

    const io = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          const id = entry.target.getAttribute("data-doc-id");
          if (!id) continue;
          if (entry.isIntersecting && entry.intersectionRatio >= 0.5) {
            if (!timers.has(entry.target)) {
              const t = setTimeout(() => {
                timers.delete(entry.target);
                removeRef.current(id);
              }, 1000);
              timers.set(entry.target, t);
            }
          } else {
            const t = timers.get(entry.target);
            if (t) {
              clearTimeout(t);
              timers.delete(entry.target);
            }
          }
        }
      },
      { root, threshold: 0.5 },
    );

    const observeAll = () => {
      root
        .querySelectorAll<HTMLElement>('[data-new="true"]')
        .forEach((el) => io.observe(el));
    };
    observeAll();

    // Cards mount/unmount as `items` changes; re-scan on next frame so
    // freshly mounted "new" cards get observed without polling.
    const raf = requestAnimationFrame(observeAll);

    return () => {
      cancelAnimationFrame(raf);
      timers.forEach(clearTimeout);
      io.disconnect();
    };
  }, [items]);
}
