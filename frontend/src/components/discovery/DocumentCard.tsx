/**
 * DocumentCard — one row in the Discovery feed.
 *
 * Presentational. The "newness" highlight is driven by props the parent
 * page owns; the card calls `onClearNew` the first time the user's
 * cursor enters it. Pure cosmetic — nothing about this state is
 * persisted.
 */
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn, safeHref } from "@/lib/utils";
import type { FeedDocument } from "@/lib/api";

type Props = {
  item: FeedDocument;
  /** True while the card is in the per-session "newly arrived" set —
   *  drives the glow + "New" chip. */
  isNew: boolean;
  /** Fires on `mouseenter` to clear the glow. The parent decides what
   *  to do (typically: drop the id from its newSet). */
  onClearNew: () => void;
  /** Fires when the user wants to open the stored original in the
   *  in-app PDF viewer. The parent (Feed) tracks which document is
   *  open. Omitted ⇒ title falls back to a link to `source_uri`. Only
   *  invoked when `item.storage_uri` is present. */
  onOpen?: (item: FeedDocument) => void;
};

export function DocumentCard({ item, isNew, onClearNew, onOpen }: Props) {
  const canOpen = !!onOpen && !!item.storage_uri;
  // Scheme-allowlist the source URL before it becomes an href (audit M9).
  const href = safeHref(item.source_uri);
  return (
    <Card
      onMouseEnter={isNew ? onClearNew : undefined}
      className={cn(
        "transition-shadow hover:shadow-md",
        isNew &&
          "ring-2 ring-primary/60 shadow-[0_0_24px_-4px_var(--color-primary)] transition-[box-shadow,outline-color] duration-700",
      )}
    >
      <CardHeader>
        <div className="flex items-start justify-between gap-3">
          <CardTitle className="text-base leading-snug">
            {canOpen ? (
              <button
                type="button"
                onClick={() => onOpen!(item)}
                className="text-left hover:underline"
              >
                {item.title ?? item.canonical_id}
              </button>
            ) : href ? (
              <a
                href={href}
                target="_blank"
                rel="noopener noreferrer"
                className="hover:underline"
              >
                {item.title ?? item.canonical_id}
              </a>
            ) : (
              // Non-http(s) / missing source_uri: render as text, never a link.
              <span>{item.title ?? item.canonical_id}</span>
            )}
          </CardTitle>
          {isNew && (
            <div className="flex shrink-0 items-center gap-2">
              <Badge variant="default">New</Badge>
            </div>
          )}
        </div>
        <Meta item={item} />
      </CardHeader>
      {item.summary && (
        <CardContent>
          <p className="text-sm line-clamp-3 text-muted-foreground">
            {item.summary}
          </p>
        </CardContent>
      )}
    </Card>
  );
}

function Meta({ item }: { item: FeedDocument }) {
  const authors = item.authors.length
    ? item.authors.length > 4
      ? `${item.authors.slice(0, 3).join(", ")}, +${item.authors.length - 3}`
      : item.authors.join(", ")
    : null;

  return (
    <div className="text-xs flex flex-wrap items-center gap-x-2 gap-y-1 text-muted-foreground">
      {authors && <span>{authors}</span>}
      {authors && <span aria-hidden>·</span>}
      <span className="uppercase tracking-wide">{item.source_type}</span>
      {item.ingested_at && (
        <>
          <span aria-hidden>·</span>
          <span>{formatRelative(item.ingested_at)}</span>
        </>
      )}
    </div>
  );
}

/** Compact relative-time formatter. ISO string in, "2h ago" / "3d ago"
 *  out. Falls back to the date for >30d. */
function formatRelative(iso: string): string {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "";
  const diffSec = Math.max(0, (Date.now() - then) / 1000);
  if (diffSec < 60) return "just now";
  const diffMin = diffSec / 60;
  if (diffMin < 60) return `${Math.floor(diffMin)}m ago`;
  const diffHr = diffMin / 60;
  if (diffHr < 24) return `${Math.floor(diffHr)}h ago`;
  const diffDay = diffHr / 24;
  if (diffDay < 30) return `${Math.floor(diffDay)}d ago`;
  return new Date(iso).toLocaleDateString();
}
