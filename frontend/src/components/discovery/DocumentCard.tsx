/**
 * DocumentCard — one row in the Discovery feed.
 *
 * Presentational. Read state and "newness" highlight are driven by props
 * the parent page owns (so optimistic mutations live in one place and
 * the IntersectionObserver glow-fader can manage `isNew` from outside).
 *
 * Click anywhere on the card body marks the document read (one-shot —
 * idempotent on the server). The "Read" chip itself is a toggle — click
 * to mark unread without affecting the rest of the click target.
 */
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import type { FeedDocument } from "@/lib/api";

type Props = {
  item: FeedDocument;
  /** True while the card is in the per-session "newly arrived" set —
   *  drives the glow + "New" chip. Removed by the page after the card
   *  has been fully visible for ~1s (see useNewnessFade). */
  isNew: boolean;
  onMarkRead: (id: string) => void;
  onMarkUnread: (id: string) => void;
};

export function DocumentCard({ item, isNew, onMarkRead, onMarkUnread }: Props) {
  const dim = item.read;

  const handleCardClick = () => {
    if (!item.read) onMarkRead(item.id);
  };

  const handleChipClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    onMarkUnread(item.id);
  };

  return (
    <Card
      data-new={isNew ? "true" : undefined}
      data-doc-id={item.id}
      onClick={handleCardClick}
      className={cn(
        "cursor-pointer transition-shadow hover:shadow-md",
        isNew &&
          "ring-2 ring-primary/60 shadow-[0_0_24px_-4px_var(--color-primary)] transition-[box-shadow,outline-color] duration-700",
      )}
    >
      <CardHeader>
        <div className="flex items-start justify-between gap-3">
          <CardTitle
            className={cn(
              "text-base leading-snug",
              dim && "text-muted-foreground",
            )}
          >
            <a
              href={item.source_uri}
              target="_blank"
              rel="noopener noreferrer"
              onClick={(e) => e.stopPropagation()}
              className="hover:underline"
            >
              {item.title ?? item.canonical_id}
            </a>
          </CardTitle>
          <div className="flex shrink-0 items-center gap-2">
            {isNew && <Badge variant="default">New</Badge>}
            {dim && (
              <Badge
                variant="secondary"
                role="button"
                tabIndex={0}
                onClick={handleChipClick}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    handleChipClick(e as unknown as React.MouseEvent);
                  }
                }}
                className="cursor-pointer"
                aria-label="Mark unread"
              >
                Read
              </Badge>
            )}
          </div>
        </div>
        <Meta item={item} dim={dim} />
      </CardHeader>
      {item.summary && (
        <CardContent>
          <p
            className={cn(
              "text-sm line-clamp-3",
              dim && "text-muted-foreground",
            )}
          >
            {item.summary}
          </p>
        </CardContent>
      )}
    </Card>
  );
}

function Meta({ item, dim }: { item: FeedDocument; dim: boolean }) {
  const authors = item.authors.length
    ? item.authors.length > 4
      ? `${item.authors.slice(0, 3).join(", ")}, +${item.authors.length - 3}`
      : item.authors.join(", ")
    : null;

  return (
    <div
      className={cn(
        "text-xs flex flex-wrap items-center gap-x-2 gap-y-1",
        dim ? "text-muted-foreground/80" : "text-muted-foreground",
      )}
    >
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
