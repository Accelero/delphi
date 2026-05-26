/**
 * Subscribe to the Discovery feed's SSE stream.
 *
 * Opens one `EventSource` per mount and forwards each `new_document`
 * payload to the supplied handler. The wire shape **is** a
 * `FeedDocument` — same as what `/api/discovery/feed` returns — so the
 * caller can prepend it directly into the React Query cache without an
 * extra refetch.
 *
 * Uses the standard "ref-stable handler" idiom so consumers can pass an
 * inline callback without recreating the EventSource on every render.
 *
 * Browser auto-reconnects on transient drops. We don't surface
 * connection state in v1; if the stream stays down the user just won't
 * see the live "N new above" pill until they refresh.
 */
import { useEffect, useRef } from "react";

import { api, type FeedDocument } from "@/lib/api";

export function useFeedEvents(onNewDocument: (item: FeedDocument) => void): void {
  const handlerRef = useRef(onNewDocument);
  handlerRef.current = onNewDocument;

  useEffect(() => {
    const es = new EventSource(api.discovery.eventsUrl);
    const listener = (ev: MessageEvent) => {
      try {
        const item = JSON.parse(ev.data) as FeedDocument;
        handlerRef.current(item);
      } catch (err) {
        // Don't crash the page over a malformed event; log and skip.
        // eslint-disable-next-line no-console
        console.error("invalid SSE payload", err);
      }
    };
    es.addEventListener("new_document", listener as EventListener);
    return () => {
      es.removeEventListener("new_document", listener as EventListener);
      es.close();
    };
  }, []);
}
