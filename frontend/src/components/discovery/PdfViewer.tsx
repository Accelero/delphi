/**
 * In-browser PDF viewer.
 *
 * Mounts on top of the Feed when the user clicks a document with a
 * stored original. The Feed is *kept mounted* underneath (display:none
 * in the parent) so the back button returns the user to their exact
 * scroll position and any in-memory state.
 *
 * Bytes come from `GET /api/documents/:key/file` — same-origin so the
 * BFF session cookie travels with the request. We pull the response as
 * a Blob and hand react-pdf a stable `{ data }` reference; without the
 * memo, react-pdf reinitialises the worker on every render.
 *
 * ## RAG v1 — chunk overlays
 *
 * When opened with a `chunkId`, the viewer additionally fetches
 * `GET /api/chunks/:id` (per-line PDF-point rectangles), scrolls to
 * the first page in the chunk's bbox list, and draws translucent CSS
 * overlays atop the relevant page canvases. The PDF→CSS transform
 * lives in `transformBbox` below.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { ArrowLeft } from "lucide-react";
import { Document, Page, pdfjs } from "react-pdf";
import type { PageCallback } from "react-pdf/dist/shared/types.js";
import "react-pdf/dist/Page/AnnotationLayer.css";
import "react-pdf/dist/Page/TextLayer.css";

import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { api, documentFileUrl, type ChunkPayload } from "@/lib/api";
import { transformBbox, type PageMeta } from "./PdfViewerMath";

// pdfjs ships its worker as a separate ESM module. Vite resolves the
// `new URL(..., import.meta.url)` form to a hashed asset at build time
// and to a dev-server URL in development — no extra config needed.
pdfjs.GlobalWorkerOptions.workerSrc = new URL(
  "pdfjs-dist/build/pdf.worker.min.mjs",
  import.meta.url,
).toString();

type Props = {
  documentId: string;
  title: string;
  onBack: () => void;
  /** Optional `chunk:<key>` id. When set, the viewer also fetches
   *  `/api/chunks/:id`, scrolls to the first page mentioned in the
   *  bbox list, and overlays per-line CSS rectangles. */
  chunkId?: string | null;
};

export function PdfViewer({ documentId, title, onBack, chunkId }: Props) {
  const { data, error, loading } = usePdfBlob(documentId);
  const [numPages, setNumPages] = useState<number | null>(null);
  const chunk = useChunk(chunkId);

  // Page metadata per rendered page number (1-indexed).
  const [pageMeta, setPageMeta] = useState<Record<number, PageMeta>>({});

  const fileProp = useMemo(() => (data ? { data } : null), [data]);

  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const width = useFitWidth(wrapperRef);

  // Page refs so we can scroll the first highlighted page into view
  // once both the chunk payload and that page's metadata are ready.
  const pageRefs = useRef<Record<number, HTMLDivElement | null>>({});
  const scrolledTo = useRef<number | null>(null);
  useEffect(() => {
    if (!chunk.data?.bboxes?.length) return;
    const firstPage = chunk.data.bboxes[0]?.page ?? 0;
    if (firstPage <= 0) return;
    if (scrolledTo.current === firstPage) return;
    const target = pageRefs.current[firstPage];
    if (target && pageMeta[firstPage]) {
      target.scrollIntoView({ behavior: "smooth", block: "start" });
      scrolledTo.current = firstPage;
    }
  }, [chunk.data, pageMeta]);

  return (
    <div className="fixed inset-0 z-50 flex flex-col bg-background">
      <header className="flex items-center gap-3 border-b border-[var(--border)] px-4 py-2">
        <Button variant="ghost" size="sm" onClick={onBack} aria-label="Back to feed">
          <ArrowLeft className="size-4" />
          Back
        </Button>
        <h1 className="truncate text-sm font-medium">{title}</h1>
        {numPages && (
          <span className="ml-auto text-xs text-muted-foreground">
            {numPages} {numPages === 1 ? "page" : "pages"}
          </span>
        )}
      </header>
      <div
        ref={wrapperRef}
        className="flex-1 overflow-auto bg-muted/30 px-4 py-6"
      >
        {loading && (
          <div className="flex justify-center py-12">
            <Spinner />
          </div>
        )}
        {error && (
          <div className="text-sm text-destructive">
            Failed to load PDF: {error.message}
          </div>
        )}
        {fileProp && (
          <Document
            file={fileProp}
            onLoadSuccess={(pdf) => setNumPages(pdf.numPages)}
            loading={
              <div className="flex justify-center py-12">
                <Spinner />
              </div>
            }
            error={
              <div className="text-sm text-destructive">
                Could not render this PDF.
              </div>
            }
            className="mx-auto flex max-w-4xl flex-col items-center gap-4"
          >
            {Array.from({ length: numPages ?? 0 }, (_, i) => {
              const pageNumber = i + 1;
              const overlays =
                chunk.data?.bboxes?.filter((b) => b.page === pageNumber) ?? [];
              const meta = pageMeta[pageNumber];
              return (
                <div
                  key={pageNumber}
                  ref={(el) => {
                    pageRefs.current[pageNumber] = el;
                  }}
                  className="relative shadow-md"
                  data-testid={`pdf-page-${pageNumber}`}
                >
                  <Page
                    pageNumber={pageNumber}
                    width={width}
                    renderAnnotationLayer
                    renderTextLayer
                    onLoadSuccess={(pageProxy: PageCallback) => {
                      // `pageProxy.view` = [x_min, y_min, x_max, y_max] in
                      // PDF points; width/height fall straight out of it.
                      const view = (pageProxy as unknown as { view: number[] }).view ?? [
                        0, 0, 612, 792,
                      ];
                      const pdfWidth = view[2] - view[0];
                      const pdfHeight = view[3] - view[1];
                      const rotate =
                        (pageProxy as unknown as { rotate?: number }).rotate ?? 0;
                      setPageMeta((prev) => ({
                        ...prev,
                        [pageNumber]: { pdfWidth, pdfHeight, rotate, cssWidth: width },
                      }));
                    }}
                  />
                  {meta &&
                    overlays.map((b, idx) => {
                      const css = transformBbox(b, meta);
                      return (
                        <div
                          key={idx}
                          className="delphi-chunk-overlay pointer-events-none absolute"
                          data-testid="chunk-overlay"
                          style={{
                            left: `${css.left}px`,
                            top: `${css.top}px`,
                            width: `${css.width}px`,
                            height: `${css.height}px`,
                            background: "rgba(255, 235, 59, 0.35)",
                            outline: "1px solid rgba(255, 193, 7, 0.7)",
                            borderRadius: "2px",
                          }}
                        />
                      );
                    })}
                </div>
              );
            })}
          </Document>
        )}
      </div>
    </div>
  );
}

/** Fetch the PDF bytes for a document. Stays at the `Uint8Array` level so
 *  the result can be passed to react-pdf's `{ data }` prop without it
 *  detaching the underlying buffer between renders (which is what
 *  happens when you hand it an ArrayBuffer directly). */
function usePdfBlob(documentId: string) {
  const [data, setData] = useState<Uint8Array | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let aborted = false;
    setData(null);
    setError(null);
    setLoading(true);
    fetch(documentFileUrl(documentId), { credentials: "same-origin" })
      .then(async (res) => {
        if (!res.ok) {
          throw new Error(`${res.status} ${res.statusText}`);
        }
        return new Uint8Array(await res.arrayBuffer());
      })
      .then((bytes) => {
        if (aborted) return;
        setData(bytes);
        setLoading(false);
      })
      .catch((e) => {
        if (aborted) return;
        setError(e instanceof Error ? e : new Error(String(e)));
        setLoading(false);
      });
    return () => {
      aborted = true;
    };
  }, [documentId]);

  return { data, error, loading };
}

/** Fetch a chunk's metadata + bboxes. Tolerates `chunkId === null` so
 *  callers can unconditionally wire the hook. */
function useChunk(chunkId: string | null | undefined): {
  data: ChunkPayload | null;
  error: Error | null;
} {
  const [data, setData] = useState<ChunkPayload | null>(null);
  const [error, setError] = useState<Error | null>(null);
  useEffect(() => {
    let aborted = false;
    setData(null);
    setError(null);
    if (!chunkId) return;
    api.chunks
      .get(chunkId)
      .then((c) => {
        if (!aborted) setData(c);
      })
      .catch((e) => {
        if (!aborted) setError(e instanceof Error ? e : new Error(String(e)));
      });
    return () => {
      aborted = true;
    };
  }, [chunkId]);
  return { data, error };
}

/** Tracks the inner width of `ref` so PDF pages can fit the pane.
 *  Subtracts the px-4 / px-6 padding the wrapper applies. */
function useFitWidth(ref: React.RefObject<HTMLDivElement | null>) {
  const [width, setWidth] = useState<number>(800);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const update = () => {
      // 32px = 2 × px-4 horizontal padding on the wrapper.
      const inner = Math.max(320, el.clientWidth - 32);
      // Cap at the same max-width the Document container uses so the
      // page doesn't outgrow its column on a wide screen.
      setWidth(Math.min(inner, 896));
    };
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => ro.disconnect();
  }, [ref]);
  return width;
}

