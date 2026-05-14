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
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { ArrowLeft } from "lucide-react";
import { Document, Page, pdfjs } from "react-pdf";
import "react-pdf/dist/Page/AnnotationLayer.css";
import "react-pdf/dist/Page/TextLayer.css";

import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { documentFileUrl } from "@/lib/api";

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
};

export function PdfViewer({ documentId, title, onBack }: Props) {
  const { data, error, loading } = usePdfBlob(documentId);
  const [numPages, setNumPages] = useState<number | null>(null);

  // react-pdf's `file` prop must be referentially stable — re-creating
  // the object on every render restarts the load.
  const fileProp = useMemo(() => (data ? { data } : null), [data]);

  // Width fits the available pane; updated on resize so pages re-render
  // crisply rather than scaling a stale bitmap.
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const width = useFitWidth(wrapperRef);

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
            {Array.from({ length: numPages ?? 0 }, (_, i) => (
              <Page
                key={i + 1}
                pageNumber={i + 1}
                width={width}
                className="shadow-md"
                renderAnnotationLayer
                renderTextLayer
              />
            ))}
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
