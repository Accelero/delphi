/**
 * Pure PDF→CSS coordinate-transform utilities, separated from
 * `PdfViewer.tsx` so unit tests can import them without dragging in
 * `react-pdf` / `pdfjs-dist` (whose worker bootstrap needs `DOMMatrix`
 * and trips up `jsdom`).
 */
import type { ChunkBbox } from "@/lib/api";

/** Page metadata captured from react-pdf's `onLoadSuccess` — the
 *  geometry the overlay scales against. */
export type PageMeta = {
  /** PDF-point width of the page viewport. */
  pdfWidth: number;
  /** PDF-point height. */
  pdfHeight: number;
  /** Page rotation in degrees (0 / 90 / 180 / 270). */
  rotate: number;
  /** Actual CSS width of the page render. */
  cssWidth: number;
};

/** Transform a PDF-point bbox into CSS pixels relative to the rendered
 *  page canvas. Origin flip + rotation handled per the design doc.
 *
 *  v1 supports rotation 0 well; 90/180/270 use the rotated transform
 *  derived in the spec but ship without a rotated fixture verifying
 *  them — a visible-but-wrong overlay is better than a hard crash.
 */
export function transformBbox(
  bbox: ChunkBbox,
  meta: PageMeta,
): { left: number; top: number; width: number; height: number } {
  const scale = meta.cssWidth / meta.pdfWidth;
  const rotated =
    meta.rotate === 0
      ? bbox
      : meta.rotate === 90
      ? {
          page: bbox.page,
          x: bbox.y,
          y: bbox.x,
          w: bbox.h,
          h: bbox.w,
        }
      : meta.rotate === 180
      ? {
          page: bbox.page,
          x: meta.pdfWidth - bbox.x - bbox.w,
          y: bbox.y,
          w: bbox.w,
          h: bbox.h,
        }
      : meta.rotate === 270
      ? {
          page: bbox.page,
          x: meta.pdfHeight - bbox.y - bbox.h,
          y: meta.pdfWidth - bbox.x - bbox.w,
          w: bbox.h,
          h: bbox.w,
        }
      : bbox;
  const left = rotated.x * scale;
  const top = (meta.pdfHeight - rotated.y - rotated.h) * scale;
  return {
    left,
    top,
    width: rotated.w * scale,
    height: rotated.h * scale,
  };
}
