/**
 * PdfViewer — `transformBbox` PDF→CSS math.
 *
 * The interesting bit is the coordinate transform, not the react-pdf
 * integration (which the e2e covers). We test the transform directly
 * against a known page geometry and a few representative bboxes.
 */
import { describe, expect, test } from "vitest";

import { transformBbox } from "./PdfViewerMath";

describe("transformBbox", () => {
  test("rotation=0 flips PDF bottom-left y into CSS top-left", () => {
    // 612 × 792 page rendered at 612 px wide ⇒ scale=1.
    const meta = { pdfWidth: 612, pdfHeight: 792, rotate: 0, cssWidth: 612 };
    // PDF bbox at y=708, h=12 (a line near the top of the page).
    const css = transformBbox(
      { page: 1, x: 72, y: 708, w: 200, h: 12 },
      meta,
    );
    // css y = 792 - 708 - 12 = 72  (line is 72px from the top, as expected).
    expect(css.left).toBeCloseTo(72);
    expect(css.top).toBeCloseTo(72);
    expect(css.width).toBeCloseTo(200);
    expect(css.height).toBeCloseTo(12);
  });

  test("scales linearly when the page is rendered at a smaller width", () => {
    const meta = { pdfWidth: 612, pdfHeight: 792, rotate: 0, cssWidth: 306 };
    const css = transformBbox(
      { page: 1, x: 60, y: 780, w: 60, h: 6 },
      meta,
    );
    // scale = 0.5; css y = (792 - 780 - 6) * 0.5 = 3
    expect(css.left).toBeCloseTo(30);
    expect(css.top).toBeCloseTo(3);
    expect(css.width).toBeCloseTo(30);
    expect(css.height).toBeCloseTo(3);
  });

  test("rotate=180 mirrors x but keeps the y flip", () => {
    const meta = { pdfWidth: 612, pdfHeight: 792, rotate: 180, cssWidth: 612 };
    const css = transformBbox(
      { page: 1, x: 72, y: 708, w: 200, h: 12 },
      meta,
    );
    // Rotated x = 612 - 72 - 200 = 340
    expect(css.left).toBeCloseTo(340);
    // y handling same as rotation=0
    expect(css.top).toBeCloseTo(72);
  });
});
