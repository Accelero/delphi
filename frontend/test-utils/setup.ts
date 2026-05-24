/**
 * Vitest setup file (runs once per test file).
 *
 * - Wires `@testing-library/jest-dom` matchers (`toBeInTheDocument`, etc.)
 *   into Vitest's `expect`.
 * - Spins up the MSW server for `/api/*` mocks. Tests that need bespoke
 *   responses use `server.use(...)` to add per-test handlers.
 */

import "@testing-library/jest-dom/vitest";
import { afterAll, afterEach, beforeAll } from "vitest";

import { server } from "./msw/server";

// pdf.js (pulled in via `react-pdf` in PdfViewer) touches `DOMMatrix` at
// module-eval time, which jsdom doesn't implement — so merely importing a
// component tree that contains the viewer (e.g. Feed) throws
// `ReferenceError: DOMMatrix is not defined`. Our tests never render a real
// PDF, so a no-op stub is enough to let the module load. Defined here in the
// setup file so it exists before any test module's imports evaluate.
if (!("DOMMatrix" in globalThis)) {
  class DOMMatrixStub {
    // chained transform calls just return self; pdf.js only needs the
    // surface to exist for the code paths our component tests reach.
    multiply() {
      return this;
    }
    translate() {
      return this;
    }
    scale() {
      return this;
    }
    inverse() {
      return this;
    }
  }
  (globalThis as { DOMMatrix?: unknown }).DOMMatrix = DOMMatrixStub;
}

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());
