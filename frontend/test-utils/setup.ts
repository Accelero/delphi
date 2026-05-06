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

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());
