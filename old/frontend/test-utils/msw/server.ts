/**
 * MSW server for Node/jsdom test runs (Vitest). Browser-mode mocks would
 * use `setupWorker` instead — we don't have a frontend-only Playwright tier
 * (decision recorded in the test plan), so this is the only MSW entry point.
 */

import { setupServer } from "msw/node";

import { handlers } from "./handlers";

export const server = setupServer(...handlers);
