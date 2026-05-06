/**
 * Canned fixtures shared across tests. Kept minimal — anything specific to
 * a single test stays in that test file.
 */

import type { Session } from "@/lib/api";

export const fixtures = {
  session: {
    user: {
      id: "app_user:test",
      email: "test@delphi.test",
      name: "Test User",
    },
    tenant: { id: "tenant:test" },
    dev: false,
  } satisfies Session & { roles?: string[] },
};
