/**
 * RTL render wrapper that pre-mounts the providers every component test
 * expects (QueryClient, Theme, Tooltip). Use in tests:
 *
 *     import { render } from '@/test-utils/render';
 *     render(<UserMenu />);
 *
 * Each call gets a fresh QueryClient so tests don't share cache state.
 */

import type { ReactElement } from "react";
import { render as rtlRender } from "@testing-library/react";
import type { RenderOptions, RenderResult } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

function makeQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0, staleTime: 0 },
      mutations: { retry: false },
    },
  });
}

export function render(
  ui: ReactElement,
  options?: Omit<RenderOptions, "wrapper">,
): RenderResult & { queryClient: QueryClient } {
  const queryClient = makeQueryClient();
  const result = rtlRender(ui, {
    ...options,
    wrapper: ({ children }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    ),
  });
  return { ...result, queryClient };
}

export * from "@testing-library/react";
