/**
 * `useSession` — TanStack Query against `/api/auth/me`.
 *
 * The backend owns the auth tokens (BFF). Frontend only knows whether
 * there's a valid session cookie. `isAuthenticated` is true once `/me`
 * returned 200; `dev` distinguishes dev-mode auth bypass from real OIDC.
 */

import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";

export function useSession() {
  const q = useQuery({
    queryKey: ["session"],
    queryFn: api.session,
    retry: false,
    staleTime: 5 * 60_000,
  });
  return {
    user: q.data?.user,
    tenant: q.data?.tenant,
    dev: q.data?.dev ?? false,
    isLoading: q.isLoading,
    isAuthenticated: !!q.data,
  };
}
