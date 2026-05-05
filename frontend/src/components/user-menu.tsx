/**
 * Sidebar user menu: avatar + email + Sign out.
 *
 * Sign-out flow: hit POST /api/auth/logout, invalidate the cached session
 * query, then hard-navigate to `/`. The hard nav forces the route's
 * `beforeLoad` to re-fetch `/api/auth/me`, which now 401s, which redirects
 * to /api/auth/login.
 */

import { useQueryClient } from "@tanstack/react-query";
import { LogOutIcon, UserIcon } from "lucide-react";

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { api } from "@/lib/api";
import { useSession } from "@/hooks/useSession";

export function UserMenu() {
  const { user, dev, isAuthenticated } = useSession();
  const qc = useQueryClient();

  if (!isAuthenticated || !user) return null;

  const onSignOut = async () => {
    try {
      await api.logout();
    } catch {
      // best-effort; we'll still flush local state
    }
    qc.invalidateQueries({ queryKey: ["session"] });
    window.location.href = "/";
  };

  const label = user.name?.trim() || user.email || "Signed in";

  return (
    <DropdownMenu>
      <DropdownMenuTrigger className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-[var(--accent)]">
        <div className="flex size-7 shrink-0 items-center justify-center rounded-full bg-[var(--secondary)]">
          <UserIcon className="size-4 text-[var(--muted-foreground)]" />
        </div>
        <div className="min-w-0 flex-1">
          <div className="truncate text-xs font-medium">{label}</div>
          <div className="truncate text-[10px] text-[var(--muted-foreground)]">
            {dev ? "dev mode" : user.email}
          </div>
        </div>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" side="top">
        <DropdownMenuLabel className="text-xs">{user.email}</DropdownMenuLabel>
        <DropdownMenuSeparator />
        <DropdownMenuItem onClick={onSignOut}>
          <LogOutIcon className="mr-2 size-4" />
          Sign out
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
