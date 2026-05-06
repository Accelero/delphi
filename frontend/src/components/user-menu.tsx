/**
 * Sidebar user menu: avatar + email + Sign out.
 *
 * Sign-out is owned by the BFF (oauth2-proxy), so we hard-navigate to
 * `/oauth2/sign_out`, which clears the session cookie and bounces back to
 * the IdP's logout endpoint. In dev mode (Tier 1) there's no BFF and no
 * cookie to clear — the menu hides the sign-out item entirely, since
 * "logging out" the dev user is a contradiction.
 */

import { LogOutIcon, UserIcon } from "lucide-react";

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { SIGN_OUT_URL } from "@/lib/api";
import { useSession } from "@/hooks/useSession";

export function UserMenu() {
  const { user, dev, isAuthenticated } = useSession();

  if (!isAuthenticated || !user) return null;

  const onSignOut = () => {
    window.location.href = SIGN_OUT_URL;
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
        {!dev && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={onSignOut}>
              <LogOutIcon className="mr-2 size-4" />
              Sign out
            </DropdownMenuItem>
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
