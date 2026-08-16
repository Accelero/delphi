import { Link } from "@tanstack/react-router";
import {
  FileUp,
  MessageSquare,
  Monitor,
  Moon,
  PanelLeftClose,
  PanelLeftOpen,
  Sun
} from "lucide-react";
import type { ReactNode } from "react";
import type { ThemeMode } from "../../hooks/useThemeMode";
import { cn } from "../../lib/utils";

export function AppNavigation({
  collapsed,
  chatActive,
  uploadActive,
  themeMode,
  onToggleCollapsed,
  onThemeModeChange
}: {
  collapsed: boolean;
  chatActive: boolean;
  uploadActive: boolean;
  themeMode: ThemeMode;
  onToggleCollapsed: () => void;
  onThemeModeChange: (mode: ThemeMode) => void;
}) {
  return (
    <aside
      className={cn(
        "flex h-full shrink-0 flex-col border-r border-[var(--color-border)] bg-[var(--color-surface-muted)] transition-[width] duration-150",
        collapsed ? "w-16" : "w-56"
      )}
    >
      <div className="flex h-14 items-center justify-between border-b border-[var(--color-border)] px-3">
        {!collapsed && <div className="truncate text-sm font-semibold">Delphi</div>}
        <button
          type="button"
          className="grid h-9 w-9 shrink-0 place-items-center rounded-md text-[var(--color-text-muted)] hover:bg-[var(--color-surface-hover)] hover:text-[var(--color-text)]"
          onClick={onToggleCollapsed}
          aria-label={collapsed ? "Expand navigation" : "Collapse navigation"}
          title={collapsed ? "Expand navigation" : "Collapse navigation"}
        >
          {collapsed ? <PanelLeftOpen className="h-4 w-4" /> : <PanelLeftClose className="h-4 w-4" />}
        </button>
      </div>

      <nav className="flex flex-1 flex-col gap-1 p-2" aria-label="Primary navigation">
        <NavLink collapsed={collapsed} active={chatActive} to="/chat" label="Chat">
          <MessageSquare className="h-4 w-4" />
        </NavLink>
        <NavLink collapsed={collapsed} active={uploadActive} to="/upload" label="Upload">
          <FileUp className="h-4 w-4" />
        </NavLink>
      </nav>

      <div className="border-t border-[var(--color-border)] p-2">
        <div
          className={
            collapsed
              ? "flex flex-col gap-1 rounded-md bg-[var(--color-surface)] p-1"
              : "grid grid-cols-3 gap-1 rounded-md bg-[var(--color-surface)] p-1"
          }
        >
          <ThemeButton
            mode="system"
            active={themeMode === "system"}
            onClick={onThemeModeChange}
            label="System theme"
          >
            <Monitor className="h-4 w-4" />
          </ThemeButton>
          <ThemeButton
            mode="light"
            active={themeMode === "light"}
            onClick={onThemeModeChange}
            label="Light theme"
          >
            <Sun className="h-4 w-4" />
          </ThemeButton>
          <ThemeButton
            mode="dark"
            active={themeMode === "dark"}
            onClick={onThemeModeChange}
            label="Dark theme"
          >
            <Moon className="h-4 w-4" />
          </ThemeButton>
        </div>
      </div>
    </aside>
  );
}

function NavLink({
  collapsed,
  active,
  to,
  label,
  children
}: {
  collapsed: boolean;
  active: boolean;
  to: "/chat" | "/upload";
  label: string;
  children: ReactNode;
}) {
  return (
    <Link
      to={to}
      className={
        active
          ? "flex h-10 items-center gap-3 rounded-md bg-[var(--color-surface-raised)] px-3 text-sm font-medium text-[var(--color-text)] shadow-[var(--shadow-raised)]"
          : "flex h-10 items-center gap-3 rounded-md px-3 text-sm text-[var(--color-text-muted)] hover:bg-[var(--color-surface-hover)] hover:text-[var(--color-text)]"
      }
      aria-label={collapsed ? label : undefined}
      title={collapsed ? label : undefined}
    >
      <span className="grid h-4 w-4 shrink-0 place-items-center">{children}</span>
      {!collapsed && <span className="truncate">{label}</span>}
    </Link>
  );
}

function ThemeButton({
  mode,
  active,
  onClick,
  label,
  children
}: {
  mode: ThemeMode;
  active: boolean;
  onClick: (mode: ThemeMode) => void;
  label: string;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      className={
        active
          ? "flex h-8 items-center justify-center rounded bg-[var(--color-surface-raised)] text-[var(--color-text)] shadow-[var(--shadow-raised)]"
          : "flex h-8 items-center justify-center rounded text-[var(--color-text-muted)] hover:text-[var(--color-text)]"
      }
      onClick={() => onClick(mode)}
      aria-label={label}
      title={label}
    >
      {children}
    </button>
  );
}
