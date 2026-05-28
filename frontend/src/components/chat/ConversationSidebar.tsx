import { Link } from "@tanstack/react-router";
import { MessageSquarePlus, Monitor, Moon, Sun, Trash2 } from "lucide-react";
import type { ReactNode } from "react";
import type { ThemeMode } from "../../hooks/useThemeMode";
import type { ConversationSummary } from "../../lib/types";
import { Button } from "../ui/button";

export function ConversationSidebar({
  conversations,
  activeId,
  onCreate,
  onDelete,
  themeMode,
  onThemeModeChange
}: {
  conversations: ConversationSummary[];
  activeId: string | null;
  onCreate: () => void;
  onDelete: (id: string) => void;
  themeMode: ThemeMode;
  onThemeModeChange: (mode: ThemeMode) => void;
}) {
  return (
    <aside className="flex h-full w-72 shrink-0 flex-col border-r border-[var(--color-border)] bg-[var(--color-surface-muted)]">
      <div className="flex h-14 items-center justify-between border-b border-[var(--color-border)] px-3">
        <div className="text-sm font-semibold">Delphi</div>
        <Button size="icon" variant="ghost" onClick={onCreate} aria-label="New chat">
          <MessageSquarePlus className="h-4 w-4" />
        </Button>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto p-2">
        {conversations.map((conversation) => (
          <div
            key={conversation.id}
            className={
              conversation.id === activeId
                ? "group flex items-center rounded-md bg-[var(--color-surface-raised)] shadow-[var(--shadow-raised)]"
                : "group flex items-center rounded-md hover:bg-[var(--color-surface-hover)]"
            }
          >
            <Link
              to="/chat/$conversationId"
              params={{ conversationId: conversation.id }}
              className="min-w-0 flex-1 truncate px-3 py-2 text-left text-sm"
            >
              {conversation.title}
            </Link>
            <Button
              size="icon"
              variant="ghost"
              className="mr-1 opacity-0 group-hover:opacity-100"
              onClick={() => onDelete(conversation.id)}
              aria-label="Delete chat"
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>
        ))}
      </div>
      <div className="border-t border-[var(--color-border)] p-2">
        <div className="grid grid-cols-3 gap-1 rounded-md bg-[var(--color-surface)] p-1">
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
