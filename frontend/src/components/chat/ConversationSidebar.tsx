import { Link } from "@tanstack/react-router";
import { MessageSquarePlus, Trash2 } from "lucide-react";
import type { ConversationSummary } from "../../lib/types";
import { cn } from "../../lib/utils";
import { Button } from "../ui/button";

export function ConversationSidebar({
  conversations,
  activeId,
  onCreate,
  onDelete,
  className
}: {
  className?: string;
  conversations: ConversationSummary[];
  activeId: string | null;
  onCreate: () => void;
  onDelete: (id: string) => void;
}) {
  return (
    <aside
      className={cn(
        "flex h-full w-72 shrink-0 flex-col border-r border-[var(--color-border)] bg-[var(--color-surface-muted)]",
        className
      )}
    >
      <div className="flex h-14 items-center justify-between border-b border-[var(--color-border)] px-3">
        <div className="text-sm font-semibold">Chats</div>
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
    </aside>
  );
}
