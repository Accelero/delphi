import { MessageSquarePlus, Trash2 } from "lucide-react";
import type { ConversationSummary } from "../../lib/types";
import { Button } from "../ui/button";

export function ConversationSidebar({
  conversations,
  activeId,
  onCreate,
  onSelect,
  onDelete
}: {
  conversations: ConversationSummary[];
  activeId: string | null;
  onCreate: () => void;
  onSelect: (id: string) => void;
  onDelete: (id: string) => void;
}) {
  return (
    <aside className="flex h-full w-72 shrink-0 flex-col border-r border-stone-200 bg-stone-50">
      <div className="flex h-14 items-center justify-between border-b border-stone-200 px-3">
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
                ? "group flex items-center rounded-md bg-white shadow-sm"
                : "group flex items-center rounded-md hover:bg-white"
            }
          >
            <button
              className="min-w-0 flex-1 truncate px-3 py-2 text-left text-sm"
              onClick={() => onSelect(conversation.id)}
            >
              {conversation.title}
            </button>
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
