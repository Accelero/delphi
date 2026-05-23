/**
 * Sidebar for the corpus-chat surface: lists the user's conversations,
 * with "New chat", rename (inline edit), and delete (with confirm).
 *
 * Selection state is owned by the URL — `/corpus/$sessionId` — so the
 * sidebar only needs to read the current `sessionId` route param to
 * highlight the active row.
 */

import { useNavigate } from "@tanstack/react-router";
import { Check, PencilLine, Plus, Trash2, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  useConversations,
  useCreateConversation,
  useDeleteConversation,
  useRenameConversation,
} from "@/hooks/useConversations";
import { conversationKey, type Conversation } from "@/lib/api";
import { cn } from "@/lib/utils";

type Props = {
  activeKey: string;
};

export function ConversationSidebar({ activeKey }: Props) {
  const navigate = useNavigate();
  const list = useConversations();
  const create = useCreateConversation();
  const remove = useDeleteConversation();

  const onNewChat = async () => {
    try {
      const c = await create.mutateAsync();
      navigate({
        to: "/corpus/$sessionId",
        params: { sessionId: conversationKey(c.id) },
      });
    } catch {
      // Mutation surfaces in `create.error`; non-fatal — user can retry.
    }
  };

  const onDelete = async (key: string) => {
    if (!window.confirm("Delete this conversation?")) return;
    try {
      await remove.mutateAsync({ key });
      if (key === activeKey) {
        // Decide the landing spot deterministically from the known list
        // (most-recent-first) rather than bouncing through /corpus and
        // racing a stale cache. Most-recent remaining, or the draft chat
        // when none are left.
        const remaining = (list.data ?? []).filter(
          (c) => conversationKey(c.id) !== key,
        );
        if (remaining.length > 0) {
          navigate({
            to: "/corpus/$sessionId",
            params: { sessionId: conversationKey(remaining[0].id) },
          });
        } else {
          navigate({ to: "/corpus" });
        }
      }
    } catch {
      // Same as create — silent here; cache invalidation reflects state.
    }
  };

  return (
    <aside className="w-64 shrink-0 border-r border-[var(--border)] flex flex-col h-full">
      <div className="p-3 border-b border-[var(--border)]">
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="w-full justify-start gap-2"
          onClick={onNewChat}
          disabled={create.isPending}
        >
          <Plus className="size-4" />
          New chat
        </Button>
      </div>

      <div className="flex-1 overflow-y-auto p-2">
        {list.isLoading && (
          <div className="space-y-1.5">
            {Array.from({ length: 3 }).map((_, i) => (
              <div
                key={i}
                className="h-8 rounded bg-[var(--muted)] animate-pulse"
              />
            ))}
          </div>
        )}

        {list.isError && (
          <p className="text-xs text-destructive px-2 py-1">
            Failed to load conversations.
          </p>
        )}

        {list.isSuccess && list.data.length === 0 && (
          <p className="text-xs text-muted-foreground px-2 py-1">
            No conversations yet.
          </p>
        )}

        {list.data?.map((c) => (
          <ConversationRow
            key={c.id}
            conversation={c}
            isActive={conversationKey(c.id) === activeKey}
            onDelete={() => onDelete(conversationKey(c.id))}
          />
        ))}
      </div>
    </aside>
  );
}

function ConversationRow({
  conversation,
  isActive,
  onDelete,
}: {
  conversation: Conversation;
  isActive: boolean;
  onDelete: () => void;
}) {
  const navigate = useNavigate();
  const rename = useRenameConversation();
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(conversation.title ?? "");
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (editing) {
      inputRef.current?.focus();
      inputRef.current?.select();
    }
  }, [editing]);

  const key = conversationKey(conversation.id);
  const title = conversation.title ?? "Untitled";

  const startEditing = (e: React.MouseEvent) => {
    e.stopPropagation();
    setDraft(conversation.title ?? "");
    setEditing(true);
  };

  const commit = async () => {
    const trimmed = draft.trim();
    if (!trimmed || trimmed === conversation.title) {
      setEditing(false);
      return;
    }
    try {
      await rename.mutateAsync({ key, title: trimmed.slice(0, 200) });
    } catch {
      // Same pattern as create/delete — surface via cache.
    }
    setEditing(false);
  };

  const cancel = () => {
    setEditing(false);
    setDraft(conversation.title ?? "");
  };

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={() => {
        if (editing) return;
        navigate({
          to: "/corpus/$sessionId",
          params: { sessionId: key },
        });
      }}
      onKeyDown={(e) => {
        if (editing) return;
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          navigate({
            to: "/corpus/$sessionId",
            params: { sessionId: key },
          });
        }
      }}
      className={cn(
        "group flex items-center gap-1 px-2 py-1.5 rounded cursor-pointer text-sm",
        "hover:bg-[var(--muted)]",
        isActive && "bg-[var(--muted)]",
      )}
    >
      {editing ? (
        <>
          <Input
            ref={inputRef}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onClick={(e) => e.stopPropagation()}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                void commit();
              } else if (e.key === "Escape") {
                e.preventDefault();
                cancel();
              }
            }}
            onBlur={() => void commit()}
            className="h-7 text-sm"
            maxLength={200}
            aria-label="Conversation title"
          />
          <Button
            type="button"
            size="icon"
            variant="ghost"
            className="size-7"
            onMouseDown={(e) => {
              // Prevent input blur from firing before the click.
              e.preventDefault();
            }}
            onClick={(e) => {
              e.stopPropagation();
              void commit();
            }}
            aria-label="Save title"
          >
            <Check className="size-3.5" />
          </Button>
          <Button
            type="button"
            size="icon"
            variant="ghost"
            className="size-7"
            onMouseDown={(e) => e.preventDefault()}
            onClick={(e) => {
              e.stopPropagation();
              cancel();
            }}
            aria-label="Cancel rename"
          >
            <X className="size-3.5" />
          </Button>
        </>
      ) : (
        <>
          <span
            className={cn(
              "flex-1 truncate",
              !conversation.title && "italic text-muted-foreground",
            )}
            title={title}
          >
            {title}
          </span>
          <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
            <Button
              type="button"
              size="icon"
              variant="ghost"
              className="size-6"
              onClick={startEditing}
              aria-label="Rename"
            >
              <PencilLine className="size-3.5" />
            </Button>
            <Button
              type="button"
              size="icon"
              variant="ghost"
              className="size-6"
              onClick={(e) => {
                e.stopPropagation();
                onDelete();
              }}
              aria-label="Delete"
            >
              <Trash2 className="size-3.5" />
            </Button>
          </div>
        </>
      )}
    </div>
  );
}
