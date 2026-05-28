import { ArrowUp, LoaderCircle, Square } from "lucide-react";
import { type CSSProperties, FormEvent, useLayoutEffect, useRef, useState } from "react";
import { ulid } from "ulid";
import { useChatSocket } from "../../hooks/useChatSocket";
import { api } from "../../lib/api";
import type { ConversationDetail } from "../../lib/types";
import { Button } from "../ui/button";
import { Textarea } from "../ui/textarea";
import { ChatFeed } from "./ChatFeed";

export function ChatPane({
  conversation,
  onRefresh,
  onTitleUpdated
}: {
  conversation: ConversationDetail | null;
  onRefresh: () => void;
  onTitleUpdated: (title: string) => void;
}) {
  const [draft, setDraft] = useState("");
  const shellRef = useRef<HTMLElement | null>(null);
  const composerRef = useRef<HTMLDivElement | null>(null);
  const {
    messages,
    status,
    realtimeStatus,
    error,
    lastMessageId,
    setStatus
  } = useChatSocket(conversation?.id ?? null, conversation?.messages ?? [], {
    onResync: onRefresh,
    onTerminalRefresh: onRefresh,
    onTitleUpdated
  });
  const busy = status === "submitted" || status === "streaming" || status === "stopping";
  const stopping = status === "stopping";
  const showThinking = busy && messages.at(-1)?.role === "user";
  const statusNotice =
    realtimeStatus === "reconnecting" || realtimeStatus === "disconnected"
      ? "Reconnecting..."
      : error;

  useLayoutEffect(() => {
    const shell = shellRef.current;
    const composer = composerRef.current;
    if (!shell || !composer) return;

    const sync = () => {
      const shellRect = shell.getBoundingClientRect();
      const composerRect = composer.getBoundingClientRect();
      const composerCenterFromBottom =
        shellRect.bottom - (composerRect.top + composerRect.height / 2);
      shell.style.setProperty(
        "--chat-composer-center-offset",
        `${Math.max(0, composerCenterFromBottom)}px`
      );
    };

    sync();
    const observer = new ResizeObserver(sync);
    observer.observe(shell);
    observer.observe(composer);
    return () => observer.disconnect();
  }, []);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!conversation || !draft.trim() || busy) return;
    const text = draft.trim();
    const userMessageId = ulid();
    const turnId = ulid();
    setDraft("");
    setStatus("submitted");
    try {
      await api.submitTurn(conversation.id, {
        user_message_id: userMessageId,
        turn_id: turnId,
        text,
        parent_message_id: lastMessageId
      });
    } catch (err) {
      setDraft(text);
      setStatus("error");
      onRefresh();
    }
  };

  const stop = async () => {
    if (!conversation) return;
    setStatus("stopping");
    try {
      await api.stopTurn(conversation.id);
    } catch {
      setStatus("streaming");
    }
  };

  if (!conversation) {
    return (
      <main className="flex flex-1 items-center justify-center text-sm text-[var(--color-text-muted)]" />
    );
  }

  return (
    <main
      ref={shellRef}
      style={{ "--chat-composer-center-offset": "4rem" } as CSSProperties}
      className="relative flex h-full min-w-0 flex-1 flex-col bg-[var(--color-surface)]"
    >
      <ChatFeed
        messages={messages}
        busy={busy}
        showThinking={showThinking}
        notice={statusNotice}
        noticeTone={error ? "danger" : "muted"}
        className="absolute inset-x-0 top-0 bottom-[var(--chat-composer-center-offset)]"
      />
      <form
        onSubmit={submit}
        className="pointer-events-none absolute inset-x-0 bottom-0 z-20 px-5 pb-4"
      >
        <div
          ref={composerRef}
          className="pointer-events-auto mx-auto flex max-w-3xl items-end rounded-3xl bg-[var(--color-object)] p-2"
        >
          <Textarea
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            placeholder="Ask about your documents"
            onKeyDown={(event) => {
              if (event.key === "Enter" && !event.shiftKey) {
                event.preventDefault();
                event.currentTarget.form?.requestSubmit();
              }
            }}
            className="min-h-20 rounded-none bg-transparent px-3 py-2 focus:ring-0"
          />
          {busy ? (
            <Button
              type="button"
              size="icon"
              variant="destructive"
              onClick={stop}
              disabled={stopping}
              aria-label={stopping ? "Stopping" : "Stop"}
              className="mb-1 shrink-0 rounded-full bg-[var(--color-primary)] text-[var(--color-primary-text)] opacity-100 hover:bg-[var(--color-primary-hover)] disabled:opacity-100"
            >
              {stopping ? (
                <LoaderCircle className="h-4 w-4 animate-spin" />
              ) : (
                <Square className="h-4 w-4" />
              )}
            </Button>
          ) : (
            <Button
              type="submit"
              size="icon"
              disabled={!draft.trim()}
              aria-label="Send"
              className="mb-1 shrink-0 rounded-full bg-[var(--color-primary)] text-[var(--color-primary-text)] opacity-100 hover:bg-[var(--color-primary-hover)] disabled:opacity-100"
            >
              <ArrowUp className="h-4 w-4" />
            </Button>
          )}
        </div>
      </form>
    </main>
  );
}
