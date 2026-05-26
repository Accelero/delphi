import { ArrowDown, LoaderCircle, Send, Square } from "lucide-react";
import { FormEvent, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { ulid } from "ulid";
import { useChatSocket } from "../../hooks/useChatSocket";
import { api } from "../../lib/api";
import type { ConversationDetail, MessageDto } from "../../lib/types";
import { Button } from "../ui/button";
import { Textarea } from "../ui/textarea";
import { MessageBody } from "./MessageBody";

export function ChatPane({
  conversation,
  onRefresh
}: {
  conversation: ConversationDetail | null;
  onRefresh: () => void;
}) {
  const [draft, setDraft] = useState("");
  const viewportRef = useRef<HTMLDivElement | null>(null);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const lastScrollTopRef = useRef(0);
  const [follow, setFollow] = useState(true);
  const { messages, status, realtimeStatus, error, lastMessageId, setStatus } = useChatSocket(
    conversation?.id ?? null,
    conversation?.messages ?? [],
    {
      onResync: onRefresh,
      onTerminalRefresh: onRefresh
    }
  );
  const turns = useMemo(() => groupTurns(messages), [messages]);
  const busy = status === "submitted" || status === "streaming" || status === "stopping";
  const stopping = status === "stopping";

  useEffect(() => {
    const viewport = viewportRef.current;
    const sentinel = sentinelRef.current;
    if (!viewport || !sentinel) return;
    const observer = new IntersectionObserver(
      ([entry]) => setFollow(entry.isIntersecting),
      { root: viewport, threshold: 0 }
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, []);

  useLayoutEffect(() => {
    const viewport = viewportRef.current;
    if (viewport && follow) {
      viewport.scrollTop = viewport.scrollHeight - viewport.clientHeight;
    }
  });

  const onScroll = () => {
    const viewport = viewportRef.current;
    if (!viewport) return;
    if (viewport.scrollTop < lastScrollTopRef.current - 1) {
      setFollow(false);
    }
    lastScrollTopRef.current = viewport.scrollTop;
  };

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!conversation || !draft.trim() || busy) return;
    const text = draft.trim();
    setDraft("");
    setStatus("submitted");
    try {
      await api.submitTurn(conversation.id, {
        user_message_id: ulid(),
        turn_id: ulid(),
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
    return <main className="flex flex-1 items-center justify-center text-sm text-stone-500" />;
  }

  return (
    <main className="flex h-full min-w-0 flex-1 flex-col bg-white">
      <div className="flex h-14 items-center border-b border-stone-200 px-5">
        <h1 className="truncate text-sm font-semibold">{conversation.title}</h1>
      </div>
      <div className="relative min-h-0 flex-1">
        <div
          ref={viewportRef}
          onScroll={onScroll}
          className="absolute inset-0 overflow-y-auto outline-none [overflow-anchor:none]"
        >
          <div className="mx-auto max-w-3xl px-5 pt-8">
            {turns.map((turn, index) => (
              <section
                key={turn.id}
                data-turn-id={turn.id}
                className={index === turns.length - 1 ? "min-h-[calc(100vh-12rem)] pb-6" : "pb-8"}
              >
                {turn.messages.map((message) => (
                  <MessageRow
                    key={message.id}
                    message={message}
                    streaming={message.id === "assistant-live" && busy}
                  />
                ))}
              </section>
            ))}
            {busy && messages.at(-1)?.role === "user" ? (
              <div className="pb-8 text-sm text-stone-500">Thinking...</div>
            ) : null}
            {realtimeStatus === "reconnecting" || realtimeStatus === "disconnected" ? (
              <div className="pb-8 text-sm text-stone-500">Reconnecting...</div>
            ) : null}
            {error ? <div className="pb-8 text-sm text-red-600">{error}</div> : null}
            <div ref={sentinelRef} aria-hidden className="h-px" />
          </div>
        </div>
        {!follow ? (
          <Button
            size="icon"
            variant="outline"
            className="absolute bottom-4 left-1/2 -translate-x-1/2 rounded-full bg-white"
            onClick={() => sentinelRef.current?.scrollIntoView({ block: "end", behavior: "smooth" })}
            aria-label="Scroll to bottom"
          >
            <ArrowDown className="h-4 w-4" />
          </Button>
        ) : null}
      </div>
      <form onSubmit={submit} className="border-t border-stone-200 p-4">
        <div className="mx-auto flex max-w-3xl gap-2">
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
          />
          {busy ? (
            <Button
              type="button"
              size="icon"
              variant="destructive"
              onClick={stop}
              disabled={stopping}
              aria-label={stopping ? "Stopping" : "Stop"}
            >
              {stopping ? (
                <LoaderCircle className="h-4 w-4 animate-spin" />
              ) : (
                <Square className="h-4 w-4" />
              )}
            </Button>
          ) : (
            <Button type="submit" size="icon" disabled={!draft.trim()} aria-label="Send">
              <Send className="h-4 w-4" />
            </Button>
          )}
        </div>
      </form>
    </main>
  );
}

function MessageRow({ message, streaming }: { message: MessageDto; streaming: boolean }) {
  return (
    <div className={message.role === "user" ? "mb-5 flex justify-end" : "mb-5"}>
      <div
        className={
          message.role === "user"
            ? "max-w-[80%] rounded-lg bg-stone-950 px-4 py-3 text-sm leading-6 text-white"
            : "max-w-none text-stone-900"
        }
      >
        {message.role === "assistant" ? (
          <>
            <MessageBody content={message.content} streaming={streaming} />
            {message.interrupted ? (
              <div className="mt-2 text-xs text-stone-500">Interrupted</div>
            ) : null}
          </>
        ) : (
          message.content
        )}
      </div>
    </div>
  );
}

function groupTurns(messages: MessageDto[]) {
  const turns: { id: string; messages: MessageDto[] }[] = [];
  for (const message of messages) {
    if (message.role === "user" || turns.length === 0) {
      turns.push({ id: message.turn_id ?? message.id, messages: [message] });
    } else {
      turns[turns.length - 1].messages.push(message);
    }
  }
  return turns;
}
