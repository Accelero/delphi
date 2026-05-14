/**
 * Reusable chat surface.
 *
 * Scroll model — chatgpt-style "per-turn min-height" approach:
 *
 *   Messages are grouped into *turns*: a user message followed by 0+ assistant
 *   replies. Each turn renders inside a <div data-turn-id>. The *last* turn is
 *   given `min-height = scroll-viewport height`, which means:
 *
 *     - Right after submit, scrollIntoView({block:'start'}) on the new turn
 *       puts the user message at the top of the viewport. The min-height
 *       reserves the rest of the viewport as empty space below it — exactly
 *       the area the assistant will fill.
 *     - As the assistant streams, content grows *inside* the last turn. While
 *       content stays under the min-height, the turn's outer height doesn't
 *       grow → no scrolling. User message stays pinned at the top.
 *     - Once assistant content exceeds the viewport, the turn grows past
 *       min-height, total scroll height grows, and the auto-follow kicks in
 *       to keep the latest token at the viewport bottom.
 *     - Old turns (no longer last) lose their min-height and collapse to
 *       natural height — turns compact together with no extra padding.
 *
 *   Auto-follow is just "if user is within 30px of the bottom, keep them
 *   pinned to the bottom on each render." Browser-native scrollIntoView and
 *   normal scrolling do the rest. No custom spacer math.
 */

import { useChat } from "@ai-sdk/react";
import { useQueryClient } from "@tanstack/react-query";
import type { ChatStatus } from "ai";
import { ArrowDownIcon, CheckIcon, CopyIcon } from "lucide-react";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";

import {
  conversationKeyFor,
  conversationsKey,
} from "@/hooks/useConversations";

import {
  Message,
  MessageAction,
  MessageActions,
  MessageContent,
} from "@/components/ai-elements/message";
import {
  PromptInput,
  PromptInputBody,
  PromptInputFooter,
  PromptInputSubmit,
  PromptInputTextarea,
  PromptInputTools,
  type PromptInputMessage,
} from "@/components/ai-elements/prompt-input";
import { Button } from "@/components/ui/button";
import { Spinner } from "@/components/ui/spinner";
import { cn } from "@/lib/utils";

import { MessageBody, type CitationEntry } from "./MessageBody";

type InitialMessage = {
  id: string;
  role: string;
  content: string;
};

export type ChatProps = {
  api: string;
  emptyTitle?: string;
  emptyDescription?: string;
  placeholder?: string;
  className?: string;
  /** Pre-populate the chat with persisted messages. Shape matches the
   *  AI SDK's `Message` (id + role + content). Optional. */
  initialMessages?: InitialMessage[];
  /** When set, `["conversations"]` and the per-conversation cache are
   *  invalidated after each completed turn so the sidebar picks up
   *  freshly-persisted messages and any auto-generated title. */
  sessionKey?: string;
};

function stripThink(text: string): string {
  return text
    .replace(/<think>[\s\S]*?<\/think>/g, "")
    .replace(/<think>[\s\S]*$/, "")
    .trim();
}

function CopyMessageAction({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const onClick = async () => {
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      // clipboard blocked (insecure context, perms) — silent
    }
  };
  return (
    <MessageAction
      tooltip={copied ? "Copied" : "Copy"}
      label="Copy message"
      onClick={onClick}
      disabled={!text}
    >
      {copied ? (
        <CheckIcon className="size-3.5" />
      ) : (
        <CopyIcon className="size-3.5" />
      )}
    </MessageAction>
  );
}

type ChatMessage = {
  id: string;
  role: "user" | "assistant" | "system" | "data";
  content: string;
};

type Turn = {
  id: string;
  messages: ChatMessage[];
};

/** Group flat message list into turns: each user message starts a new turn,
 *  any non-user messages append to the current turn. */
function groupTurns(messages: ChatMessage[]): Turn[] {
  const turns: Turn[] = [];
  for (const m of messages) {
    if (m.role === "user" || turns.length === 0) {
      turns.push({ id: m.id, messages: [m] });
    } else {
      turns[turns.length - 1].messages.push(m);
    }
  }
  return turns;
}

export function Chat({
  api,
  emptyTitle = "No messages yet",
  emptyDescription = "Send a message to start the conversation.",
  placeholder = "Type a message…",
  className,
  initialMessages,
  sessionKey,
}: ChatProps) {
  const queryClient = useQueryClient();
  const { messages, append, isLoading, error, stop, data } = useChat({
    api,
    initialMessages: initialMessages as never,
  });

  // After a turn completes (streaming → ready), invalidate the
  // conversation caches so the sidebar reflects any auto-generated
  // title and the per-conversation cache picks up the just-persisted
  // assistant message.
  const wasLoadingRef = useRef(false);
  useEffect(() => {
    if (wasLoadingRef.current && !isLoading && sessionKey) {
      queryClient.invalidateQueries({ queryKey: conversationsKey });
      queryClient.invalidateQueries({
        queryKey: conversationKeyFor(sessionKey),
      });
    }
    wasLoadingRef.current = isLoading;
  }, [isLoading, sessionKey, queryClient]);

  // RAG v1: the backend streams a `citations` data block at the head of
  // each chat response. `useChat` collects all data records into `data`;
  // we look for the most recent `{ type: 'citations', chunks: [...] }`
  // entry and apply it to the tail assistant message so its `[N]`
  // markers can resolve to deep links into the PDF viewer.
  const citations: CitationEntry[] = useMemo(() => {
    if (!Array.isArray(data)) return [];
    for (let i = data.length - 1; i >= 0; i--) {
      const entry = data[i] as unknown;
      if (
        entry &&
        typeof entry === "object" &&
        (entry as { type?: string }).type === "citations"
      ) {
        const chunks = (entry as { chunks?: CitationEntry[] }).chunks;
        if (Array.isArray(chunks)) return chunks;
      }
    }
    return [];
  }, [data]);

  const status: ChatStatus = error
    ? "error"
    : isLoading
      ? "streaming"
      : "ready";

  const handleSubmit = (msg: PromptInputMessage) => {
    const text = msg.text.trim();
    if (!text) return;
    void append({ role: "user", content: text });
  };

  const turns = useMemo(
    () => groupTurns(messages as ChatMessage[]),
    [messages],
  );
  const lastTurnId = turns[turns.length - 1]?.id ?? null;
  const lastTurn = turns[turns.length - 1];
  const tail = lastTurn?.messages[lastTurn.messages.length - 1];
  const showThinking = isLoading && (!tail || tail.role === "user");

  // ------------------------------------------------------------------
  // Scroll machinery — IntersectionObserver on a sentinel div.
  //
  //   The sentinel is the last child of the scroll content. When it's in
  //   view, the user is "at the bottom" and `follow = true`. When it leaves
  //   view *because content grew below it* (still want to follow), we snap
  //   the sentinel back into view. When it leaves view *because the user
  //   scrolled up*, the wheel/touch handler flips `follow = false` first,
  //   so we don't fight the user.
  // ------------------------------------------------------------------
  const scrollRef = useRef<HTMLDivElement>(null);
  const sentinelRef = useRef<HTMLDivElement>(null);
  const lastTurnIdRef = useRef<string | null>(null);
  const followRef = useRef(true);
  const [escaped, setEscaped] = useState(false);
  const [viewportH, setViewportH] = useState(0);

  const setFollow = (v: boolean) => {
    followRef.current = v;
    setEscaped(!v);
  };

  // Track viewport height so we can apply it as min-height to the last turn.
  useLayoutEffect(() => {
    const c = scrollRef.current;
    if (!c) return;
    const sync = () => setViewportH(c.clientHeight);
    sync();
    const ro = new ResizeObserver(sync);
    ro.observe(c);
    return () => ro.disconnect();
  }, []);

  // Synchronous snap-to-bottom on every render. Runs before the browser
  // paints, so token arrivals don't show a single frame of "scrollbar drifted
  // up" before the IO catches up. Cheap — scrollTop assignment is a no-op
  // when value is unchanged.
  useLayoutEffect(() => {
    if (!followRef.current) return;
    const c = scrollRef.current;
    if (!c) return;
    c.scrollTop = c.scrollHeight - c.clientHeight;
  });

  // IntersectionObserver: re-engages follow when the user scrolls back to the
  // bottom (sentinel re-enters view). Also a backup for post-paint growth
  // (Shiki highlight, web font, etc.) that the layout effect didn't see.
  useEffect(() => {
    const c = scrollRef.current;
    const sentinel = sentinelRef.current;
    if (!c || !sentinel) return;
    const io = new IntersectionObserver(
      (entries) => {
        const e = entries[entries.length - 1];
        if (e.isIntersecting) {
          setFollow(true);
        } else if (followRef.current) {
          // Async fallback for layout shifts after paint.
          sentinel.scrollIntoView({ block: "end" });
        }
      },
      { root: c, threshold: 0 },
    );
    io.observe(sentinel);
    return () => io.disconnect();
  }, []);

  // User-initiated escape detection: compare scrollTop deltas. Only an
  // *actual upward* scroll (decreasing scrollTop) flips follow off — this
  // is more reliable than wheel/touch events, which fire spuriously on
  // trackpad inertia and don't distinguish our programmatic scrolls.
  useEffect(() => {
    const c = scrollRef.current;
    if (!c) return;
    let lastTop = c.scrollTop;
    const onScroll = () => {
      const t = c.scrollTop;
      if (t < lastTop - 1) setFollow(false);
      lastTop = t;
    };
    c.addEventListener("scroll", onScroll, { passive: true });
    return () => c.removeEventListener("scroll", onScroll);
  }, []);

  // On a new turn: re-engage follow and smooth-scroll the new turn's top
  // edge to the viewport top. The IO will take over from there.
  useEffect(() => {
    if (!lastTurnId || lastTurnId === lastTurnIdRef.current) return;
    lastTurnIdRef.current = lastTurnId;
    setFollow(true);
    requestAnimationFrame(() => {
      const c = scrollRef.current;
      if (!c) return;
      const el = c.querySelector(
        `[data-turn-id="${lastTurnId}"]`,
      ) as HTMLElement | null;
      if (!el) return;
      // Instant, not smooth — the layout effect already snaps to bottom
      // synchronously, and the two scrolls fighting causes a hitch.
      el.scrollIntoView({ block: "start" });
    });
  }, [lastTurnId]);

  const onScrollToBottom = () => {
    sentinelRef.current?.scrollIntoView({
      behavior: "smooth",
      block: "end",
    });
    // IO will fire intersecting=true after the smooth scroll completes,
    // re-engaging follow.
  };

  return (
    <div className={cn("flex flex-col h-full min-h-0", className)}>
      <div className="relative flex-1 min-h-0">
        <div
          ref={scrollRef}
          role="log"
          tabIndex={0}
          className="absolute inset-0 overflow-y-auto outline-none [overflow-anchor:none]"
        >
          {/* px+pt only; bottom padding would push scrollHeight past the
              auto-follow target and shift the anchored user message off-top. */}
          <div className="flex flex-col px-4 pt-4">
            {turns.length === 0 && !showThinking && (
              <div className="flex flex-col items-center justify-center gap-2 py-16 text-center text-sm text-muted-foreground">
                <h3 className="font-medium text-foreground">{emptyTitle}</h3>
                <p>{emptyDescription}</p>
              </div>
            )}

            {turns.map((turn, idx) => {
              const isLast = idx === turns.length - 1;
              return (
                <div
                  key={turn.id}
                  data-turn-id={turn.id}
                  className="flex flex-col gap-6 pb-6 scroll-mt-0"
                  style={isLast ? { minHeight: viewportH } : undefined}
                >
                  {turn.messages.map((m, mi) => {
                    const isTailAssistant =
                      isLast &&
                      m.role === "assistant" &&
                      mi === turn.messages.length - 1;
                    const copyText = stripThink(m.content);
                    return (
                      <Message key={m.id} from={m.role} data-msg-id={m.id}>
                        <MessageContent>
                          <MessageBody
                            content={m.content}
                            isStreaming={isLoading && isTailAssistant}
                            citations={
                              m.role === "assistant" ? citations : undefined
                            }
                          />
                        </MessageContent>
                        <MessageActions
                          className={cn(
                            "pt-1 opacity-0 group-hover:opacity-100 transition-opacity duration-150",
                            m.role === "user"
                              ? "justify-end"
                              : "justify-start",
                          )}
                        >
                          <CopyMessageAction text={copyText} />
                        </MessageActions>
                      </Message>
                    );
                  })}

                  {isLast && showThinking && (
                    <Message from="assistant" key="__thinking__">
                      <MessageContent>
                        <div className="flex items-center gap-2 text-base leading-7 text-muted-foreground">
                          <Spinner className="size-4" />
                          <span>Thinking…</span>
                        </div>
                      </MessageContent>
                    </Message>
                  )}
                </div>
              );
            })}

            {error && (
              <div className="rounded-md p-3 border border-red-500/50 text-sm text-red-500">
                {error.message}
              </div>
            )}

            {/* Sentinel — last child of scroll content. Its IO intersection
                with the scroll container is the source of truth for "user
                is at bottom" / follow-streaming. */}
            <div ref={sentinelRef} aria-hidden className="h-px" />
          </div>
        </div>

        {/* Floating "scroll to bottom" — visible when user has scrolled away. */}
        {escaped && (
          <Button
            type="button"
            size="icon"
            variant="outline"
            onClick={onScrollToBottom}
            aria-label="Scroll to bottom"
            className="absolute bottom-3 left-1/2 -translate-x-1/2 size-8 rounded-full shadow-md"
          >
            <ArrowDownIcon className="size-4" />
          </Button>
        )}
      </div>

      <PromptInput onSubmit={handleSubmit} className="mt-2">
        <PromptInputBody>
          <PromptInputTextarea
            placeholder={placeholder}
            className="field-sizing-content min-h-11 max-h-[max(5.5rem,33vh)] overflow-y-auto"
          />
        </PromptInputBody>
        <PromptInputFooter>
          <PromptInputTools />
          <PromptInputSubmit status={status} onStop={stop} />
        </PromptInputFooter>
      </PromptInput>
    </div>
  );
}
