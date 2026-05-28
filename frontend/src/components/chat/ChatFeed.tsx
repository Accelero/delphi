import { ArrowDown } from "lucide-react";
import {
  CSSProperties,
  ReactNode,
  Ref,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState
} from "react";
import type { MessageDto } from "../../lib/types";
import { cn } from "../../lib/utils";
import { Button } from "../ui/button";
import { MessageBody } from "./MessageBody";

export type ChatTurn = {
  id: string;
  messages: MessageDto[];
};

export type ChatMessageRenderContext = {
  isStreaming: boolean;
  isLastTurn: boolean;
};

export type ChatFeedProps = {
  messages: MessageDto[];
  busy?: boolean;
  showThinking?: boolean;
  notice?: string | null;
  noticeTone?: "muted" | "danger";
  className?: string;
  contentClassName?: string;
  renderMessage?: (message: MessageDto, context: ChatMessageRenderContext) => ReactNode;
};

const LAST_TURN_STYLE: CSSProperties = {
  minHeight: "var(--chat-feed-height)"
};

export function ChatFeed({
  messages,
  busy = false,
  showThinking = false,
  notice,
  noticeTone = "muted",
  className,
  contentClassName,
  renderMessage = defaultRenderMessage
}: ChatFeedProps) {
  const viewportRef = useRef<HTMLDivElement | null>(null);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const lastTurnRef = useRef<HTMLDivElement | null>(null);
  const lastTurnIdRef = useRef<string | null>(null);
  const followRef = useRef(true);
  const [escaped, setEscaped] = useState(false);
  const turns = useMemo(() => groupMessagesIntoTurns(messages), [messages]);
  const lastTurnId = turns.at(-1)?.id ?? null;

  const setFollow = (value: boolean) => {
    followRef.current = value;
    setEscaped(!value);
  };

  useLayoutEffect(() => {
    const viewport = viewportRef.current;
    if (!viewport) return;

    const sync = () => {
      viewport.style.setProperty("--chat-feed-height", `${viewport.clientHeight}px`);
    };

    sync();
    const observer = new ResizeObserver(sync);
    observer.observe(viewport);
    return () => observer.disconnect();
  }, []);

  useLayoutEffect(() => {
    const viewport = viewportRef.current;
    if (!viewport) return;

    if (lastTurnId !== lastTurnIdRef.current) {
      lastTurnIdRef.current = lastTurnId;
      followRef.current = true;
      setEscaped(false);
      lastTurnRef.current?.scrollIntoView({ block: "start" });
      return;
    }

    if (followRef.current) {
      viewport.scrollTop = viewport.scrollHeight - viewport.clientHeight;
    }
  }, [lastTurnId, messages]);

  useEffect(() => {
    const viewport = viewportRef.current;
    const sentinel = sentinelRef.current;
    if (!viewport || !sentinel) return;

    const observer = new IntersectionObserver(
      (entries) => {
        const entry = entries.at(-1);
        if (!entry) return;
        if (entry.isIntersecting) {
          setFollow(true);
        } else if (followRef.current) {
          sentinel.scrollIntoView({ block: "end" });
        }
      },
      { root: viewport, threshold: 0 }
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const viewport = viewportRef.current;
    if (!viewport) return;

    let lastTop = viewport.scrollTop;
    const onScroll = () => {
      const top = viewport.scrollTop;
      if (top < lastTop - 1) setFollow(false);
      lastTop = top;
    };

    viewport.addEventListener("scroll", onScroll, { passive: true });
    return () => viewport.removeEventListener("scroll", onScroll);
  }, []);

  const scrollToBottom = () => {
    sentinelRef.current?.scrollIntoView({ block: "end", behavior: "smooth" });
  };

  return (
    <section className={cn("relative min-h-0 flex-1", className)}>
      <div
        ref={viewportRef}
        className="absolute inset-0 overflow-y-auto outline-none [overflow-anchor:none]"
      >
        <div className={cn("mx-auto flex max-w-3xl flex-col px-5 pt-8", contentClassName)}>
          {turns.map((turn, index) => {
            const isLastTurn = index === turns.length - 1;
            return (
              <ChatTurnContainer
                key={turn.id}
                turn={turn}
                busy={busy}
                isLastTurn={isLastTurn}
                ref={isLastTurn ? lastTurnRef : undefined}
                renderMessage={renderMessage}
              >
                {showThinking ? <ThinkingRow /> : null}
              </ChatTurnContainer>
            );
          })}
          <div ref={sentinelRef} aria-hidden className="h-px" />
        </div>
      </div>
      {notice ? (
        <div
          className={cn(
            "absolute bottom-4 left-5 text-sm",
            noticeTone === "danger"
              ? "text-[var(--color-danger)]"
              : "text-[var(--color-text-muted)]"
          )}
        >
          {notice}
        </div>
      ) : null}
      {escaped ? (
        <Button
          size="icon"
          variant="outline"
          className="absolute bottom-[var(--chat-scroll-button-offset)] left-1/2 -translate-x-1/2 rounded-full"
          onClick={scrollToBottom}
          aria-label="Scroll to bottom"
        >
          <ArrowDown className="h-4 w-4" />
        </Button>
      ) : null}
    </section>
  );
}

function ChatTurnContainer({
  turn,
  busy,
  isLastTurn,
  renderMessage,
  children,
  ref
}: {
  turn: ChatTurn;
  busy: boolean;
  isLastTurn: boolean;
  renderMessage: (message: MessageDto, context: ChatMessageRenderContext) => ReactNode;
  children?: ReactNode;
  ref?: Ref<HTMLDivElement>;
}) {
  return (
    <div
      ref={ref}
      className="flex flex-col gap-6 pb-6 scroll-mt-0"
      style={isLastTurn ? LAST_TURN_STYLE : undefined}
    >
      {turn.messages.map((message) => (
        <div key={message.id} className="flex items-center py-2">
          {renderMessage(message, {
            isStreaming: message.id === "assistant-live" && busy,
            isLastTurn
          })}
        </div>
      ))}
      {isLastTurn ? children : null}
    </div>
  );
}

export function ChatMessageRow({
  message,
  streaming
}: {
  message: MessageDto;
  streaming: boolean;
}) {
  return (
    <div className={message.role === "user" ? "flex w-full justify-end" : "w-full"}>
      <div
        className={
          message.role === "user"
            ? "max-w-[80%] rounded-lg bg-[var(--color-primary)] px-4 py-3 text-sm leading-6 text-[var(--color-primary-text)]"
            : "max-w-none text-[var(--color-text)]"
        }
      >
        {message.role === "assistant" ? (
          <>
            <MessageBody
              content={message.content}
              streaming={streaming}
              citations={message.citations}
            />
            {message.interrupted ? (
              <div className="mt-2 text-xs text-[var(--color-text-muted)]">Interrupted</div>
            ) : null}
          </>
        ) : (
          message.content
        )}
      </div>
    </div>
  );
}

export function groupMessagesIntoTurns(messages: MessageDto[]): ChatTurn[] {
  const turns: ChatTurn[] = [];
  for (const message of messages) {
    const currentTurn = turns.at(-1);
    if (message.turn_id) {
      if (!currentTurn || currentTurn.id !== message.turn_id) {
        turns.push({ id: message.turn_id, messages: [message] });
      } else {
        currentTurn.messages.push(message);
      }
    } else if (message.role === "user" || !currentTurn) {
      const turnId = message.id;
      turns.push({ id: turnId, messages: [message] });
    } else {
      currentTurn.messages.push(message);
    }
  }
  return turns;
}

function defaultRenderMessage(message: MessageDto, context: ChatMessageRenderContext) {
  return <ChatMessageRow message={message} streaming={context.isStreaming} />;
}

function ThinkingRow() {
  return <div className="pb-8 text-sm text-[var(--color-text-muted)]">Thinking...</div>;
}
