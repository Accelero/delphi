/**
 * `useSessionStream(conversationKey)` — subscribe to the per-session
 * byte log produced by the backend chat worker.
 *
 * Replaces `@ai-sdk/react`'s `useChat` for our split protocol:
 *
 *  - `POST /api/chat/conversations/{key}/messages` is fire-and-forget
 *    (returns 202). The hook calls it from `submit(text)` and
 *    optimistically appends the user message to local state — the
 *    backend persists it synchronously before returning 202, so the
 *    mirror is safe.
 *  - `GET /api/chat/conversations/{key}/stream` is a long-lived
 *    response whose body is the AI SDK data-stream format (`0:` /
 *    `2:` / `3:` / `d:` newline-delimited records). The hook opens
 *    this on mount and tails it for the component's lifetime.
 *  - `POST /api/chat/conversations/{key}/stop` cancels the in-flight
 *    turn. Idempotent — every tab can call it.
 *
 * Multi-tab fan-out is free: every tab that mounts opens its own
 * `/stream` subscription, the backend tees the same bytes to all of
 * them, so two tabs of the same chat see the same live tokens.
 *
 * The hook is intentionally *minimal*. Persisted message history is
 * loaded once by the route loader (`getConversation`) and passed in
 * via `initialMessages`; the hook then layers in-flight text from the
 * stream on top. State that needs to survive HMR / route swaps lives
 * in TanStack Query's cache, not here.
 */

import { useCallback, useEffect, useRef, useState } from "react";

import type { CitationEntry } from "@/components/chat/MessageBody";
import { ApiError, type ChatMessageWire } from "@/lib/api";

/** Status enum shaped like `@ai-sdk/react`'s so callers don't have to
 *  learn a new word. `submitted` is the brief gap between POST and the
 *  first `0:` / `2:` arriving on the stream; UIs typically render the
 *  same "thinking…" placeholder for both `submitted` and `streaming`. */
export type StreamStatus = "ready" | "submitted" | "streaming" | "error";

export type LocalMessage = {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
};

export type UseSessionStreamOptions = {
  /** Persisted history from the conversation GET response. Rendered as
   *  the initial `messages` until the live stream replaces/extends it. */
  initialMessages?: LocalMessage[];
  /** Fires after each completed turn (we saw a `d:` frame). Callers use
   *  it to invalidate sidebar caches so the auto-generated title and
   *  newly-persisted assistant message show up. */
  onTurnEnd?: () => void;
};

export type UseSessionStreamReturn = {
  messages: LocalMessage[];
  status: StreamStatus;
  citations: CitationEntry[];
  error: string | null;
  /** Submit a user message. Optimistically inserts it locally, then
   *  POSTs to `/messages`. Throws on non-2xx. The assistant reply
   *  arrives via the open stream. */
  submit: (text: string) => Promise<void>;
  /** Ask the backend to abort the in-flight turn. The worker's
   *  `proto::finish("stop")` frame closes out the turn for every tab. */
  stop: () => Promise<void>;
};

/* -----------------------------------------------------------------
 * Parser — public so we can unit-test it in isolation. The AI SDK's
 * data-stream format is line-prefixed records:
 *
 *   0:"hello world"\n      ← text delta (JSON-encoded string)
 *   2:[{...}]\n            ← data block (JSON array)
 *   3:"error text"\n       ← error (JSON-encoded string)
 *   d:{"finishReason":"stop"}\n ← finish marker (JSON object)
 *
 * The parser is a tiny incremental machine: feed it `Uint8Array`
 * chunks via `push()`, drain complete records out via `take()`. Lines
 * arrive in arbitrary chunk splits (TCP / TextDecoder boundaries), so
 * the parser buffers partial lines until a `\n`.
 * ----------------------------------------------------------------- */

export type ParsedRecord =
  | { type: "text"; value: string }
  | { type: "data"; value: unknown[] }
  | { type: "error"; value: string }
  | { type: "finish"; value: { finishReason?: string; [k: string]: unknown } };

export class StreamParser {
  private decoder = new TextDecoder("utf-8");
  private buf = "";
  private out: ParsedRecord[] = [];

  /** Feed a chunk. Idempotent on empty input. */
  push(chunk: Uint8Array): void {
    this.buf += this.decoder.decode(chunk, { stream: true });
    this.drainLines();
  }

  /** Drain and return all complete records parsed so far. */
  take(): ParsedRecord[] {
    const o = this.out;
    this.out = [];
    return o;
  }

  /** Flush any in-flight TextDecoder state (call on stream end). After
   *  flush, an incomplete trailing line is discarded — the wire format
   *  requires every record to end in `\n`. */
  flush(): void {
    this.buf += this.decoder.decode();
    this.drainLines();
  }

  private drainLines(): void {
    let nl: number;
    while ((nl = this.buf.indexOf("\n")) !== -1) {
      const line = this.buf.slice(0, nl);
      this.buf = this.buf.slice(nl + 1);
      if (line.length === 0) continue;
      const sep = line.indexOf(":");
      if (sep < 0) continue; // malformed — drop silently, same as the AI SDK
      const tag = line.slice(0, sep);
      const body = line.slice(sep + 1);
      try {
        const parsed = JSON.parse(body);
        switch (tag) {
          case "0":
            if (typeof parsed === "string")
              this.out.push({ type: "text", value: parsed });
            break;
          case "2":
            if (Array.isArray(parsed))
              this.out.push({ type: "data", value: parsed });
            break;
          case "3":
            if (typeof parsed === "string")
              this.out.push({ type: "error", value: parsed });
            break;
          case "d":
            if (parsed && typeof parsed === "object")
              this.out.push({ type: "finish", value: parsed });
            break;
          default:
            // Unknown tag — ignore. AI SDK occasionally adds new ones.
            break;
        }
      } catch {
        // Malformed JSON — drop. The wire format is well-defined; if we
        // see a bad record it's a backend bug, not something to crash
        // the chat over.
      }
    }
  }
}

/* -----------------------------------------------------------------
 * Hook
 * ----------------------------------------------------------------- */

function messagesUrl(key: string): string {
  return `/api/chat/conversations/${encodeURIComponent(key)}/messages`;
}
function streamUrl(key: string): string {
  return `/api/chat/conversations/${encodeURIComponent(key)}/stream`;
}
function stopUrl(key: string): string {
  return `/api/chat/conversations/${encodeURIComponent(key)}/stop`;
}

/** Random id for optimistic / streaming assistant message rows. The
 *  backend's persisted ids replace these on the next history GET. */
function ephemeralId(prefix: string): string {
  return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}

export function useSessionStream(
  conversationKey: string,
  opts: UseSessionStreamOptions = {},
): UseSessionStreamReturn {
  const { initialMessages = [], onTurnEnd } = opts;
  const [messages, setMessages] = useState<LocalMessage[]>(initialMessages);
  const [status, setStatus] = useState<StreamStatus>("ready");
  const [citations, setCitations] = useState<CitationEntry[]>([]);
  const [error, setError] = useState<string | null>(null);

  // Keep the latest onTurnEnd in a ref so the stream loop can call it
  // without resubscribing on every render.
  const onTurnEndRef = useRef(onTurnEnd);
  useEffect(() => {
    onTurnEndRef.current = onTurnEnd;
  }, [onTurnEnd]);

  // Tracking the in-flight assistant message id is the cleanest way to
  // append text deltas without scanning the message array on every
  // record. Reset on each `d:` (turn boundary).
  const inFlightAssistantIdRef = useRef<string | null>(null);

  /**
   * Open the stream and consume it until the component unmounts.
   * `initialMessages` is captured by ref so it can be updated by
   * `setMessages` without resubscribing. We resubscribe ONLY when
   * `conversationKey` changes.
   */
  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();

    const run = async () => {
      try {
        const res = await fetch(streamUrl(conversationKey), {
          credentials: "same-origin",
          signal: controller.signal,
        });
        if (!res.ok || !res.body) {
          if (res.status === 401) {
            // Let the global handler kick the OIDC redirect; the route
            // will be remounted afterwards and the stream re-opens.
            return;
          }
          throw new ApiError(res.status, `stream open: ${res.status}`);
        }
        const reader = res.body.getReader();
        const parser = new StreamParser();

        while (!cancelled) {
          const { value, done } = await reader.read();
          if (done) break;
          if (value) parser.push(value);
          for (const rec of parser.take()) applyRecord(rec);
        }
        parser.flush();
        for (const rec of parser.take()) applyRecord(rec);
      } catch (e) {
        if (cancelled) return;
        if (e instanceof DOMException && e.name === "AbortError") return;
        setError(e instanceof Error ? e.message : "stream error");
        setStatus("error");
      }
    };

    const applyRecord = (rec: ParsedRecord) => {
      switch (rec.type) {
        case "text": {
          setStatus("streaming");
          setMessages((prev) => {
            const id = inFlightAssistantIdRef.current;
            if (id) {
              return prev.map((m) =>
                m.id === id ? { ...m, content: m.content + rec.value } : m,
              );
            }
            const fresh: LocalMessage = {
              id: ephemeralId("a"),
              role: "assistant",
              content: rec.value,
            };
            inFlightAssistantIdRef.current = fresh.id;
            return [...prev, fresh];
          });
          break;
        }
        case "data": {
          // RAG v1: the worker emits one `2:` block per turn carrying
          // citations. Replace local citations wholesale; if a future
          // turn lacks any, citations clear on the next `d:`.
          for (const entry of rec.value) {
            if (
              entry &&
              typeof entry === "object" &&
              (entry as { type?: string }).type === "citations"
            ) {
              const chunks = (entry as { chunks?: CitationEntry[] }).chunks;
              if (Array.isArray(chunks)) setCitations(chunks);
            }
          }
          break;
        }
        case "error": {
          setError(rec.value);
          setStatus("error");
          break;
        }
        case "finish": {
          inFlightAssistantIdRef.current = null;
          setStatus("ready");
          onTurnEndRef.current?.();
          break;
        }
      }
    };

    void run();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [conversationKey]);

  const submit = useCallback(
    async (text: string) => {
      const trimmed = text.trim();
      if (!trimmed) return;
      // Optimistic insert — the backend persists the user message
      // synchronously before returning 202, so the local mirror won't
      // diverge unless the POST itself fails (rolled back below).
      const userId = ephemeralId("u");
      setMessages((prev) => [
        ...prev,
        { id: userId, role: "user", content: trimmed },
      ]);
      setStatus("submitted");
      setError(null);
      inFlightAssistantIdRef.current = null;

      try {
        const res = await fetch(messagesUrl(conversationKey), {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            messages: [{ role: "user", content: trimmed }],
          }),
        });
        if (!res.ok) {
          throw new ApiError(res.status, `submit: ${res.status}`);
        }
      } catch (e) {
        // Roll back the optimistic message; surface the error.
        setMessages((prev) => prev.filter((m) => m.id !== userId));
        setStatus("error");
        setError(e instanceof Error ? e.message : "submit failed");
        throw e;
      }
    },
    [conversationKey],
  );

  const stop = useCallback(async () => {
    try {
      await fetch(stopUrl(conversationKey), {
        method: "POST",
        credentials: "same-origin",
      });
      // Don't flip status here — the worker emits a `d:` frame that
      // the stream loop folds into `status = "ready"` on every tab.
    } catch {
      // Stop is best-effort; if the request itself fails the user can
      // retry. The next `d:` (or component unmount) will clean up
      // status regardless.
    }
  }, [conversationKey]);

  return { messages, status, citations, error, submit, stop };
}

/** Re-exported for the convenience of components that want to type
 *  `ChatMessageWire`-shaped initial data. */
export type { ChatMessageWire };
