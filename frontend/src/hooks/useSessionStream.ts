/**
 * `useSessionStream(conversationKey, opts)` — drive one chat
 * conversation, ChatGPT-style.
 *
 *   - `submit(text)` does the whole turn: generate a ULID for the user
 *     message, optimistic insert, POST the turn, consume the response
 *     body as the AI SDK data-stream.
 *   - `stop()` POSTs `/tasks/{taskId}/stop` and aborts the read loop.
 *
 * There is **no** persistent connection. No `useEffect` opens a stream
 * on mount. The hook is idle between turns; refetching the
 * conversation (`onTurnEnd` triggered by the caller) is what surfaces
 * cross-tab updates.
 *
 * The hook stays minimal: it owns the local message list, in-flight
 * assistant overlay, the current `taskId`, `lastKnownMessageId`
 * (threading the next turn's `parent_id`), and the abort controller.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { ulid } from "ulid";

import type { CitationEntry } from "@/components/chat/MessageBody";
import { ApiError, api, type ChatMessageWire } from "@/lib/api";

/** Status enum shaped like `@ai-sdk/react`'s so callers don't have to
 *  learn a new word. `submitted` is the brief gap between POST and the
 *  first `0:` arriving; UIs typically render the same "thinking…"
 *  placeholder for both `submitted` and `streaming`. */
export type StreamStatus = "ready" | "submitted" | "streaming" | "error";

export type LocalMessage = {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
};

export type UseSessionStreamOptions = {
  /** Persisted history from the conversation GET response. Rendered as
   *  the initial `messages` until the live stream replaces/extends it.
   *  When the hook is `ready` (no in-flight turn) and the prop
   *  reference changes — typically because `onTurnEnd` triggered a
   *  refetch — the hook replaces its local state wholesale. */
  initialMessages?: LocalMessage[];
  /** Fires after each successful turn (we saw a `d:` frame). Callers
   *  use it to invalidate the conversation query so the persisted pair
   *  replaces the optimistic+streaming overlay. */
  onTurnEnd?: () => void;
};

export type UseSessionStreamReturn = {
  messages: LocalMessage[];
  status: StreamStatus;
  citations: CitationEntry[];
  error: string | null;
  /** Submit one turn. Optimistically inserts the user message, POSTs,
   *  and consumes the streaming response. On 409 (stale parent),
   *  rolls back and surfaces an error. */
  submit: (text: string) => Promise<void>;
  /** Cancel the in-flight turn. POSTs `/tasks/{taskId}/stop` and
   *  aborts the read loop. Rolls back the optimistic user message —
   *  the turn was never persisted. */
  stop: () => Promise<void>;
};

/* -----------------------------------------------------------------
 * Parser — public so we can unit-test it in isolation. The AI SDK's
 * data-stream format is line-prefixed records:
 *
 *   0:"hello world"\n      ← text delta (JSON-encoded string)
 *   2:[{...}]\n            ← data block (JSON array)
 *   3:"error text"\n       ← error (JSON-encoded string)
 *   8:{"taskId":"…"}\n     ← task announcement (delphi extension)
 *   d:{"finishReason":"stop","assistantMessageId":"message:…"}\n
 *                            ← finish marker
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
  | { type: "task"; value: { taskId: string } }
  | {
      type: "finish";
      value: {
        finishReason?: string;
        assistantMessageId?: string;
        [k: string]: unknown;
      };
    };

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

  /** Flush any in-flight TextDecoder state (call on stream end). */
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
      if (sep < 0) continue;
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
          case "8":
            if (
              parsed &&
              typeof parsed === "object" &&
              typeof (parsed as { taskId?: unknown }).taskId === "string"
            ) {
              this.out.push({
                type: "task",
                value: { taskId: (parsed as { taskId: string }).taskId },
              });
            }
            break;
          case "d":
            if (parsed && typeof parsed === "object")
              this.out.push({ type: "finish", value: parsed });
            break;
          default:
            break;
        }
      } catch {
        // Malformed JSON — drop.
      }
    }
  }
}

/* -----------------------------------------------------------------
 * Hook
 * ----------------------------------------------------------------- */

/** Derive the initial `lastKnownMessageId` from the seeded history.
 *  The tail message's id threads the next turn's `parent_id`. */
function tailMessageId(messages: LocalMessage[]): string | null {
  const tail = messages[messages.length - 1];
  return tail ? tail.id : null;
}

const STREAMING_ID_PREFIX = "streaming-";

export function useSessionStream(
  conversationKey: string,
  opts: UseSessionStreamOptions = {},
): UseSessionStreamReturn {
  const { initialMessages = [], onTurnEnd } = opts;
  const [messages, setMessages] = useState<LocalMessage[]>(initialMessages);
  const [status, setStatus] = useState<StreamStatus>("ready");
  const [citations, setCitations] = useState<CitationEntry[]>([]);
  const [error, setError] = useState<string | null>(null);

  // Track the parent_id for the next submit. Updated from history on
  // every refetch while idle, and from each `d:` frame mid-turn.
  const lastKnownMessageIdRef = useRef<string | null>(
    tailMessageId(initialMessages),
  );
  const currentTaskIdRef = useRef<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);
  // Holds the optimistic user-message id mid-turn so `stop()` can
  // roll it back. Cleared at turn end.
  const optimisticUserIdRef = useRef<string | null>(null);
  // Stable id for the in-flight streaming assistant placeholder.
  // Tracked in a ref because setMessages updates would otherwise need
  // to scan the array on every delta.
  const streamingAssistantIdRef = useRef<string | null>(null);

  const onTurnEndRef = useRef(onTurnEnd);
  useEffect(() => {
    onTurnEndRef.current = onTurnEnd;
  }, [onTurnEnd]);

  // Track status in a ref so the prop-driven seed effect can read it
  // without taking a dependency (which would loop).
  const statusRef = useRef(status);
  useEffect(() => {
    statusRef.current = status;
  }, [status]);

  // When the seed changes (caller refetched), replace local state ONLY
  // when we're idle. A mid-stream prop refresh must not wipe in-flight
  // overlay state. Also re-anchors `lastKnownMessageId` to the new
  // tail — so a tab that refetches after another tab's commit picks
  // up the new parent for free.
  useEffect(() => {
    if (statusRef.current !== "ready") return;
    setMessages(initialMessages);
    lastKnownMessageIdRef.current = tailMessageId(initialMessages);
    // initialMessages is the caller's memoised array; identity
    // changes only when the underlying history changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialMessages]);

  const submit = useCallback(
    async (text: string) => {
      const trimmed = text.trim();
      if (!trimmed) return;

      const userIdRaw = ulid();
      const userIdRecord = `message:${userIdRaw}`;
      const optimistic: LocalMessage = {
        id: userIdRecord,
        role: "user",
        content: trimmed,
      };
      optimisticUserIdRef.current = userIdRecord;
      setMessages((prev) => [...prev, optimistic]);
      setStatus("submitted");
      setError(null);
      setCitations([]);

      const controller = new AbortController();
      abortControllerRef.current = controller;
      currentTaskIdRef.current = null;
      streamingAssistantIdRef.current = null;

      let res: Response;
      try {
        res = await api.chat.submitMessage(
          conversationKey,
          {
            id: userIdRaw,
            text: trimmed,
            parent_id: lastKnownMessageIdRef.current,
          },
          controller.signal,
        );
      } catch (e) {
        if (e instanceof DOMException && e.name === "AbortError") return;
        rollbackOptimistic(
          setMessages,
          optimisticUserIdRef,
          streamingAssistantIdRef,
        );
        setStatus("error");
        setError(e instanceof Error ? e.message : "submit failed");
        return;
      }

      if (res.status === 409) {
        rollbackOptimistic(
          setMessages,
          optimisticUserIdRef,
          streamingAssistantIdRef,
        );
        setStatus("error");
        setError("Conversation changed; refreshing…");
        onTurnEndRef.current?.();
        return;
      }
      if (!res.ok || !res.body) {
        rollbackOptimistic(
          setMessages,
          optimisticUserIdRef,
          streamingAssistantIdRef,
        );
        setStatus("error");
        setError(
          res.status === 401 ? "Not authenticated" : `submit: ${res.status}`,
        );
        if (res.status !== 401) {
          throw new ApiError(res.status, `submit: ${res.status}`);
        }
        return;
      }

      const reader = res.body.getReader();
      const parser = new StreamParser();
      try {
        // eslint-disable-next-line no-constant-condition
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          if (value) parser.push(value);
          for (const rec of parser.take()) {
            applyRecord(rec, {
              setMessages,
              setStatus,
              setCitations,
              setError,
              userIdRaw,
              lastKnownMessageIdRef,
              currentTaskIdRef,
              optimisticUserIdRef,
              streamingAssistantIdRef,
              onTurnEnd: onTurnEndRef.current,
            });
          }
        }
        parser.flush();
        for (const rec of parser.take()) {
          applyRecord(rec, {
            setMessages,
            setStatus,
            setCitations,
            setError,
            userIdRaw,
            lastKnownMessageIdRef,
            currentTaskIdRef,
            optimisticUserIdRef,
            streamingAssistantIdRef,
            onTurnEnd: onTurnEndRef.current,
          });
        }
      } catch (e) {
        if (e instanceof DOMException && e.name === "AbortError") return;
        setStatus("error");
        setError(e instanceof Error ? e.message : "stream error");
      } finally {
        abortControllerRef.current = null;
      }
    },
    [conversationKey],
  );

  const stop = useCallback(async () => {
    const taskId = currentTaskIdRef.current;
    abortControllerRef.current?.abort();
    abortControllerRef.current = null;

    if (taskId) {
      try {
        await api.chat.stopTask(conversationKey, taskId);
      } catch {
        // Best-effort.
      }
    }

    rollbackOptimistic(
      setMessages,
      optimisticUserIdRef,
      streamingAssistantIdRef,
    );
    currentTaskIdRef.current = null;
    setStatus("ready");
    setCitations([]);
  }, [conversationKey]);

  return { messages, status, citations, error, submit, stop };
}

function rollbackOptimistic(
  setMessages: React.Dispatch<React.SetStateAction<LocalMessage[]>>,
  optimisticUserIdRef: React.MutableRefObject<string | null>,
  streamingAssistantIdRef: React.MutableRefObject<string | null>,
) {
  const userId = optimisticUserIdRef.current;
  const asstId = streamingAssistantIdRef.current;
  if (!userId && !asstId) return;
  setMessages((prev) =>
    prev.filter((m) => m.id !== userId && m.id !== asstId),
  );
  optimisticUserIdRef.current = null;
  streamingAssistantIdRef.current = null;
}

type ApplyContext = {
  setMessages: React.Dispatch<React.SetStateAction<LocalMessage[]>>;
  setStatus: React.Dispatch<React.SetStateAction<StreamStatus>>;
  setCitations: React.Dispatch<React.SetStateAction<CitationEntry[]>>;
  setError: React.Dispatch<React.SetStateAction<string | null>>;
  userIdRaw: string;
  lastKnownMessageIdRef: React.MutableRefObject<string | null>;
  currentTaskIdRef: React.MutableRefObject<string | null>;
  optimisticUserIdRef: React.MutableRefObject<string | null>;
  streamingAssistantIdRef: React.MutableRefObject<string | null>;
  onTurnEnd: (() => void) | undefined;
};

function applyRecord(rec: ParsedRecord, ctx: ApplyContext) {
  switch (rec.type) {
    case "task":
      ctx.currentTaskIdRef.current = rec.value.taskId;
      break;
    case "text": {
      ctx.setStatus("streaming");
      const delta = rec.value;
      const existing = ctx.streamingAssistantIdRef.current;
      if (existing == null) {
        const id = `${STREAMING_ID_PREFIX}${ctx.userIdRaw}`;
        ctx.streamingAssistantIdRef.current = id;
        ctx.setMessages((prev) => [
          ...prev,
          { id, role: "assistant", content: delta },
        ]);
      } else {
        ctx.setMessages((prev) =>
          prev.map((m) =>
            m.id === existing
              ? { ...m, content: m.content + delta }
              : m,
          ),
        );
      }
      break;
    }
    case "data":
      for (const entry of rec.value) {
        if (
          entry &&
          typeof entry === "object" &&
          (entry as { type?: string }).type === "citations"
        ) {
          const chunks = (entry as { chunks?: CitationEntry[] }).chunks;
          if (Array.isArray(chunks)) ctx.setCitations(chunks);
        }
      }
      break;
    case "error":
      ctx.setError(rec.value);
      ctx.setStatus("error");
      break;
    case "finish": {
      const asstId = rec.value.assistantMessageId;
      if (typeof asstId === "string" && asstId.length > 0) {
        ctx.lastKnownMessageIdRef.current = asstId;
      }
      ctx.currentTaskIdRef.current = null;
      ctx.optimisticUserIdRef.current = null;
      ctx.streamingAssistantIdRef.current = null;
      ctx.setStatus("ready");
      // Caller's onTurnEnd invalidates the conversation query; the
      // resulting prop refresh replaces the optimistic+streaming pair
      // with the persisted rows.
      ctx.onTurnEnd?.();
      break;
    }
  }
}

export type { ChatMessageWire };
