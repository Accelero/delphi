/**
 * `useChatStream(conversationKey, opts)` — multi-tab chat surface (v3).
 *
 * Every tab opens a long-lived `EventSource` against
 * `/api/chat/conversations/{key}/stream` on mount and treats it as the
 * single source of truth for the in-flight turn:
 *
 *   - `submit(text)` is fire-and-forget POST + 202; the user message
 *     arrives via SSE (`user_message`) within a few ms — no optimistic
 *     insert, no dedup.
 *   - `stop()` POSTs `/conversations/{key}/stop` (fire-and-forget).
 *     UI rollback comes via the SSE `clear` event.
 *
 * Reset rule: **every `user_message` event clears the assistant overlay
 * and resets the text accumulator** — regardless of whether it's a brand
 * new turn or a reconnect-replay of an in-flight turn. Without this rule
 * a mid-turn reconnect would produce `hellohelloworld`.
 *
 * Reconcile-on-reopen rule: every time the EventSource (re)opens we
 * trigger `onTurnEnd?.()` to refetch committed history. A freshly
 * mounted late joiner (e.g. switching chat sessions in the same tab)
 * or a tab reconnecting after a blip may have missed the `finish` for
 * a turn that committed while it was disconnected — that committed
 * pair lives only in the DB, not in this client's cached seed, and the
 * replay buffer was dropped at `finish`. Refetching surfaces it. Any
 * *in-flight* turn is replayed on top via the SSE buffer.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { ulid } from "ulid";

import type { CitationEntry } from "@/components/chat/MessageBody";
import { api } from "@/lib/api";

/** Status enum shaped like `@ai-sdk/react`'s. `submitted` is the brief
 *  gap between POST and the first `text` event; UIs typically render
 *  the same placeholder for `submitted` and `streaming`. */
export type StreamStatus = "ready" | "submitted" | "streaming" | "error";

export type LocalMessage = {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
};

export type UseChatStreamOptions = {
  initialMessages?: LocalMessage[];
  /** Called after a turn ends (`finish`, `error`, `clear`, or 409),
   *  and on EventSource (re)open when overlay is non-empty. Callers
   *  use it to invalidate the conversation query. */
  onTurnEnd?: () => void;
};

export type UseChatStreamReturn = {
  messages: LocalMessage[];
  status: StreamStatus;
  citations: CitationEntry[];
  error: string | null;
  submit: (text: string) => Promise<void>;
  stop: () => Promise<void>;
};

function tailMessageId(messages: LocalMessage[]): string | null {
  const tail = messages[messages.length - 1];
  return tail ? tail.id : null;
}

/** Stable content fingerprint for the seed array. Used to detect
 *  whether the caller's `initialMessages` prop actually changed (vs.
 *  changing reference for the same content), so the reset effect
 *  doesn't loop on unmemoised props. */
function seedKey(messages: LocalMessage[]): string {
  return messages.map((m) => `${m.id}:${m.content.length}`).join("|");
}

const STREAMING_ASSISTANT_ID = "__streaming-assistant__";

export function useChatStream(
  conversationKey: string,
  opts: UseChatStreamOptions = {},
): UseChatStreamReturn {
  const { initialMessages = [], onTurnEnd } = opts;
  const [messages, setMessages] = useState<LocalMessage[]>(initialMessages);
  const [status, setStatus] = useState<StreamStatus>("ready");
  const [citations, setCitations] = useState<CitationEntry[]>([]);
  const [error, setError] = useState<string | null>(null);

  const lastKnownMessageIdRef = useRef<string | null>(
    tailMessageId(initialMessages),
  );
  /** Holds the current turn's user_message id so `clear` can drop the
   *  optimistic-ish user row from the local message list. */
  const inFlightUserIdRef = useRef<string | null>(null);
  /** Assistant overlay buffer. Rendered as the last assistant message
   *  while status is `streaming`. */
  const overlayRef = useRef<string>("");

  const onTurnEndRef = useRef(onTurnEnd);
  useEffect(() => {
    onTurnEndRef.current = onTurnEnd;
  }, [onTurnEnd]);

  const statusRef = useRef(status);
  useEffect(() => {
    statusRef.current = status;
  }, [status]);

  // When the seed changes (caller refetched), replace local state ONLY
  // when we're idle. A mid-stream prop refresh must not wipe in-flight
  // overlay state. We compare *content* (id + content per row) so a
  // caller who forgets to memoise the prop doesn't trigger an infinite
  // render loop on identical seed data.
  const lastSeedKeyRef = useRef<string>(seedKey(initialMessages));
  useEffect(() => {
    const next = seedKey(initialMessages);
    if (lastSeedKeyRef.current === next) return;
    lastSeedKeyRef.current = next;
    if (statusRef.current !== "ready") return;
    setMessages(initialMessages);
    lastKnownMessageIdRef.current = tailMessageId(initialMessages);
  }, [initialMessages]);

  // -----------------------------------------------------------------
  // Long-lived SSE subscription.
  // -----------------------------------------------------------------
  useEffect(() => {
    const url = `/api/chat/conversations/${encodeURIComponent(conversationKey)}/stream`;
    const es = new EventSource(url);

    es.addEventListener("open", () => {
      // Reconcile committed history on every (re)connect. A freshly
      // mounted late joiner — or a tab reconnecting after a blip — may
      // have missed the `finish` for a turn that committed while it was
      // disconnected. The classic case: the user submits, navigates
      // away (this surface unmounts, closing its EventSource), the turn
      // commits, then they navigate back. The tab that would have
      // invalidated the conversation cache on `finish` is gone, so the
      // committed pair lives only in the DB — not in this client's
      // cached seed, and no replay covers it (the buffer is dropped at
      // `finish`). Refetching here surfaces that completed turn. Any
      // *in-flight* turn is still replayed on top via the SSE buffer,
      // and the seed-reset is guarded by `status` so the refetch can't
      // wipe a live overlay.
      onTurnEndRef.current?.();
    });

    es.addEventListener("user_message", (ev: MessageEvent) => {
      let parsed: { id?: string; content?: string };
      try {
        parsed = JSON.parse(ev.data);
      } catch {
        return;
      }
      const id = parsed.id;
      const content = parsed.content ?? "";
      if (typeof id !== "string") return;

      // RESET RULE: every user_message clears the assistant overlay
      // and any prior in-flight user id.
      overlayRef.current = "";
      inFlightUserIdRef.current = id;
      setCitations([]);
      setError(null);
      setStatus("submitted");
      setMessages((prev) => {
        // Drop any previous streaming overlay row.
        const filtered = prev.filter((m) => m.id !== STREAMING_ASSISTANT_ID);
        if (filtered.some((m) => m.id === id)) {
          return filtered;
        }
        return [...filtered, { id, role: "user", content }];
      });
    });

    es.addEventListener("text", (ev: MessageEvent) => {
      let delta: string;
      try {
        delta = JSON.parse(ev.data);
      } catch {
        return;
      }
      if (typeof delta !== "string") return;
      overlayRef.current += delta;
      const overlay = overlayRef.current;
      setStatus("streaming");
      setMessages((prev) => {
        const idx = prev.findIndex((m) => m.id === STREAMING_ASSISTANT_ID);
        if (idx >= 0) {
          const next = prev.slice();
          next[idx] = { ...next[idx], content: overlay };
          return next;
        }
        return [
          ...prev,
          { id: STREAMING_ASSISTANT_ID, role: "assistant", content: overlay },
        ];
      });
    });

    es.addEventListener("citations", (ev: MessageEvent) => {
      try {
        const arr = JSON.parse(ev.data);
        if (Array.isArray(arr)) {
          setCitations(arr as CitationEntry[]);
        }
      } catch {
        // ignore malformed
      }
    });

    es.addEventListener("error", (ev: MessageEvent) => {
      // Two distinct event shapes hit this handler:
      //   1) named SSE `error` frames from the server — `ev.data` is a
      //      JSON string carrying the message.
      //   2) EventSource transport errors — no `data` field. Treated
      //      as transient; EventSource will auto-reconnect.
      if (typeof (ev as MessageEvent).data === "string") {
        let msg: string;
        try {
          msg = JSON.parse((ev as MessageEvent).data);
        } catch {
          msg = "stream error";
        }
        setError(typeof msg === "string" ? msg : "stream error");
        setStatus("error");
      }
    });

    es.addEventListener("finish", (ev: MessageEvent) => {
      let parsed: { finishReason?: string; assistantMessageId?: string };
      try {
        parsed = JSON.parse(ev.data);
      } catch {
        parsed = {};
      }
      const asstId = parsed.assistantMessageId;
      const overlay = overlayRef.current;
      overlayRef.current = "";
      inFlightUserIdRef.current = null;
      if (typeof asstId === "string" && asstId.length > 0) {
        lastKnownMessageIdRef.current = asstId;
        setMessages((prev) =>
          prev.map((m) =>
            m.id === STREAMING_ASSISTANT_ID
              ? { id: asstId, role: "assistant", content: overlay }
              : m,
          ),
        );
      } else {
        setMessages((prev) => prev.filter((m) => m.id !== STREAMING_ASSISTANT_ID));
      }
      setStatus("ready");
      onTurnEndRef.current?.();
    });

    es.addEventListener("clear", () => {
      const dropUserId = inFlightUserIdRef.current;
      overlayRef.current = "";
      inFlightUserIdRef.current = null;
      setMessages((prev) =>
        prev.filter(
          (m) => m.id !== STREAMING_ASSISTANT_ID && m.id !== dropUserId,
        ),
      );
      setCitations([]);
      setStatus("ready");
      onTurnEndRef.current?.();
    });

    return () => {
      es.close();
    };
  }, [conversationKey]);

  // -----------------------------------------------------------------
  // submit / stop
  // -----------------------------------------------------------------
  const submit = useCallback(
    async (text: string) => {
      const trimmed = text.trim();
      if (!trimmed) return;
      const id = ulid();
      setError(null);
      setStatus("submitted");
      const res = await api.chat.submitMessage(conversationKey, {
        id,
        text: trimmed,
        parent_id: lastKnownMessageIdRef.current,
      });
      if (res.ok) {
        // The user_message + frames arrive via SSE.
        return;
      }
      if (res.status === 409) {
        setError("Conversation changed; refreshing…");
        setStatus("ready");
        onTurnEndRef.current?.();
        return;
      }
      setError(
        res.status === 401 ? "Not authenticated" : `submit: ${res.status}`,
      );
      setStatus("error");
    },
    [conversationKey],
  );

  const stop = useCallback(async () => {
    try {
      await api.chat.stopChat(conversationKey);
    } catch {
      // best-effort; SSE `clear` is the source of truth
    }
  }, [conversationKey]);

  return { messages, status, citations, error, submit, stop };
}

export type { ChatMessageWire } from "@/lib/api";
