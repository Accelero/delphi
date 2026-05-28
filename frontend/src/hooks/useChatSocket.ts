import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ChatEvent, CitationEntry, MessageDto, ServerWsMessage } from "../lib/types";

type ChatStatus = "ready" | "submitted" | "streaming" | "stopping" | "error";
type LiveStatus = "submitted" | "streaming" | "stopping";
export type RealtimeConnectionStatus =
  | "idle"
  | "connecting"
  | "connected"
  | "reconnecting"
  | "disconnected";
export type RealtimeRecoveryStatus = "idle" | "resyncing" | "retrying" | "failed";

type ChatSocketOptions = {
  onResync?: (conversationId: string) => MessageDto[] | void | Promise<MessageDto[] | void>;
  onTerminalRefresh?: (conversationId: string) => void | Promise<void>;
  onTitleUpdated?: (conversationId: string, title: string) => void | Promise<void>;
};

const RECONNECT_DELAYS_MS = [250, 500, 1000, 2000, 5000];
const RESYNC_DELAYS_MS = [500, 1000, 2000, 5000, 10000];

export function useChatSocket(
  conversationId: string | null,
  seedMessages: MessageDto[],
  options: ChatSocketOptions = {}
) {
  const [messages, setMessages] = useState<MessageDto[]>(seedMessages);
  const [status, setStatus] = useState<ChatStatus>("ready");
  const [connectionStatus, setConnectionStatus] = useState<RealtimeConnectionStatus>("idle");
  const [recoveryStatus, setRecoveryStatus] = useState<RealtimeRecoveryStatus>("idle");
  const [recoveryError, setRecoveryError] = useState<string | null>(null);
  const [recoveryAttempt, setRecoveryAttempt] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const statusRef = useRef<ChatStatus>("ready");
  const recoveryStatusRef = useRef<RealtimeRecoveryStatus>("idle");
  const seedSignatureRef = useRef("");
  const lastEventIdByConversationRef = useRef(new Map<string, string>());
  const inFlightUserIdRef = useRef<string | null>(null);
  const inFlightTurnIdRef = useRef<string | null>(null);
  const overlayTextRef = useRef("");
  const liveCitationsRef = useRef<CitationEntry[]>([]);
  const onResyncRef = useRef(options.onResync);
  const onTerminalRefreshRef = useRef(options.onTerminalRefresh);
  const onTitleUpdatedRef = useRef(options.onTitleUpdated);
  const reconnectNowRef = useRef<() => void>(() => undefined);
  const retryRecoveryRef = useRef<() => void>(() => undefined);
  const seedSignature = useMemo(() => messagesSignature(seedMessages), [seedMessages]);

  useEffect(() => {
    statusRef.current = status;
  }, [status]);

  useEffect(() => {
    recoveryStatusRef.current = recoveryStatus;
  }, [recoveryStatus]);

  useEffect(() => {
    onResyncRef.current = options.onResync;
    onTerminalRefreshRef.current = options.onTerminalRefresh;
    onTitleUpdatedRef.current = options.onTitleUpdated;
  }, [options.onResync, options.onTerminalRefresh, options.onTitleUpdated]);

  useEffect(() => {
    seedSignatureRef.current = seedSignature;
    setMessages(seedMessages);
    setStatus("ready");
    setError(null);
    recoveryStatusRef.current = "idle";
    setRecoveryStatus("idle");
    setRecoveryError(null);
    setRecoveryAttempt(0);
    inFlightUserIdRef.current = null;
    inFlightTurnIdRef.current = null;
    overlayTextRef.current = "";
    liveCitationsRef.current = [];
  }, [conversationId]);

  useEffect(() => {
    if (seedSignatureRef.current === seedSignature) return;
    seedSignatureRef.current = seedSignature;
    if (isLiveStatus(statusRef.current)) return;
    setMessages(seedMessages);
  }, [seedMessages, seedSignature]);

  const applyEvent = useCallback((event: ChatEvent) => {
    switch (event.type) {
      case "turn_started":
        setStatus((current) => (current === "stopping" ? current : "submitted"));
        setError(null);
        return;
      case "user_message":
        inFlightUserIdRef.current = event.id;
        inFlightTurnIdRef.current = event.turn_id ?? null;
        overlayTextRef.current = "";
        liveCitationsRef.current = [];
        setStatus((current) => (current === "stopping" ? current : "streaming"));
        setMessages((current) => [
          ...current.filter((message) => message.id !== event.id && message.id !== "assistant-live"),
          {
            id: event.id,
            role: "user",
            content: event.content,
            parent_message_id: null,
            citations: [],
            turn_id: event.turn_id ?? null,
            created_at: new Date().toISOString()
          }
        ]);
        return;
      case "citations":
        liveCitationsRef.current = event.citations;
        setMessages((current) =>
          current.map((message) =>
            message.id === "assistant-live" ? { ...message, citations: event.citations } : message
          )
        );
        return;
      case "text_delta":
        overlayTextRef.current += event.delta;
        setStatus((current) => (current === "stopping" ? current : "streaming"));
        setMessages((current) =>
          upsertAssistantOverlay(
            current,
            overlayTextRef.current,
            liveCitationsRef.current,
            inFlightTurnIdRef.current
          )
        );
        return;
      case "finish":
        setStatus("ready");
        setMessages((current) =>
          current.map((message) =>
            message.id === "assistant-live"
              ? { ...message, id: event.assistant_message_id, citations: liveCitationsRef.current }
              : message
          )
        );
        overlayTextRef.current = "";
        inFlightUserIdRef.current = null;
        inFlightTurnIdRef.current = null;
        if (conversationId) void onTerminalRefreshRef.current?.(conversationId);
        return;
      case "interrupted":
        setStatus("ready");
        setMessages((current) =>
          upsertInterruptedAssistant(
            current,
            event.assistant_message_id,
            event.content,
            liveCitationsRef.current,
            event.finish_reason,
            inFlightTurnIdRef.current
          )
        );
        overlayTextRef.current = "";
        inFlightUserIdRef.current = null;
        inFlightTurnIdRef.current = null;
        liveCitationsRef.current = [];
        if (conversationId) void onTerminalRefreshRef.current?.(conversationId);
        return;
      case "clear":
        setStatus("ready");
        setMessages((current) =>
          current.filter(
            (message) => message.id !== "assistant-live" && message.id !== inFlightUserIdRef.current
          )
        );
        overlayTextRef.current = "";
        inFlightUserIdRef.current = null;
        inFlightTurnIdRef.current = null;
        liveCitationsRef.current = [];
        if (conversationId) void onTerminalRefreshRef.current?.(conversationId);
        return;
      case "error":
        setStatus("error");
        setError(event.message);
        return;
      case "title_updated":
        if (conversationId) void onTitleUpdatedRef.current?.(conversationId, event.title);
        return;
    }
  }, [conversationId]);

  useEffect(() => {
    if (!conversationId) {
      setConnectionStatus("idle");
      return;
    }

    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    let closedByEffect = false;
    let socket: WebSocket | null = null;
    let reconnectTimer: number | undefined;
    let resyncTimer: number | undefined;
    let pingTimer: number | undefined;
    let reconnectAttempt = 0;
    let resyncAttempt = 0;
    let resyncInFlight = false;

    const clearTimers = () => {
      if (reconnectTimer) {
        window.clearTimeout(reconnectTimer);
        reconnectTimer = undefined;
      }
      if (pingTimer) {
        window.clearInterval(pingTimer);
        pingTimer = undefined;
      }
      if (resyncTimer) {
        window.clearTimeout(resyncTimer);
        resyncTimer = undefined;
      }
    };

    const resetLiveState = () => {
      setStatus("ready");
      overlayTextRef.current = "";
      inFlightUserIdRef.current = null;
      inFlightTurnIdRef.current = null;
      liveCitationsRef.current = [];
    };

    const replaceWithAuthoritativeMessages = (nextMessages: MessageDto[]) => {
      setStatus("ready");
      overlayTextRef.current = "";
      inFlightUserIdRef.current = null;
      inFlightTurnIdRef.current = null;
      liveCitationsRef.current = [];
      setMessages(nextMessages);
    };

    const subscribe = (target: WebSocket) => {
      target.send(
        JSON.stringify({
          type: "subscribe_conversation",
          conversation_id: conversationId,
          last_event_id: lastEventIdByConversationRef.current.get(conversationId) ?? null
        })
      );
    };

    const finishRecovery = (nextMessages?: MessageDto[] | void) => {
      if (Array.isArray(nextMessages)) {
        replaceWithAuthoritativeMessages(nextMessages);
      } else {
        resetLiveState();
      }
      recoveryStatusRef.current = "idle";
      setRecoveryStatus("idle");
      setRecoveryError(null);
      setRecoveryAttempt(0);
      resyncAttempt = 0;
      if (socket?.readyState === WebSocket.OPEN) {
        subscribe(socket);
      }
    };

    const runResync = () => {
      if (closedByEffect) return;
      if (resyncInFlight) return;
      if (resyncTimer) {
        window.clearTimeout(resyncTimer);
        resyncTimer = undefined;
      }

      resyncInFlight = true;
      const attempt = resyncAttempt;
      recoveryStatusRef.current = attempt === 0 ? "resyncing" : "retrying";
      setRecoveryAttempt(attempt + 1);
      setRecoveryStatus(attempt === 0 ? "resyncing" : "retrying");
      setRecoveryError(null);

      void Promise.resolve(onResyncRef.current?.(conversationId))
        .then(finishRecovery)
        .catch((err) => {
          if (closedByEffect) return;
          const message = err instanceof Error ? err.message : "Unable to resync chat state.";
          recoveryStatusRef.current = "retrying";
          setRecoveryStatus("retrying");
          setRecoveryError(message);
          const baseDelay = RESYNC_DELAYS_MS[Math.min(attempt, RESYNC_DELAYS_MS.length - 1)];
          const jitter = Math.floor(Math.random() * 250);
          resyncAttempt += 1;
          resyncTimer = window.setTimeout(runResync, baseDelay + jitter);
        })
        .finally(() => {
          resyncInFlight = false;
        });
    };

    retryRecoveryRef.current = () => {
      resyncAttempt = 0;
      runResync();
    };

    const handleMessage = (raw: MessageEvent<string>) => {
      let msg: ServerWsMessage;
      try {
        msg = JSON.parse(raw.data) as ServerWsMessage;
      } catch {
        setStatus("error");
        setError("Realtime connection returned an invalid message.");
        return;
      }

      if (msg.type === "event" && msg.conversation_id === conversationId) {
        applyEvent(msg.event);
        lastEventIdByConversationRef.current.set(conversationId, msg.event_id);
      } else if (msg.type === "resync_required" && msg.conversation_id === conversationId) {
        lastEventIdByConversationRef.current.delete(conversationId);
        runResync();
      } else if (msg.type === "error") {
        setStatus("error");
        setError(msg.message);
      }
    };

    const scheduleReconnect = () => {
      if (closedByEffect) return;
      setConnectionStatus(reconnectAttempt === 0 ? "disconnected" : "reconnecting");
      const baseDelay = RECONNECT_DELAYS_MS[Math.min(reconnectAttempt, RECONNECT_DELAYS_MS.length - 1)];
      const jitter = Math.floor(Math.random() * 150);
      reconnectAttempt += 1;
      reconnectTimer = window.setTimeout(connect, baseDelay + jitter);
    };

    const connect = () => {
      if (closedByEffect) return;
      if (reconnectTimer) {
        window.clearTimeout(reconnectTimer);
        reconnectTimer = undefined;
      }
      if (pingTimer) {
        window.clearInterval(pingTimer);
        pingTimer = undefined;
      }
      socket?.close();
      setConnectionStatus(reconnectAttempt === 0 ? "connecting" : "reconnecting");
      const nextSocket = new WebSocket(`${protocol}://${window.location.host}/ws/chat`);
      socket = nextSocket;

      nextSocket.addEventListener("open", () => {
        reconnectAttempt = 0;
        setConnectionStatus("connected");
        setError(null);
        if (recoveryStatusRef.current === "idle") {
          subscribe(nextSocket);
        }
        pingTimer = window.setInterval(() => {
          if (nextSocket.readyState === WebSocket.OPEN) {
            nextSocket.send(JSON.stringify({ type: "ping" }));
          }
        }, 25000);
      });
      nextSocket.addEventListener("message", handleMessage);
      nextSocket.addEventListener("close", () => {
        if (socket !== nextSocket) return;
        clearTimers();
        scheduleReconnect();
      });
      nextSocket.addEventListener("error", () => {
        nextSocket.close();
      });
    };

    reconnectNowRef.current = () => {
      reconnectAttempt = 0;
      connect();
    };

    connect();

    return () => {
      closedByEffect = true;
      clearTimers();
      if (socket) {
        socket.close();
        socket = null;
      }
      setConnectionStatus("idle");
      retryRecoveryRef.current = () => undefined;
      reconnectNowRef.current = () => undefined;
    };
  }, [conversationId, applyEvent]);

  const lastMessageId = useMemo(() => {
    const committed = messages.filter((message) => message.id !== "assistant-live");
    return committed.at(-1)?.id ?? null;
  }, [messages]);

  return {
    messages,
    status,
    connectionStatus,
    recoveryStatus,
    recoveryError,
    recoveryAttempt,
    error,
    lastMessageId,
    reconnectNow: () => reconnectNowRef.current(),
    retryRecovery: () => retryRecoveryRef.current(),
    setStatus
  };
}

function isLiveStatus(status: ChatStatus): status is LiveStatus {
  return status === "submitted" || status === "streaming" || status === "stopping";
}

function messagesSignature(messages: MessageDto[]): string {
  return messages
    .map(
      (message) =>
        `${message.id}:${message.created_at}:${message.content.length}:${message.interrupted ?? false}:${message.finish_reason ?? ""}`
    )
    .join("|");
}

function upsertAssistantOverlay(
  messages: MessageDto[],
  content: string,
  citations: CitationEntry[],
  turnId: string | null
): MessageDto[] {
  const overlay = {
    id: "assistant-live",
    role: "assistant" as const,
    content,
    parent_message_id: null,
    citations,
    turn_id: turnId,
    interrupted: false,
    finish_reason: null,
    created_at: new Date().toISOString()
  };
  const index = messages.findIndex((message) => message.id === "assistant-live");
  if (index === -1) return [...messages, overlay];
  const next = messages.slice();
  next[index] = overlay;
  return next;
}

function upsertInterruptedAssistant(
  messages: MessageDto[],
  assistantMessageId: string,
  content: string,
  citations: CitationEntry[],
  finishReason: string,
  turnId: string | null
): MessageDto[] {
  const next = messages.filter((message) => message.id !== "assistant-live");
  const assistant = {
    id: assistantMessageId,
    role: "assistant" as const,
    content,
    parent_message_id: null,
    citations,
    turn_id: turnId,
    interrupted: true,
    finish_reason: finishReason,
    created_at: new Date().toISOString()
  };
  const index = next.findIndex((message) => message.id === assistantMessageId);
  if (index === -1) return [...next, assistant];
  next[index] = assistant;
  return next;
}
