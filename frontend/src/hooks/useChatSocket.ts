import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ChatEvent, CitationEntry, MessageDto, ServerWsMessage } from "../lib/types";

type ChatStatus = "ready" | "submitted" | "streaming" | "error";
type LiveStatus = "submitted" | "streaming" | "stopping";
export type RealtimeStatus = "idle" | "connecting" | "connected" | "reconnecting" | "disconnected";

type ChatSocketOptions = {
  onResync?: () => void | Promise<void>;
  onTerminalRefresh?: () => void | Promise<void>;
  onTitleUpdated?: (title: string) => void | Promise<void>;
};

const RECONNECT_DELAYS_MS = [250, 500, 1000, 2000, 5000];

export function useChatSocket(
  conversationId: string | null,
  seedMessages: MessageDto[],
  options: ChatSocketOptions = {}
) {
  const [messages, setMessages] = useState<MessageDto[]>(seedMessages);
  const [status, setStatus] = useState<ChatStatus | LiveStatus>("ready");
  const [realtimeStatus, setRealtimeStatus] = useState<RealtimeStatus>("idle");
  const [error, setError] = useState<string | null>(null);
  const statusRef = useRef<ChatStatus | LiveStatus>("ready");
  const seedSignatureRef = useRef("");
  const lastEventIdByConversationRef = useRef(new Map<string, string>());
  const inFlightUserIdRef = useRef<string | null>(null);
  const overlayTextRef = useRef("");
  const liveCitationsRef = useRef<CitationEntry[]>([]);
  const onResyncRef = useRef(options.onResync);
  const onTerminalRefreshRef = useRef(options.onTerminalRefresh);
  const onTitleUpdatedRef = useRef(options.onTitleUpdated);
  const seedSignature = useMemo(() => messagesSignature(seedMessages), [seedMessages]);

  useEffect(() => {
    statusRef.current = status;
  }, [status]);

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
    inFlightUserIdRef.current = null;
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
            turn_id: null,
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
        setMessages((current) => upsertAssistantOverlay(current, overlayTextRef.current, liveCitationsRef.current));
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
        void onTerminalRefreshRef.current?.();
        return;
      case "interrupted":
        setStatus("ready");
        setMessages((current) =>
          upsertInterruptedAssistant(
            current,
            event.assistant_message_id,
            event.content,
            liveCitationsRef.current,
            event.finish_reason
          )
        );
        overlayTextRef.current = "";
        inFlightUserIdRef.current = null;
        liveCitationsRef.current = [];
        void onTerminalRefreshRef.current?.();
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
        liveCitationsRef.current = [];
        void onTerminalRefreshRef.current?.();
        return;
      case "error":
        setStatus("error");
        setError(event.message);
        return;
      case "title_updated":
        void onTitleUpdatedRef.current?.(event.title);
        return;
    }
  }, []);

  useEffect(() => {
    if (!conversationId) {
      setRealtimeStatus("idle");
      return;
    }

    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    let closedByEffect = false;
    let socket: WebSocket | null = null;
    let reconnectTimer: number | undefined;
    let pingTimer: number | undefined;
    let reconnectAttempt = 0;

    const clearTimers = () => {
      if (reconnectTimer) {
        window.clearTimeout(reconnectTimer);
        reconnectTimer = undefined;
      }
      if (pingTimer) {
        window.clearInterval(pingTimer);
        pingTimer = undefined;
      }
    };

    const resetTransientState = () => {
      const inFlightUserId = inFlightUserIdRef.current;
      setStatus("ready");
      overlayTextRef.current = "";
      inFlightUserIdRef.current = null;
      liveCitationsRef.current = [];
      setMessages((current) =>
        current.filter((message) => message.id !== "assistant-live" && message.id !== inFlightUserId)
      );
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
        resetTransientState();
        void Promise.resolve(onResyncRef.current?.()).finally(() => {
          if (socket?.readyState === WebSocket.OPEN) {
            subscribe(socket);
          }
        });
      } else if (msg.type === "error") {
        setStatus("error");
        setError(msg.message);
      }
    };

    const scheduleReconnect = () => {
      if (closedByEffect) return;
      setRealtimeStatus(reconnectAttempt === 0 ? "disconnected" : "reconnecting");
      const baseDelay = RECONNECT_DELAYS_MS[Math.min(reconnectAttempt, RECONNECT_DELAYS_MS.length - 1)];
      const jitter = Math.floor(Math.random() * 150);
      reconnectAttempt += 1;
      reconnectTimer = window.setTimeout(connect, baseDelay + jitter);
    };

    const connect = () => {
      if (closedByEffect) return;
      clearTimers();
      socket?.close();
      setRealtimeStatus(reconnectAttempt === 0 ? "connecting" : "reconnecting");
      const nextSocket = new WebSocket(`${protocol}://${window.location.host}/ws/chat`);
      socket = nextSocket;

      nextSocket.addEventListener("open", () => {
        reconnectAttempt = 0;
        setRealtimeStatus("connected");
        setError(null);
        subscribe(nextSocket);
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

    connect();

    return () => {
      closedByEffect = true;
      clearTimers();
      if (socket) {
        socket.close();
        socket = null;
      }
      setRealtimeStatus("idle");
    };
  }, [conversationId, applyEvent]);

  const lastMessageId = useMemo(() => {
    const committed = messages.filter((message) => message.id !== "assistant-live");
    return committed.at(-1)?.id ?? null;
  }, [messages]);

  return { messages, status, realtimeStatus, error, lastMessageId, setStatus };
}

function isLiveStatus(status: ChatStatus | LiveStatus): status is LiveStatus {
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
  citations: CitationEntry[]
): MessageDto[] {
  const overlay = {
    id: "assistant-live",
    role: "assistant" as const,
    content,
    parent_message_id: null,
    citations,
    turn_id: null,
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
  finishReason: string
): MessageDto[] {
  const next = messages.filter((message) => message.id !== "assistant-live");
  const assistant = {
    id: assistantMessageId,
    role: "assistant" as const,
    content,
    parent_message_id: null,
    citations,
    turn_id: null,
    interrupted: true,
    finish_reason: finishReason,
    created_at: new Date().toISOString()
  };
  const index = next.findIndex((message) => message.id === assistantMessageId);
  if (index === -1) return [...next, assistant];
  next[index] = assistant;
  return next;
}
