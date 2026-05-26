import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ChatEvent, CitationEntry, MessageDto, ServerWsMessage } from "../lib/types";

type ChatStatus = "ready" | "submitted" | "streaming" | "error";

export function useChatSocket(conversationId: string | null, seedMessages: MessageDto[]) {
  const [messages, setMessages] = useState<MessageDto[]>(seedMessages);
  const [status, setStatus] = useState<ChatStatus>("ready");
  const [error, setError] = useState<string | null>(null);
  const lastEventIdRef = useRef<string | null>(null);
  const inFlightUserIdRef = useRef<string | null>(null);
  const overlayTextRef = useRef("");
  const liveCitationsRef = useRef<CitationEntry[]>([]);

  useEffect(() => {
    setMessages(seedMessages);
    setStatus("ready");
    setError(null);
    lastEventIdRef.current = null;
    inFlightUserIdRef.current = null;
    overlayTextRef.current = "";
    liveCitationsRef.current = [];
  }, [conversationId, seedMessages]);

  const applyEvent = useCallback((event: ChatEvent) => {
    switch (event.type) {
      case "turn_started":
        setStatus("submitted");
        setError(null);
        return;
      case "user_message":
        inFlightUserIdRef.current = event.id;
        overlayTextRef.current = "";
        liveCitationsRef.current = [];
        setStatus("streaming");
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
        setStatus("streaming");
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
        return;
      case "error":
        setStatus("error");
        setError(event.message);
        return;
      case "title_updated":
        return;
    }
  }, []);

  useEffect(() => {
    if (!conversationId) return;

    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    const socket = new WebSocket(`${protocol}://${window.location.host}/ws/chat`);
    let closedByEffect = false;
    let reconnectTimer: number | undefined;

    const subscribe = () => {
      socket.send(
        JSON.stringify({
          type: "subscribe_conversation",
          conversation_id: conversationId,
          last_event_id: lastEventIdRef.current
        })
      );
    };

    socket.addEventListener("open", subscribe);
    socket.addEventListener("message", (raw) => {
      const msg = JSON.parse(raw.data) as ServerWsMessage;
      if (msg.type === "event" && msg.conversation_id === conversationId) {
        applyEvent(msg.event);
        lastEventIdRef.current = msg.event_id;
      } else if (msg.type === "resync_required" && msg.conversation_id === conversationId) {
        setStatus("ready");
        overlayTextRef.current = "";
      } else if (msg.type === "error") {
        setStatus("error");
        setError(msg.message);
      }
    });
    socket.addEventListener("close", () => {
      if (!closedByEffect) {
        reconnectTimer = window.setTimeout(() => {
          setStatus("error");
          setError("Realtime connection closed. Reopen the chat or refresh after reconnecting.");
        }, 1200);
      }
    });

    const pingTimer = window.setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ type: "ping" }));
      }
    }, 25000);

    return () => {
      closedByEffect = true;
      window.clearInterval(pingTimer);
      if (reconnectTimer) window.clearTimeout(reconnectTimer);
      socket.close();
    };
  }, [conversationId, applyEvent]);

  const lastMessageId = useMemo(() => {
    const committed = messages.filter((message) => message.id !== "assistant-live");
    return committed.at(-1)?.id ?? null;
  }, [messages]);

  return { messages, status, error, lastMessageId, setStatus };
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
    created_at: new Date().toISOString()
  };
  const index = messages.findIndex((message) => message.id === "assistant-live");
  if (index === -1) return [...messages, overlay];
  const next = messages.slice();
  next[index] = overlay;
  return next;
}
