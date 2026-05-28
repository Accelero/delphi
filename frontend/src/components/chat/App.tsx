import { useCallback, useEffect, useState } from "react";
import { useThemeMode } from "../../hooks/useThemeMode";
import { api } from "../../lib/api";
import type { ConversationDetail, ConversationSummary } from "../../lib/types";
import { ChatPane } from "./ChatPane";
import { ConversationSidebar } from "./ConversationSidebar";

export function App() {
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);
  const [activeId, setActiveId] = useState<string | null>(() => routeConversationId());
  const [active, setActive] = useState<ConversationDetail | null>(null);
  const theme = useThemeMode();

  const navigateToConversation = useCallback((id: string, mode: "push" | "replace" = "push") => {
    setActiveId(id);
    writeConversationRoute(id, mode);
  }, []);

  const navigateToChatRoot = useCallback((mode: "push" | "replace" = "push") => {
    setActiveId(null);
    writeChatRootRoute(mode);
  }, []);

  const refreshList = useCallback(async () => {
    const rows = await api.listConversations();
    setConversations((current) => {
      const existingTitleById = new Map(
        current.map((conversation) => [conversation.id, conversation.title])
      );
      return rows.map((row) => {
        const existingTitle = existingTitleById.get(row.id);
        return row.title === "New chat" && existingTitle && existingTitle !== "New chat"
          ? { ...row, title: existingTitle }
          : row;
      });
    });
    if (!activeId && rows[0]) navigateToConversation(rows[0].id, "replace");
  }, [activeId, navigateToConversation]);

  const refreshActive = useCallback(async () => {
    if (!activeId) return;
    const detail = await api.getConversation(activeId);
    setActive((current) =>
      detail.title === "New chat" &&
      current &&
      current.id === detail.id &&
      current.title !== "New chat"
        ? { ...detail, title: current.title }
        : detail
    );
  }, [activeId]);

  const refreshChatState = useCallback(async () => {
    await Promise.all([refreshActive(), refreshList()]);
  }, [refreshActive, refreshList]);

  const applyTitleUpdate = useCallback(
    (title: string) => {
      if (!activeId) return;
      setConversations((current) =>
        current.map((conversation) =>
          conversation.id === activeId ? { ...conversation, title } : conversation
        )
      );
      setActive((current) => (current && current.id === activeId ? { ...current, title } : current));
    },
    [activeId]
  );

  useEffect(() => {
    const onPopState = () => {
      setActiveId(routeConversationId());
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    api.me().then(refreshList).catch(() => undefined);
  }, []);

  useEffect(() => {
    if (activeId) {
      refreshActive().catch(() => setActive(null));
    } else {
      setActive(null);
    }
  }, [activeId]);

  const create = async () => {
    const conversation = await api.createConversation();
    setConversations((current) => [conversation, ...current]);
    navigateToConversation(conversation.id);
    setActive(conversation);
  };

  const remove = async (id: string) => {
    await api.deleteConversation(id);
    const remaining = conversations.filter((row) => row.id !== id);
    setConversations(remaining);
    if (activeId === id) {
      setActive(null);
      if (remaining[0]) {
        navigateToConversation(remaining[0].id, "replace");
      } else {
        navigateToChatRoot("replace");
      }
    }
  };

  return (
    <div className="flex h-screen min-h-0 bg-[var(--color-background)] text-[var(--color-foreground)]">
      <ConversationSidebar
        conversations={conversations}
        activeId={activeId}
        onCreate={create}
        onSelect={navigateToConversation}
        onDelete={remove}
        themeMode={theme.mode}
        onThemeModeChange={theme.setMode}
      />
      <ChatPane
        conversation={active}
        onRefresh={refreshChatState}
        onTitleUpdated={applyTitleUpdate}
      />
    </div>
  );
}

function routeConversationId() {
  const [, segment, conversationId] = window.location.pathname.split("/");
  if (segment !== "chat" || !conversationId) return null;
  try {
    return decodeURIComponent(conversationId);
  } catch {
    return null;
  }
}

function writeConversationRoute(id: string, mode: "push" | "replace") {
  const path = `/chat/${encodeURIComponent(id)}`;
  if (window.location.pathname === path) return;
  const method = mode === "replace" ? "replaceState" : "pushState";
  window.history[method](null, "", path);
}

function writeChatRootRoute(mode: "push" | "replace") {
  if (window.location.pathname === "/chat") return;
  const method = mode === "replace" ? "replaceState" : "pushState";
  window.history[method](null, "", "/chat");
}
