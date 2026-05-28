import { useCallback, useEffect, useState } from "react";
import { useThemeMode } from "../../hooks/useThemeMode";
import { api } from "../../lib/api";
import type { ConversationDetail, ConversationSummary } from "../../lib/types";
import { ChatPane } from "./ChatPane";
import { ConversationSidebar } from "./ConversationSidebar";

export function App() {
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [active, setActive] = useState<ConversationDetail | null>(null);
  const theme = useThemeMode();

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
    if (!activeId && rows[0]) setActiveId(rows[0].id);
  }, [activeId]);

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
    api.me().then(refreshList).catch(() => undefined);
  }, []);

  useEffect(() => {
    if (activeId) refreshActive().catch(() => setActive(null));
  }, [activeId]);

  const create = async () => {
    const conversation = await api.createConversation();
    setConversations((current) => [conversation, ...current]);
    setActiveId(conversation.id);
    setActive(conversation);
  };

  const remove = async (id: string) => {
    await api.deleteConversation(id);
    setConversations((current) => current.filter((row) => row.id !== id));
    if (activeId === id) {
      setActiveId(null);
      setActive(null);
    }
  };

  return (
    <div className="flex h-screen min-h-0 bg-[var(--color-app)] text-[var(--color-text)]">
      <ConversationSidebar
        conversations={conversations}
        activeId={activeId}
        onCreate={create}
        onSelect={setActiveId}
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
