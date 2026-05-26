import { useEffect, useState } from "react";
import { api } from "../../lib/api";
import type { ConversationDetail, ConversationSummary } from "../../lib/types";
import { ChatPane } from "./ChatPane";
import { ConversationSidebar } from "./ConversationSidebar";

export function App() {
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [active, setActive] = useState<ConversationDetail | null>(null);

  const refreshList = async () => {
    const rows = await api.listConversations();
    setConversations(rows);
    if (!activeId && rows[0]) setActiveId(rows[0].id);
  };

  const refreshActive = async () => {
    if (!activeId) return;
    setActive(await api.getConversation(activeId));
  };

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
    <div className="flex h-screen min-h-0 bg-white text-stone-950">
      <ConversationSidebar
        conversations={conversations}
        activeId={activeId}
        onCreate={create}
        onSelect={setActiveId}
        onDelete={remove}
      />
      <ChatPane conversation={active} onRefresh={refreshActive} />
    </div>
  );
}
