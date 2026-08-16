import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate, useParams, useRouterState } from "@tanstack/react-router";
import { useCallback, useEffect, useState } from "react";
import { useThemeMode } from "../../hooks/useThemeMode";
import { api } from "../../lib/api";
import { conversationListQueryKey, conversationQueryKey } from "../../lib/chatQueries";
import type { ConversationDetail, ConversationSummary } from "../../lib/types";
import { UploadPage } from "../upload/UploadPage";
import { AppNavigation } from "./AppNavigation";
import { ChatPane } from "./ChatPane";
import { ConversationSidebar } from "./ConversationSidebar";

export function App() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const activeId = useParams({
    strict: false,
    select: (params) =>
      typeof params.conversationId === "string" ? params.conversationId : null
  });
  const theme = useThemeMode();
  const [navigationCollapsed, setNavigationCollapsed] = useState(readInitialNavigationCollapsed);
  const chatActive = pathname.startsWith("/chat");
  const uploadActive = pathname === "/upload";

  useEffect(() => {
    localStorage.setItem("delphi.navigationCollapsed", navigationCollapsed ? "true" : "false");
  }, [navigationCollapsed]);

  const conversationsQuery = useQuery({
    queryKey: conversationListQueryKey,
    queryFn: api.listConversations
  });

  const conversations = conversationsQuery.data ?? [];
  const activeConversationQuery = useQuery({
    queryKey: activeId ? conversationQueryKey(activeId) : ["chat", "conversation", "none"],
    queryFn: () => api.getConversation(activeId!),
    enabled: activeId !== null
  });
  const active = activeId ? activeConversationQuery.data ?? null : null;

  const navigateToConversation = useCallback(
    (conversationId: string, replace = false) =>
      navigate({
        to: "/chat/$conversationId",
        params: { conversationId },
        replace
      }),
    [navigate]
  );

  const navigateToChatRoot = useCallback(
    (replace = false) => navigate({ to: "/chat", replace }),
    [navigate]
  );

  useEffect(() => {
    if (activeId || pathname !== "/chat" || conversations.length === 0) return;
    void navigateToConversation(conversations[0].id, true);
  }, [activeId, conversations, navigateToConversation, pathname]);

  useEffect(() => {
    if (!activeId || activeConversationQuery.status !== "error") return;
    const error = activeConversationQuery.error as Error & { status?: number };
    if (error.status !== 404) return;

    const remaining = conversations.filter((conversation) => conversation.id !== activeId);
    queryClient.setQueryData(conversationListQueryKey, remaining);
    queryClient.removeQueries({ queryKey: conversationQueryKey(activeId) });
    if (remaining[0]) {
      void navigateToConversation(remaining[0].id, true);
    } else {
      void navigateToChatRoot(true);
    }
  }, [
    activeConversationQuery.error,
    activeConversationQuery.status,
    activeId,
    conversations,
    navigateToChatRoot,
    navigateToConversation,
    queryClient
  ]);

  const createMutation = useMutation({
    mutationFn: () => api.createConversation(),
    onSuccess: (conversation) => {
      queryClient.setQueryData<ConversationSummary[]>(conversationListQueryKey, (current = []) => [
        toConversationSummary(conversation),
        ...current.filter((row) => row.id !== conversation.id)
      ]);
      queryClient.setQueryData(conversationQueryKey(conversation.id), conversation);
      void navigateToConversation(conversation.id);
    }
  });

  const deleteMutation = useMutation({
    mutationFn: api.deleteConversation,
    onMutate: async (conversationId) => {
      await queryClient.cancelQueries({ queryKey: conversationListQueryKey });
      const previous = queryClient.getQueryData<ConversationSummary[]>(conversationListQueryKey);
      queryClient.setQueryData<ConversationSummary[]>(conversationListQueryKey, (current = []) =>
        current.filter((conversation) => conversation.id !== conversationId)
      );
      queryClient.removeQueries({ queryKey: conversationQueryKey(conversationId) });
      return { previous };
    },
    onError: (_error, _conversationId, context) => {
      if (context?.previous) {
        queryClient.setQueryData(conversationListQueryKey, context.previous);
      }
    },
    onSettled: () => {
      void queryClient.invalidateQueries({ queryKey: conversationListQueryKey });
    }
  });

  const create = useCallback(() => {
    createMutation.mutate();
  }, [createMutation]);

  const remove = useCallback(
    async (conversationId: string) => {
      const rows = queryClient.getQueryData<ConversationSummary[]>(conversationListQueryKey) ?? conversations;
      const remaining = rows.filter((conversation) => conversation.id !== conversationId);
      await deleteMutation.mutateAsync(conversationId);
      if (activeId !== conversationId) return;
      if (remaining[0]) {
        void navigateToConversation(remaining[0].id, true);
      } else {
        void navigateToChatRoot(true);
      }
    },
    [
      activeId,
      conversations,
      deleteMutation,
      navigateToChatRoot,
      navigateToConversation,
      queryClient
    ]
  );

  const refreshConversation = useCallback(
    async (conversationId: string) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: conversationListQueryKey }),
        queryClient.invalidateQueries({ queryKey: conversationQueryKey(conversationId) })
      ]);
    },
    [queryClient]
  );

  const resyncConversation = useCallback(
    async (conversationId: string) => {
      const [detail] = await Promise.all([
        queryClient.fetchQuery({
          queryKey: conversationQueryKey(conversationId),
          queryFn: () => api.getConversation(conversationId)
        }),
        queryClient.invalidateQueries({ queryKey: conversationListQueryKey })
      ]);
      return detail.messages;
    },
    [queryClient]
  );

  const applyTitleUpdate = useCallback(
    (conversationId: string, title: string) => {
      queryClient.setQueryData<ConversationSummary[]>(conversationListQueryKey, (current = []) =>
        current.map((conversation) =>
          conversation.id === conversationId ? { ...conversation, title } : conversation
        )
      );
      queryClient.setQueryData<ConversationDetail | undefined>(
        conversationQueryKey(conversationId),
        (current) => (current ? { ...current, title } : current)
      );
    },
    [queryClient]
  );

  return (
    <div className="flex h-screen min-h-0 bg-[var(--color-background)] text-[var(--color-foreground)]">
      <AppNavigation
        collapsed={navigationCollapsed}
        chatActive={chatActive}
        uploadActive={uploadActive}
        themeMode={theme.mode}
        onToggleCollapsed={() => setNavigationCollapsed((collapsed) => !collapsed)}
        onThemeModeChange={theme.setMode}
      />
      {chatActive && (
        <ConversationSidebar
          className="max-sm:hidden"
          conversations={conversations}
          activeId={activeId}
          onCreate={create}
          onDelete={remove}
        />
      )}
      {uploadActive ? (
        <UploadPage />
      ) : (
        <ChatPane
          conversation={active}
          onRefresh={refreshConversation}
          onResync={resyncConversation}
          onTitleUpdated={applyTitleUpdate}
        />
      )}
    </div>
  );
}

function readInitialNavigationCollapsed() {
  const stored = localStorage.getItem("delphi.navigationCollapsed");
  if (stored === "true") return true;
  if (stored === "false") return false;
  return window.matchMedia("(max-width: 640px)").matches;
}

function toConversationSummary(conversation: ConversationDetail): ConversationSummary {
  return {
    id: conversation.id,
    title: conversation.title,
    updated_at: conversation.updated_at
  };
}
