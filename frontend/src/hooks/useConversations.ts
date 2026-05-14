/**
 * Conversation queries / mutations.
 *
 * Single source of truth for the `["conversations"]` cache key — the
 * sidebar list and the `corpus.$sessionId` route share it. Mutations
 * invalidate the list and (for rename) the per-conversation cache.
 */

import {
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";

import { api, conversationKey, type Conversation } from "@/lib/api";

export const conversationsKey = ["conversations"] as const;
export function conversationKeyFor(key: string) {
  return ["conversation", key] as const;
}

export function useConversations() {
  return useQuery({
    queryKey: conversationsKey,
    queryFn: api.chat.listConversations,
  });
}

export function useConversation(key: string | undefined) {
  return useQuery({
    queryKey: conversationKeyFor(key ?? ""),
    queryFn: () => api.chat.getConversation(key!),
    enabled: !!key,
  });
}

export function useCreateConversation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: api.chat.createConversation,
    onSuccess: (created: Conversation) => {
      qc.invalidateQueries({ queryKey: conversationsKey });
      // Seed the per-conversation cache so the route load is instant.
      qc.setQueryData(conversationKeyFor(conversationKey(created.id)), {
        conversation: created,
        messages: [],
      });
    },
  });
}

export function useRenameConversation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ key, title }: { key: string; title: string }) =>
      api.chat.renameConversation(key, title),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: conversationsKey });
      qc.invalidateQueries({ queryKey: conversationKeyFor(vars.key) });
    },
  });
}

export function useDeleteConversation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ key }: { key: string }) => api.chat.deleteConversation(key),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: conversationsKey });
      qc.removeQueries({ queryKey: conversationKeyFor(vars.key) });
    },
  });
}
