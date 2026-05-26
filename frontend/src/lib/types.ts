export type MessageRole = "user" | "assistant" | "system";

export type CitationEntry = {
  index: number;
  label: string;
  url?: string | null;
};

export type MessageDto = {
  id: string;
  role: MessageRole;
  content: string;
  parent_message_id?: string | null;
  citations: CitationEntry[];
  turn_id?: string | null;
  interrupted?: boolean;
  finish_reason?: string | null;
  created_at: string;
};

export type ConversationSummary = {
  id: string;
  title: string;
  updated_at: string;
};

export type ConversationDetail = ConversationSummary & {
  messages: MessageDto[];
};

export type ChatEvent =
  | { type: "turn_started"; turn_id: string }
  | { type: "user_message"; id: string; content: string }
  | { type: "citations"; citations: CitationEntry[] }
  | { type: "text_delta"; delta: string }
  | { type: "finish"; assistant_message_id: string; finish_reason: "stop" | "error" }
  | {
      type: "interrupted";
      assistant_message_id: string;
      content: string;
      finish_reason: "user_interrupted";
    }
  | { type: "clear"; reason: "cancelled" | "worker_lost" | "failed_before_commit" }
  | { type: "error"; message: string }
  | { type: "title_updated"; title: string };

export type ServerWsMessage =
  | { type: "subscribed"; conversation_id: string }
  | { type: "event"; conversation_id: string; event_id: string; event: ChatEvent }
  | { type: "resync_required"; conversation_id: string }
  | { type: "error"; code: string; message: string }
  | { type: "pong"; nonce?: string };
