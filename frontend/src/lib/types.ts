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

export type PresignedPart = {
  part_number: number;
  /** Always a PUT. */
  url: string;
  /** Only of interest to a client that signs a window of parts at once. */
  expires_at: string;
};

export type CreateUploadResponse = {
  upload_id: string;
  document_id: string;
  /** Browser uploaders need `{ uploadId, key }` from their create hook. */
  key: string;
  /** Server-owned. The client MUST slice at exactly this size. */
  part_size_bytes: number;
  part_count: number;
};

/** Geometry is not echoed — it was fixed at preflight and cannot change. */
export type RenewUploadResponse = {
  parts: PresignedPart[];
};

export type UploadedPart = {
  part_number: number;
  /** Quoted as S3 returned it; goes back verbatim in /complete. */
  etag: string;
  size: number;
};

/** What storage already holds. The basis for resuming an interrupted upload. */
export type UploadedPartsResponse = {
  part_size_bytes: number;
  part_count: number;
  parts: UploadedPart[];
};


export type CompleteUploadRequest = {
  /** Replace mode only. */
  if_match?: number | null;
  on_conflict?: "supersede" | "fail";
  title?: string | null;
  tags?: string[] | null;
  description?: string | null;
  metadata?: Record<string, unknown> | null;
};

/**
 * `accepted` and `rejected` are terminal. A 202 from /complete is not a
 * guarantee that a document exists.
 */
export type UploadStatusResponse =
  | { state: "uploading"; document_id: string }
  | { state: "scanning"; document_id: string }
  | { state: "accepted"; document_id: string; version: number; superseded: boolean }
  | { state: "rejected"; document_id: string; reason: string };


export type DocumentDto = {
  document_id: string;
  version: number;
  state: "active" | "deleted";
  index_state: "pending" | "current" | "failed";
  filename?: string | null;
  content_type?: string | null;
  byte_size?: number | null;
  checksum?: string | null;
  title?: string | null;
  tags: string[];
  description?: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
};

export type DocumentListResponse = {
  items: DocumentDto[];
  /** Opaque cursor: pass back as `cursor` to get the next page. */
  next: string | null;
};

export type ChatEvent =
  | { type: "turn_started"; turn_id: string }
  | { type: "user_message"; id: string; turn_id?: string | null; content: string }
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
