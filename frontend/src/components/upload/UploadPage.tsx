import {
  AlertCircle,
  CheckCircle2,
  FileText,
  FileUp,
  Loader2,
  Trash2,
  UploadCloud
} from "lucide-react";
import { DragEvent, FormEvent, ReactNode, useMemo, useRef, useState } from "react";
import { UploadRejectedError, uploadDocumentWithUppy } from "../../lib/documentUpload";
import { api } from "../../lib/api";
import { Button } from "../ui/button";

const FILE_UPLOAD_CONCURRENCY = 3;

/**
 * Mirrors the server's upload lifecycle. `scanning` is not cosmetic: the 202
 * from /complete means the work item is durable, not that a document exists,
 * so the client stays in this state until `GET /api/uploads/{id}` is terminal.
 */
type QueueItemStatus =
  | "queued"
  | "preflight"
  | "uploading"
  | "scanning"
  | "accepted"
  | "rejected"
  | "failed";

type QueueItem = {
  id: string;
  file: File;
  title: string;
  description: string;
  authors: string;
  tags: string;
  language: string;
  notes: string;
  /** Set to replace an existing document instead of creating one. */
  documentTarget: string;
  status: QueueItemStatus;
  uploadedBytes: number;
  totalParts: number;
  currentPart: number;
  documentId?: string;
  version?: number;
  superseded?: boolean;
  /** Warning shown when the replace target already has an upload in flight. */
  /** Set while a pass is being re-entered; cleared as soon as bytes move. */
  resumePass?: number;
  error?: string;
};

export function UploadPage() {
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [items, setItems] = useState<QueueItem[]>([]);
  const [dragging, setDragging] = useState(false);
  const [running, setRunning] = useState(false);

  const pendingCount = items.filter(
    (item) => item.status === "queued" || item.status === "failed" || item.status === "rejected"
  ).length;
  const active = items.some((item) => isActive(item.status));
  const acceptedCount = items.filter((item) => item.status === "accepted").length;
  const totalBytes = items.reduce((sum, item) => sum + item.file.size, 0);
  const uploadedBytes = items.reduce((sum, item) => sum + item.uploadedBytes, 0);
  const totalProgress = useMemo(() => {
    if (totalBytes === 0) return 0;
    return Math.min(100, Math.round((uploadedBytes / totalBytes) * 100));
  }, [totalBytes, uploadedBytes]);

  const addFiles = (files: FileList | File[]) => {
    const next = Array.from(files).filter((file) => file.size > 0);
    if (next.length === 0) return;
    setItems((current) => [
      ...current,
      ...next.map((file) => ({
        id: createQueueItemId(),
        file,
        title: filenameTitle(file.name),
        description: "",
        authors: "",
        tags: "",
        language: "",
        notes: "",
        documentTarget: "",
        status: "queued" as const,
        uploadedBytes: 0,
        totalParts: 0,
        currentPart: 0
      }))
    ]);
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  };

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (active || running) return;
    const uploadable = items.filter(
      (item) => item.status === "queued" || item.status === "failed" || item.status === "rejected"
    );
    if (uploadable.length === 0) return;

    setRunning(true);
    try {
      await runConcurrent(uploadable, FILE_UPLOAD_CONCURRENCY, uploadItem);
    } finally {
      setRunning(false);
    }
  };

  const uploadItem = async (item: QueueItem) => {
    const documentId = item.documentTarget.trim() || null;
    try {
      updateItem(item.id, {
        status: "preflight",
        uploadedBytes: 0,
        totalParts: 0,
        currentPart: 0,
        error: undefined,
      });

      // The version this replace is based on. There is no longer a warning for
      // "someone else is uploading to this document right now": uploads live in
      // a user-scoped KV keyspace that cannot be queried across users. The
      // server still detects the clash after the fact and reports `superseded`.
      let ifMatch: number | null = null;
      if (documentId) {
        ifMatch = (await api.getDocument(documentId)).version;
      }

      const result = await uploadDocumentWithUppy(
        {
          file: item.file,
          title: item.title.trim() || null,
          tags: splitList(item.tags),
          description: item.description.trim() || null,
          metadata: metadataFor(item),
          documentId,
          ifMatch
        },
        (event) => {
          if (event.type === "preflight") {
            updateItem(item.id, { status: "preflight" });
            return;
          }
          if (event.type === "uploading") {
            const totalParts = event.partCount;
            updateItem(item.id, {
              status: "uploading",
              uploadedBytes: Math.min(event.uploadedBytes, item.file.size),
              totalParts,
              currentPart: Math.min(
                totalParts,
                Math.max(1, Math.ceil(event.uploadedBytes / event.partSizeBytes))
              ),
              // Progress means the resume worked; stop saying it is resuming.
              resumePass: undefined
            });
            return;
          }
          if (event.type === "resuming") {
            updateItem(item.id, { status: "uploading", resumePass: event.pass });
            return;
          }
          if (event.type === "scanning") {
            updateItem(item.id, { status: "scanning", uploadedBytes: item.file.size });
            return;
          }
          if (event.type === "accepted") {
            updateItem(item.id, {
              status: "accepted",
              uploadedBytes: item.file.size,
              documentId: event.documentId,
              version: event.version,
              superseded: event.superseded
            });
          }
        }
      );
      updateItem(item.id, {
        status: "accepted",
        uploadedBytes: item.file.size,
        documentId: result.documentId,
        version: result.version,
        superseded: result.superseded
      });
    } catch (error) {
      // A rejection is terminal for this upload_id: re-completing is deduped
      // and does nothing. Recovery is always a fresh upload.
      updateItem(item.id, {
        status: error instanceof UploadRejectedError ? "rejected" : "failed",
        error: error instanceof Error ? error.message : "Upload failed."
      });
    }
  };

  const updateItem = (id: string, patch: Partial<QueueItem>) => {
    setItems((current) => current.map((item) => (item.id === id ? { ...item, ...patch } : item)));
  };

  const removeItem = (id: string) => {
    setItems((current) => current.filter((item) => item.id !== id));
  };

  const handleDrop = (event: DragEvent<HTMLButtonElement>) => {
    event.preventDefault();
    setDragging(false);
    addFiles(event.dataTransfer.files);
  };

  return (
    <main className="flex min-h-0 min-w-0 flex-1 flex-col bg-[var(--color-background)]">
      <div className="flex h-14 shrink-0 items-center justify-between border-b border-[var(--color-border)] px-6">
        <h1 className="text-sm font-semibold text-[var(--color-text)]">Upload documents</h1>
        {items.length > 0 && (
          <div className="text-xs text-[var(--color-text-muted)]">
            {acceptedCount} / {items.length} accepted
          </div>
        )}
      </div>

      <form className="flex min-h-0 flex-1 flex-col" onSubmit={submit}>
        <div className="min-h-0 flex-1 overflow-y-auto px-6 py-6">
          <div className="max-w-5xl space-y-5">
            <button
              type="button"
              className={
                dragging
                  ? "flex min-h-36 w-full items-center justify-center rounded-md border border-dashed border-[var(--color-focus)] bg-[var(--color-surface-hover)] px-4 py-6 text-left"
                  : "flex min-h-36 w-full items-center justify-center rounded-md border border-dashed border-[var(--color-border-strong)] bg-[var(--color-surface)] px-4 py-6 text-left hover:bg-[var(--color-surface-hover)]"
              }
              onClick={() => fileInputRef.current?.click()}
              onDragOver={(event) => {
                event.preventDefault();
                setDragging(true);
              }}
              onDragLeave={() => setDragging(false)}
              onDrop={handleDrop}
            >
              <span className="flex items-center gap-3">
                <FileUp className="h-5 w-5 text-[var(--color-text-muted)]" />
                <span className="min-w-0">
                  <span className="block text-sm font-medium text-[var(--color-text)]">
                    Drop files here or choose files
                  </span>
                  <span className="block text-xs text-[var(--color-text-muted)]">
                    Each file is uploaded directly to object storage, then validated and
                    scanned before it becomes a document.
                  </span>
                </span>
              </span>
            </button>
            <input
              ref={fileInputRef}
              type="file"
              multiple
              className="hidden"
              onChange={(event) => addFiles(event.target.files ?? [])}
            />

            {items.length > 0 && (
              <div className="space-y-2">
                <div className="h-2 overflow-hidden rounded-full bg-[var(--color-surface-muted)]">
                  <div
                    className="h-full bg-[var(--color-primary)] transition-[width]"
                    style={{ width: `${totalProgress}%` }}
                  />
                </div>
                <div className="text-xs text-[var(--color-text-muted)]">
                  {formatBytes(uploadedBytes)} / {formatBytes(totalBytes)}
                </div>
              </div>
            )}

            <div className="space-y-3">
              {items.map((item) => (
                <UploadRow
                  key={item.id}
                  item={item}
                  disabled={isActive(item.status)}
                  onChange={updateItem}
                  onRemove={removeItem}
                />
              ))}
            </div>
          </div>
        </div>

        <div className="shrink-0 border-t border-[var(--color-border)] bg-[var(--color-surface)] px-6 py-3">
          <div className="flex max-w-5xl items-center gap-3">
            <Button
              type="submit"
              disabled={items.length === 0 || pendingCount === 0 || active || running}
            >
              {active || running ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <UploadCloud className="h-4 w-4" />
              )}
              {active || running
                ? "Uploading"
                : pendingCount > 1
                  ? `Upload ${pendingCount} files`
                  : "Upload"}
            </Button>
            {items.length > 0 && !active && !running && (
              <Button type="button" variant="ghost" onClick={() => setItems([])}>
                Clear
              </Button>
            )}
          </div>
        </div>
      </form>
    </main>
  );
}

function UploadRow({
  item,
  disabled,
  onChange,
  onRemove
}: {
  item: QueueItem;
  disabled: boolean;
  onChange: (id: string, patch: Partial<QueueItem>) => void;
  onRemove: (id: string) => void;
}) {
  const progress = item.file.size === 0 ? 0 : Math.min(100, Math.round((item.uploadedBytes / item.file.size) * 100));
  return (
    <section className="rounded-md border border-[var(--color-border)] bg-[var(--color-surface)] p-4">
      <div className="flex items-start gap-3">
        <FileText className="mt-1 h-5 w-5 shrink-0 text-[var(--color-text-muted)]" />
        <div className="min-w-0 flex-1 space-y-3">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="truncate text-sm font-medium text-[var(--color-text)]">{item.file.name}</div>
              <div className="text-xs text-[var(--color-text-muted)]">{formatBytes(item.file.size)}</div>
            </div>
            <button
              type="button"
              className="grid h-8 w-8 shrink-0 place-items-center rounded-md text-[var(--color-text-muted)] hover:bg-[var(--color-surface-hover)] disabled:opacity-40"
              onClick={() => onRemove(item.id)}
              disabled={disabled}
              aria-label="Remove file"
              title="Remove file"
            >
              <Trash2 className="h-4 w-4" />
            </button>
          </div>

          <div className="grid gap-3 md:grid-cols-2">
            <Field label="Title">
              <input
                className="h-9 w-full rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 text-sm text-[var(--color-text)] outline-none focus:border-[var(--color-focus)] disabled:opacity-70"
                value={item.title}
                disabled={disabled}
                onChange={(event) => onChange(item.id, { title: event.target.value })}
              />
            </Field>
            <Field label="Description">
              <input
                className="h-9 w-full rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 text-sm text-[var(--color-text)] outline-none focus:border-[var(--color-focus)] disabled:opacity-70"
                value={item.description}
                disabled={disabled}
                placeholder="Optional"
                onChange={(event) => onChange(item.id, { description: event.target.value })}
              />
            </Field>
            <Field label="Replace document">
              <input
                className="h-9 w-full rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 font-mono text-xs text-[var(--color-text)] outline-none focus:border-[var(--color-focus)] disabled:opacity-70"
                value={item.documentTarget}
                disabled={disabled}
                placeholder="Document id — leave blank to create"
                onChange={(event) => onChange(item.id, { documentTarget: event.target.value })}
              />
            </Field>
            <Field label="Authors">
              <input
                className="h-9 w-full rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 text-sm text-[var(--color-text)] outline-none focus:border-[var(--color-focus)] disabled:opacity-70"
                value={item.authors}
                disabled={disabled}
                placeholder="Comma separated"
                onChange={(event) => onChange(item.id, { authors: event.target.value })}
              />
            </Field>
            <Field label="Tags">
              <input
                className="h-9 w-full rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 text-sm text-[var(--color-text)] outline-none focus:border-[var(--color-focus)] disabled:opacity-70"
                value={item.tags}
                disabled={disabled}
                placeholder="Comma separated"
                onChange={(event) => onChange(item.id, { tags: event.target.value })}
              />
            </Field>
            <Field label="Language">
              <input
                className="h-9 w-full rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 text-sm text-[var(--color-text)] outline-none focus:border-[var(--color-focus)] disabled:opacity-70"
                value={item.language}
                disabled={disabled}
                placeholder="Optional"
                onChange={(event) => onChange(item.id, { language: event.target.value })}
              />
            </Field>
            <Field label="Notes">
              <input
                className="h-9 w-full rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 text-sm text-[var(--color-text)] outline-none focus:border-[var(--color-focus)] disabled:opacity-70"
                value={item.notes}
                disabled={disabled}
                placeholder="Optional"
                onChange={(event) => onChange(item.id, { notes: event.target.value })}
              />
            </Field>
          </div>

          {(isActive(item.status) || isTerminal(item.status)) && (
            <div className="space-y-2">
              <div className="h-2 overflow-hidden rounded-full bg-[var(--color-surface-muted)]">
                <div
                  className={
                    isFailed(item.status)
                      ? "h-full bg-[var(--color-danger)]"
                      : "h-full bg-[var(--color-primary)] transition-[width]"
                  }
                  style={{ width: `${isFailed(item.status) ? 100 : progress}%` }}
                />
              </div>
              <ItemStatus item={item} />
            </div>
          )}
        </div>
      </div>
    </section>
  );
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="block">
      <span className="mb-1 block text-xs font-medium text-[var(--color-text-muted)]">{label}</span>
      {children}
    </label>
  );
}

function ItemStatus({ item }: { item: QueueItem }) {
  if (item.status === "accepted") {
    const version = item.version != null ? ` v${item.version}` : "";
    if (item.superseded) {
      // The uploader was looking at an older version, so someone else's change
      // was overwritten. Surfacing this is the whole point of `superseded`.
      return (
        <StatusLine
          tone="warning"
          text={`Accepted as ${item.documentId}${version} — your upload replaced a newer version`}
        />
      );
    }
    return <StatusLine tone="success" text={`Accepted: ${item.documentId}${version}`} />;
  }
  if (item.status === "rejected") {
    return <StatusLine tone="error" text={item.error ?? "Upload rejected."} />;
  }
  if (item.status === "failed") {
    return <StatusLine tone="error" text={item.error ?? "Upload failed."} />;
  }
  if (item.status === "preflight") {
    return <StatusLine tone="muted" text="Preparing upload" />;
  }
  if (item.status === "scanning") {
    return <StatusLine tone="muted" text="Validating and scanning" />;
  }
  if (item.resumePass != null) {
    // Not a restart: the parts already in storage are kept.
    return <StatusLine tone="muted" text={`Connection lost — resuming (attempt ${item.resumePass + 1})`} />;
  }
  return <StatusLine tone="muted" text={`Uploading part ${item.currentPart} of ${item.totalParts}`} />;
}

type StatusTone = "success" | "warning" | "error" | "muted";

function StatusLine({ tone, text }: { tone: StatusTone; text: string }) {
  const Icon =
    tone === "success" ? CheckCircle2 : tone === "muted" ? Loader2 : AlertCircle;
  const className =
    tone === "success"
      ? "flex items-center gap-2 text-xs text-[var(--color-text)]"
      : tone === "error"
        ? "flex items-center gap-2 text-xs text-[var(--color-danger)]"
        : tone === "warning"
          ? "flex items-center gap-2 text-xs text-[var(--color-warning,#b45309)]"
          : "flex items-center gap-2 text-xs text-[var(--color-text-muted)]";
  return (
    <div className={className}>
      <Icon className={tone === "muted" ? "h-4 w-4 animate-spin" : "h-4 w-4"} />
      <span className="min-w-0 truncate">{text}</span>
    </div>
  );
}

/**
 * Free-form metadata only. `title`, `tags`, and `description` are first-class
 * on the API and are sent separately, so they are deliberately not repeated
 * here — the projection would then hold two copies that could disagree.
 */
function metadataFor(item: QueueItem): Record<string, unknown> {
  return {
    authors: splitList(item.authors),
    language: item.language.trim() || null,
    notes: item.notes.trim() || null
  };
}

function splitList(value: string) {
  return value
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
}

async function runConcurrent<T>(
  items: T[],
  concurrency: number,
  task: (item: T) => Promise<void>
) {
  let nextIndex = 0;
  const workerCount = Math.min(Math.max(concurrency, 1), items.length);
  await Promise.all(
    Array.from({ length: workerCount }, async () => {
      while (nextIndex < items.length) {
        const item = items[nextIndex];
        nextIndex += 1;
        await task(item);
      }
    })
  );
}

function isActive(status: QueueItemStatus) {
  return status === "preflight" || status === "uploading" || status === "scanning";
}

function isTerminal(status: QueueItemStatus) {
  return status === "accepted" || status === "rejected" || status === "failed";
}

function isFailed(status: QueueItemStatus) {
  return status === "rejected" || status === "failed";
}

function filenameTitle(name: string) {
  return name.replace(/\.[^.]+$/, "").replace(/[-_]+/g, " ").trim();
}

function createQueueItemId() {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  const bytes = new Uint8Array(16);
  if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
    crypto.getRandomValues(bytes);
  } else {
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }

  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function formatBytes(value: number) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}
