import AwsS3 from "@uppy/aws-s3";
import Uppy from "@uppy/core";
import { api } from "./api";
import type { CreateUploadResponse, UploadStatusResponse } from "./types";

const PART_UPLOAD_CONCURRENCY = 6;
/**
 * How many times the upload is re-entered after a recoverable failure. Each
 * pass resumes: preflight is not repeated, the `upload_id` and its multipart
 * survive, and Uppy skips every part storage already holds. Only a pass that
 * makes no progress at all costs anything.
 */
const UPLOAD_PASSES = 3;
const RESUME_BACKOFF_MS = 1_000;
const STATUS_POLL_INTERVAL_MS = 750;
/**
 * Scanning a large object is not instant, and the worker gets `max_deliver`
 * attempts before it gives up. Poll long enough to outlast that.
 */
const STATUS_POLL_TIMEOUT_MS = 10 * 60_000;

export type DocumentUploadInput = {
  file: File;
  title?: string | null;
  tags?: string[] | null;
  description?: string | null;
  metadata?: Record<string, unknown> | null;
  /** Supply to replace an existing document instead of creating one. */
  documentId?: string | null;
  /** The version the user was looking at, for replace mode. */
  ifMatch?: number | null;
  onConflict?: "supersede" | "fail";
};

export type DocumentUploadResult = {
  documentId: string;
  version: number;
  /** The upload replaced a version its author had not seen. */
  superseded: boolean;
};

export class UploadRejectedError extends Error {
  readonly reason: string;

  constructor(reason: string) {
    super(rejectionMessage(reason));
    this.name = "UploadRejectedError";
    this.reason = reason;
  }
}

export type DocumentUploadEvent =
  | { type: "preflight" }
  | {
      type: "uploading";
      uploadedBytes: number;
      totalBytes: number;
      partSizeBytes: number;
      partCount: number;
    }
  | { type: "resuming"; pass: number; passes: number }
  | { type: "scanning" }
  | { type: "accepted"; documentId: string; version: number; superseded: boolean };

export async function uploadDocumentWithUppy(
  input: DocumentUploadInput,
  onEvent: (event: DocumentUploadEvent) => void
): Promise<DocumentUploadResult> {
  onEvent({ type: "preflight" });

  // Preflight BEFORE constructing Uppy. Uppy's MultipartUploader fixes chunk
  // boundaries in its constructor, before `createMultipartUpload` runs, so a
  // part size fetched inside that hook would arrive after the file was already
  // sliced — and the server's geometry is the one S3 will be asked to assemble.
  const created = await api.createUpload({
    filename: input.file.name,
    size: input.file.size,
    content_type: input.file.type || null,
    document_id: input.documentId ?? null
  });

  // `created` survives across passes, which is what makes a retry a resume:
  // the same `upload_id` addresses the same multipart in storage.
  for (let pass = 1; ; pass += 1) {
    try {
      await runUploadPass(input, created, onEvent);
      break;
    } catch (error) {
      if (pass >= UPLOAD_PASSES || !isResumable(error)) {
        throw error;
      }
      onEvent({ type: "resuming", pass, passes: UPLOAD_PASSES });
      await sleep(RESUME_BACKOFF_MS * pass);
    }
  }

  // No parts. The server asks storage what it holds; see `completeMultipartUpload`.
  await api.completeUpload(created.upload_id, {
    if_match: input.ifMatch ?? null,
    on_conflict: input.onConflict ?? "supersede",
    title: input.title ?? null,
    tags: input.tags ?? null,
    description: input.description ?? null,
    metadata: input.metadata ?? null
  });

  // A 202 is not success. The document does not exist until the worker has
  // validated the bytes and appended an event, and it may never exist.
  onEvent({ type: "scanning" });
  const outcome = await pollUntilTerminal(created.upload_id, onEvent);
  onEvent({ type: "accepted", ...outcome });
  return outcome;
}

/**
 * One attempt at getting every part into storage.
 *
 * A fresh Uppy instance per pass is deliberate. Uppy asks `listParts` once, at
 * the start of an upload, and skips every part it names — so re-entering with a
 * new instance is exactly how a resume is expressed. Nothing that matters lives
 * in the instance; the state that counts is the multipart in storage.
 */
async function runUploadPass(
  input: DocumentUploadInput,
  created: CreateUploadResponse,
  onEvent: (event: DocumentUploadEvent) => void
): Promise<void> {
  let lastError: unknown = null;

  const uppy = new Uppy({
    autoProceed: false,
    allowMultipleUploadBatches: false,
    restrictions: {
      maxNumberOfFiles: 1,
      minFileSize: 1
    }
  });

  uppy.use(AwsS3, {
    shouldUseMultipart: true,
    limit: PART_UPLOAD_CONCURRENCY,
    // Server-owned. Uppy's own 10 000-part clamp can never fire, because the
    // server guarantees ceil(size / part_size) <= 10 000.
    getChunkSize: () => created.part_size_bytes,
    // Unreachable: seeding `s3Multipart` below puts every pass on Uppy's
    // restore path, which takes the ids from there. Kept because the plugin
    // requires the hook, and because echoing preflight is the honest answer.
    createMultipartUpload: async () => ({
      uploadId: created.upload_id,
      key: created.key
    }),
    // The resume mechanism. Uppy calls this before uploading anything and skips
    // every part named here. The ETags still come back because Uppy's own
    // bookkeeping wants them — but they no longer go anywhere: the server asks
    // storage directly at assembly time. The server only reports parts whose
    // length matches the geometry, so a half-written part is re-uploaded rather
    // than resumed.
    listParts: async () => {
      const listed = await api.listUploadedParts(created.upload_id);
      return listed.parts.map((part) => ({
        PartNumber: part.part_number,
        ETag: part.etag,
        Size: part.size
      }));
    },
    // Signed one part at a time, immediately before Uppy uploads it. Uppy runs
    // this through the same concurrency limit as the uploads, so only a handful
    // are ever in flight, and a URL is used seconds after it is minted — which
    // is why nothing here has to reason about the 300s expiry.
    signPart: async (_file, { partNumber, signal }) => {
      const { parts } = await api.renewUploadParts(
        created.upload_id,
        { from_part: partNumber, count: 1 },
        signal
      );
      const grant = parts[0];
      if (grant?.part_number !== partNumber) {
        throw new Error(`server did not sign part ${partNumber}`);
      }
      return { url: grant.url, method: "PUT" };
    },
    // Nothing to do, and nothing to report. The server asks storage what it
    // holds and assembles from that, so the ETags Uppy collected never leave
    // the browser. Calling S3's CompleteMultipartUpload from here would bypass
    // the work item that makes the pipeline crash-safe.
    completeMultipartUpload: async () => ({}),
    // Deliberately a no-op. Aborting would destroy the multipart that the next
    // pass resumes from, turning every recoverable blip into a full re-upload.
    // An upload nobody ever finishes stays an *incomplete* multipart, which
    // storage's own incomplete-upload reaper clears.
    abortMultipartUpload: async () => {}
  });

  uppy.on("upload-progress", (_file, progress) => {
    onEvent({
      type: "uploading",
      uploadedBytes: progress.bytesUploaded ?? 0,
      totalBytes: progress.bytesTotal ?? input.file.size,
      partSizeBytes: created.part_size_bytes,
      partCount: created.part_count
    });
  });

  uppy.on("upload-error", (_file, error) => {
    lastError = error;
  });

  try {
    const fileId = uppy.addFile({
      name: input.file.name,
      type: input.file.type,
      data: input.file
    });
    // This is what arms `listParts`. `MultipartUploader.start()` only takes the
    // restore path when the file already carries `{ uploadId, key }`; without
    // it Uppy calls `createMultipartUpload` and uploads every chunk blind,
    // which is a restart, not a resume. We have had both ids since preflight,
    // so every pass — including the first — can honestly say "resume this".
    uppy.setFileState(fileId, {
      s3Multipart: { uploadId: created.upload_id, key: created.key }
    } as never);
    const result = await uppy.upload();
    if ((result?.failed?.length ?? 0) > 0) {
      throw lastError instanceof Error ? lastError : new Error("upload failed");
    }
    if ((result?.successful?.length ?? 0) === 0) {
      // Uppy resolved without uploading anything. It used to be caught by the
      // harvested parts list being empty; with no list to inspect, the
      // uploader's own result is the signal that the pass did something.
      throw new Error("upload finished without processing the file");
    }
  } finally {
    uppy.destroy();
  }
}

/**
 * Whether another pass could plausibly get further.
 *
 * A 4xx from our own API is the upload itself being finished, expired, gone, or
 * not ours — the next pass makes the same call and gets the same answer, so
 * retrying only delays the error. Everything else (a dropped connection, a 5xx,
 * an S3 blip) is worth resuming through.
 */
function isResumable(error: unknown): boolean {
  if (error instanceof UploadRejectedError) return false;
  const status = (error as { status?: number } | null | undefined)?.status;
  return typeof status !== "number" || status < 400 || status >= 500;
}

async function pollUntilTerminal(
  uploadId: string,
  onEvent: (event: DocumentUploadEvent) => void
): Promise<DocumentUploadResult> {
  const deadline = Date.now() + STATUS_POLL_TIMEOUT_MS;
  while (Date.now() < deadline) {
    let status: UploadStatusResponse | null = null;
    try {
      status = await api.getUploadStatus(uploadId);
    } catch {
      // A blip in the status endpoint says nothing about the upload; keep going.
    }
    if (status?.state === "accepted") {
      return {
        documentId: status.document_id,
        version: status.version,
        superseded: status.superseded
      };
    }
    if (status?.state === "rejected") {
      throw new UploadRejectedError(status.reason);
    }
    if (status?.state === "scanning") {
      onEvent({ type: "scanning" });
    }
    await sleep(STATUS_POLL_INTERVAL_MS);
  }
  throw new Error("timed out waiting for the upload to be accepted");
}

/**
 * Reject reasons are stable tokens; the text is for humans. In every case the
 * recovery path is a **fresh upload**, never a retry of /complete: the work
 * item is deduplicated on the upload id, so re-completing does nothing.
 */
function rejectionMessage(reason: string): string {
  switch (reason) {
    case "malware_detected":
      return "This file was flagged by the malware scanner.";
    case "size_mismatch":
      return "The uploaded bytes did not match the declared size. Upload the file again.";
    case "invalid_parts":
      return "The uploaded parts were rejected by storage. Upload the file again.";
    case "multipart_lost":
      return "This upload expired before it was completed. Upload the file again.";
    case "version_conflict":
      return "The document changed while you were uploading. Reload and try again.";
    case "content_rejected":
      return "The file contents did not match the declared type.";
    default:
      return `The upload was rejected (${reason}). Upload the file again.`;
  }
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
