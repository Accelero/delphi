/**
 * Uppy wiring for the UploadManager.
 *
 * One Uppy instance per manager, `autoProceed: true` so files start as
 * soon as `enqueue` adds them. `@uppy/aws-s3` (multipart mode) drives the
 * create→sign→complete cycle; each callback delegates to the manager so
 * the state machine + recovery poll live in plain TS (testable without
 * the Uppy DOM).
 *
 * Per-file prefill is read from `file.meta.prefill` (stashed at enqueue
 * time), never closed over — so a second enqueue with a different prefill
 * can't race in-flight createMultipartUpload callbacks.
 */

import Uppy from "@uppy/core";
import AwsS3 from "@uppy/aws-s3";

import type { UploadDriver, UploadManager } from "@/lib/uploadManager";
import type { PartRef, UploadPrefill } from "@/lib/api";

const DEFAULT_PART_SIZE = 8 * 1024 * 1024; // INGEST_UPLOAD_PART_SIZE_BYTES default

export function createUppyDriver(manager: UploadManager): UploadDriver {
  const uppy = new Uppy({ autoProceed: true });

  // The backend's part size is a single deployment config value (same for
  // every file), so a closure suffices. `getChunkSize` only receives
  // `{ size }` in @uppy/aws-s3 v4 — it can't read per-file meta — so the
  // create callback refines this from the `/uploads` response.
  let partSize = DEFAULT_PART_SIZE;

  uppy.use(AwsS3, {
    // Boolean literal (not a predicate fn): tells Uppy *all* uploads are
    // multipart, so the options type doesn't also demand the
    // non-multipart `getUploadParameters` callback.
    shouldUseMultipart: true,
    createMultipartUpload: async (file) => {
      const prefill = (file.meta.prefill as UploadPrefill) ?? {};
      const res = await manager.onCreate(file.id, {
        filename: file.name ?? "upload",
        size: file.size ?? 0,
        prefill,
      });
      partSize = res.partSize || partSize;
      return { uploadId: res.uploadId, key: res.key };
    },
    signPart: async (file, { partNumber }) => {
      const url = await manager.onSignPart(file.id, partNumber);
      return { url };
    },
    completeMultipartUpload: async (file, { parts }) => {
      const refs: PartRef[] = parts.map((p) => ({
        part_number: p.PartNumber as number,
        etag: (p.ETag as string) ?? "",
      }));
      await manager.onComplete(file.id, refs);
      return {};
    },
    // We deliberately don't support resume-across-reload, and abort is a
    // no-op: cancel-mid-upload isn't exposed (closing the tab is enough),
    // and the nightly cleaner reaps any orphaned multipart. Both methods
    // are required by @uppy/aws-s3 v4's multipart types, so they're
    // present but inert.
    listParts: async () => [],
    abortMultipartUpload: async () => {},
    getChunkSize: () => partSize,
  });

  uppy.on("upload-progress", (file, progress) => {
    if (!file) return;
    const total = progress.bytesTotal ?? 0;
    const ratio = total > 0 ? (progress.bytesUploaded ?? 0) / total : 0;
    manager.onProgress(file.id, Math.min(ratio, 1));
  });

  uppy.on("upload-error", (file) => {
    if (!file) return;
    manager.onUploadError(file.id);
  });

  return {
    addFile(file: File, meta: Record<string, unknown>) {
      const taskId = meta.taskId as string;
      try {
        const fileId = uppy.addFile({
          name: file.name,
          type: file.type,
          data: file,
          meta,
        });
        manager.bindFile(String(fileId), taskId);
      } catch (err) {
        // uppy.addFile throws on a duplicate (same name/size/type/
        // lastModified as a file the uploader still tracks — e.g. retrying
        // the very same file) or any restriction violation. Without this
        // catch the throw is swallowed and the task hangs forever in
        // `queued`. Fail it with a reason instead.
        console.warn("[upload] uppy.addFile rejected", err);
        manager.onAddFileError(taskId, "duplicate_file");
      }
    },
    removeFile(fileId: string) {
      // Idempotent — the file may already be gone.
      try {
        uppy.removeFile(fileId);
      } catch {
        /* already removed */
      }
    },
  };
}
