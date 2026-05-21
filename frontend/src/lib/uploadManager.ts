/**
 * UploadManager — a plain TS controller (NOT a React component) that owns
 * the whole direct-to-S3 upload lifecycle, above the router so it survives
 * navigation. Pressing "Upload" hands files here and returns immediately;
 * ingestion continues in the background, surfaced by the always-mounted
 * `<UploadTracker/>`.
 *
 * It is a tiny observable (`subscribe` / `getSnapshot`) consumed via
 * React's `useSyncExternalStore` — no new state library.
 *
 * Lifecycle per task:
 *   queued → creating → uploading(progress) → validating → ready | failed
 *
 * The `/complete` response is authoritative (it's synchronous: validate →
 * extract → autofill → commit), so the happy path needs no polling. A
 * dropped/`timed-out` `/complete` falls back to a bounded recovery poll of
 * `GET /uploads/:id` (single timer, 1 s, ≤60 s) since the server may have
 * committed anyway.
 */

import { ulid } from "ulid";

import { api } from "@/lib/api";
import type {
  CompleteResponse,
  PartRef,
  UploadPrefill,
  UploadStatus,
} from "@/lib/api";

export type UploadState =
  | "queued"
  | "creating"
  | "uploading"
  | "validating"
  | "ready"
  | "failed";

export type UploadTask = {
  /** Client ULID until doc_id is known, then the doc_id. */
  id: string;
  filename: string;
  size: number;
  state: UploadState;
  /** 0..1 during `uploading`. */
  progress: number;
  docId?: string;
  /** Friendly reason code when `failed`. */
  reason?: string;
};

/** Minimal surface the manager needs from an Uppy-like driver, so the
 *  controller's state machine is unit-testable without the Uppy DOM. */
export interface UploadDriver {
  /** Add a file and start it. `meta` carries the per-file prefill +
   *  client task id (stashed at enqueue time, never closed over). */
  addFile(file: File, meta: Record<string, unknown>): void;
}

/** Timers/poll knobs (overridable in tests for fake timers). */
const READY_DISMISS_MS = 5_000;
const FAILED_DISMISS_MS = 30_000;
const POLL_INTERVAL_MS = 1_000;
const POLL_MAX_MS = 60_000;

type Listener = () => void;

export class UploadManager {
  private tasks: UploadTask[] = [];
  private listeners = new Set<Listener>();
  private timers = new Map<string, ReturnType<typeof setTimeout>>();
  private driver: UploadDriver | null = null;
  /** Maps the Uppy file id → current task id (which migrates ULID→docId). */
  private fileToTask = new Map<string, string>();

  /** Inject the Uppy driver (done by UploadProvider). Kept separate so
   *  the state machine can be unit-tested without Uppy. */
  setDriver(driver: UploadDriver): void {
    this.driver = driver;
  }

  // ---- observable ---------------------------------------------------------

  subscribe = (cb: Listener): (() => void) => {
    this.listeners.add(cb);
    return () => this.listeners.delete(cb);
  };

  getSnapshot = (): UploadTask[] => this.tasks;

  private emit(): void {
    // New array identity so useSyncExternalStore re-renders.
    this.tasks = [...this.tasks];
    for (const l of this.listeners) l();
  }

  private patch(taskId: string, fields: Partial<UploadTask>): void {
    const i = this.tasks.findIndex((t) => t.id === taskId);
    if (i < 0) return;
    this.tasks[i] = { ...this.tasks[i], ...fields };
    this.emit();
  }

  // ---- public API ---------------------------------------------------------

  /** Hand N files (with an optional single-file prefill) to the manager.
   *  Returns immediately; the route is free to reset/navigate. */
  enqueue(files: File[], prefill: UploadPrefill): void {
    for (const file of files) {
      const taskId = ulid();
      this.tasks = [
        ...this.tasks,
        {
          id: taskId,
          filename: file.name,
          size: file.size,
          state: "queued",
          progress: 0,
        },
      ];
      // Prefill is stashed on the file meta at enqueue time (not closed
      // over) so a later enqueue with a different prefill can't race the
      // createMultipartUpload callbacks of files still being created.
      //
      // Defer the driver start by a microtask so tasks are observably
      // `queued` the instant enqueue returns (the driver's create stage
      // flips them to `creating`); this keeps `queued` a real state and
      // lets the UI render the queue before uploads kick off.
      const driver = this.driver;
      if (driver) queueMicrotask(() => driver.addFile(file, { taskId, prefill }));
    }
    this.emit();
  }

  dismiss(taskId: string): void {
    this.clearTimer(taskId);
    this.tasks = this.tasks.filter((t) => t.id !== taskId);
    this.fileToTask.forEach((tid, fid) => {
      if (tid === taskId) this.fileToTask.delete(fid);
    });
    this.emit();
  }

  // ---- driver callbacks (called by the Uppy wiring) -----------------------

  /** Register an Uppy file id against its client task id. */
  bindFile(fileId: string, taskId: string): void {
    this.fileToTask.set(fileId, taskId);
  }

  private taskIdFor(fileId: string): string | undefined {
    return this.fileToTask.get(fileId);
  }

  /** Stage create: POST /uploads, returns the create response so the Uppy
   *  callback can hand uploadId/key back to Uppy. Promotes task id → docId. */
  async onCreate(
    fileId: string,
    args: {
      filename: string;
      content_type: string;
      size: number;
      prefill: UploadPrefill;
    },
  ): Promise<{ uploadId: string; key: string; partSize: number }> {
    const taskId = this.taskIdFor(fileId);
    if (!taskId) throw new Error("unknown file");
    this.patch(taskId, { state: "creating" });
    try {
      const res = await api.ingestion.createUpload({
        ...args.prefill,
        filename: args.filename,
        content_type: args.content_type,
        size: args.size,
      });
      // Promote the task id to the deterministic doc_id.
      this.promote(taskId, res.doc_id);
      this.fileToTask.set(fileId, res.doc_id);
      this.patch(res.doc_id, { state: "uploading", progress: 0 });
      return {
        uploadId: res.upload_id,
        key: res.key,
        partSize: res.part_size_bytes,
      };
    } catch {
      this.fail(taskId, "create_failed");
      throw new Error("create_failed");
    }
  }

  async onSignPart(fileId: string, partNumber: number): Promise<string> {
    const docId = this.taskIdFor(fileId);
    if (!docId) throw new Error("unknown file");
    const res = await api.ingestion.signUploadPart(docId, partNumber);
    return res.url;
  }

  onProgress(fileId: string, progress: number): void {
    const taskId = this.taskIdFor(fileId);
    if (!taskId) return;
    this.patch(taskId, { state: "uploading", progress });
  }

  /** Complete: the synchronous `/complete` response is authoritative. On a
   *  transport error/timeout, fall back to the recovery poll. */
  async onComplete(fileId: string, parts: PartRef[]): Promise<void> {
    const docId = this.taskIdFor(fileId);
    if (!docId) return;
    this.patch(docId, { state: "validating" });
    let res: CompleteResponse;
    try {
      res = await api.ingestion.completeUpload(docId, parts);
    } catch {
      // Dropped/timed-out /complete — the server may have committed.
      this.startRecoveryPoll(docId);
      return;
    }
    this.applyComplete(docId, res);
  }

  onUploadError(fileId: string, reasonCode?: string): void {
    const taskId = this.taskIdFor(fileId);
    if (!taskId) return;
    this.fail(taskId, reasonCode ?? "upload_failed");
  }

  // ---- internal -----------------------------------------------------------

  private applyComplete(docId: string, res: CompleteResponse): void {
    if (res.result === "ready") {
      this.succeed(docId, res.doc_id);
    } else if (res.result === "rejected") {
      this.fail(docId, res.reason);
    } else {
      // conflict
      this.fail(docId, "canonical_id_conflict");
    }
  }

  private promote(oldId: string, newId: string): void {
    const i = this.tasks.findIndex((t) => t.id === oldId);
    if (i < 0) return;
    this.tasks[i] = { ...this.tasks[i], id: newId, docId: newId };
    const t = this.timers.get(oldId);
    if (t) {
      this.timers.delete(oldId);
      this.timers.set(newId, t);
    }
    this.emit();
  }

  private succeed(taskId: string, docId: string): void {
    this.patch(taskId, {
      state: "ready",
      progress: 1,
      docId: docId.includes(":") ? docId : `document:${docId}`,
    });
    this.scheduleDismiss(taskId, READY_DISMISS_MS);
  }

  private fail(taskId: string, reason: string): void {
    this.patch(taskId, { state: "failed", reason });
    this.scheduleDismiss(taskId, FAILED_DISMISS_MS);
  }

  private scheduleDismiss(taskId: string, ms: number): void {
    this.clearTimer(taskId);
    this.timers.set(
      taskId,
      setTimeout(() => this.dismiss(taskId), ms),
    );
  }

  private clearTimer(taskId: string): void {
    const t = this.timers.get(taskId);
    if (t) {
      clearTimeout(t);
      this.timers.delete(taskId);
    }
  }

  /** Bounded recovery poll: GET /uploads/:id every POLL_INTERVAL_MS up to
   *  POLL_MAX_MS, resolving the real terminal outcome the dropped
   *  `/complete` response would have carried. */
  private startRecoveryPoll(docId: string): void {
    const started = Date.now();
    const tick = async (): Promise<void> => {
      // Stop if the task was dismissed.
      if (!this.tasks.some((t) => t.id === docId)) return;
      let status: UploadStatus | null = null;
      try {
        status = await api.ingestion.uploadStatus(docId);
      } catch {
        status = null;
      }
      if (status?.state === "ready") {
        this.succeed(docId, status.doc_id);
        return;
      }
      if (status?.state === "rejected") {
        this.fail(docId, status.reason);
        return;
      }
      if (Date.now() - started >= POLL_MAX_MS) {
        this.fail(docId, "timeout");
        return;
      }
      this.timers.set(docId, setTimeout(tick, POLL_INTERVAL_MS));
    };
    this.timers.set(docId, setTimeout(tick, POLL_INTERVAL_MS));
  }
}
