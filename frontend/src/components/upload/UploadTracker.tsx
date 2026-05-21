/**
 * UploadTracker — always-mounted, bottom-right notification stack showing
 * every in-flight / recently-finished upload task. Reads the manager via
 * `useUploadTasks`; hidden entirely when there are no tasks.
 *
 * Per row: filename, a state badge, a progress bar while uploading, a
 * spinner while validating, a check on ready (links to the feed), and an
 * error + reason on failed. `ready` rows auto-dismiss (~5 s) and `failed`
 * rows persist with an × (and self-expire ~30 s) — both handled by the
 * manager's timers; the × calls `dismiss`.
 */
import { Link } from "@tanstack/react-router";
import { Check, X, AlertCircle } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Spinner } from "@/components/ui/spinner";
import { documentKey } from "@/lib/api";
import { uploadRejectReason } from "@/lib/uploadRejectReason";
import type { UploadState, UploadTask } from "@/lib/uploadManager";
import { useUploadManager, useUploadTasks } from "./UploadProvider";

const STATE_LABEL: Record<UploadState, string> = {
  queued: "Queued",
  creating: "Starting",
  uploading: "Uploading",
  validating: "Processing",
  ready: "Done",
  failed: "Failed",
};

function badgeVariant(state: UploadState): "default" | "secondary" | "destructive" {
  if (state === "ready") return "default";
  if (state === "failed") return "destructive";
  return "secondary";
}

export function UploadTracker() {
  const tasks = useUploadTasks();
  const manager = useUploadManager();

  if (tasks.length === 0) return null;

  return (
    <div
      className="fixed bottom-4 right-4 z-50 flex w-80 flex-col gap-2"
      aria-label="Upload progress"
      role="status"
    >
      {tasks.map((t) => (
        <TrackerRow
          key={t.id}
          task={t}
          onDismiss={() => manager.dismiss(t.id)}
        />
      ))}
    </div>
  );
}

function TrackerRow({
  task,
  onDismiss,
}: {
  task: UploadTask;
  onDismiss: () => void;
}) {
  return (
    <div className="rounded-md border border-[var(--border)] bg-[var(--background)] p-3 text-sm shadow-md">
      <div className="flex items-center justify-between gap-2">
        <span className="truncate font-medium" title={task.filename}>
          {task.filename}
        </span>
        <div className="flex items-center gap-1.5">
          {task.state === "validating" && <Spinner className="size-4" />}
          {task.state === "ready" && (
            <Check className="size-4 text-green-600" aria-label="done" />
          )}
          {task.state === "failed" && (
            <AlertCircle className="size-4 text-destructive" aria-label="failed" />
          )}
          <Badge variant={badgeVariant(task.state)}>
            {STATE_LABEL[task.state]}
          </Badge>
          {task.state === "failed" && (
            <button
              type="button"
              onClick={onDismiss}
              aria-label="Dismiss"
              className="rounded p-0.5 hover:bg-[var(--accent)]"
            >
              <X className="size-3.5" />
            </button>
          )}
        </div>
      </div>

      {task.state === "uploading" && (
        <Progress value={Math.round(task.progress * 100)} className="mt-2" />
      )}

      {task.state === "failed" && task.reason && (
        <p className="mt-1 text-xs text-destructive">
          {uploadRejectReason(task.reason)}
        </p>
      )}

      {task.state === "ready" && task.docId && (
        <Link
          to="/feed"
          className="mt-1 inline-block text-xs text-primary hover:underline"
          title={documentKey(task.docId)}
        >
          View in feed
        </Link>
      )}
    </div>
  );
}
