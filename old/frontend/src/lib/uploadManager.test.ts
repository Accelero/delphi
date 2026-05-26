/**
 * UploadManager state-machine tests — the single highest-value test for
 * the upload feature. Drives the controller in isolation (no Uppy DOM)
 * via a fake driver that calls the manager's lifecycle hooks, with fake
 * timers for the auto-dismiss + recovery-poll assertions, and MSW for the
 * `/api/ingestion/*` calls.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";

import { server } from "../../test-utils/msw/server";
import { UploadManager, type UploadDriver } from "./uploadManager";

/** A fake driver that mirrors what the Uppy wiring does: on addFile it
 *  binds the file, then drives create → progress → complete through the
 *  manager. Each step is awaited so fake timers + microtasks interleave
 *  deterministically. */
function fakeDriver(
  manager: UploadManager,
  opts: {
    parts?: { part_number: number; etag: string }[];
    failComplete?: boolean;
  } = {},
): UploadDriver {
  let fileSeq = 0;
  return {
    addFile(file, meta) {
      const fileId = `f${fileSeq++}`;
      manager.bindFile(fileId, meta.taskId as string);
      void (async () => {
        try {
          await manager.onCreate(fileId, {
            filename: file.name,
            size: file.size,
            prefill: (meta.prefill as Record<string, unknown>) ?? {},
          });
        } catch {
          return; // create failed → task already marked failed
        }
        manager.onProgress(fileId, 0.5);
        manager.onProgress(fileId, 1);
        if (opts.failComplete) {
          // Simulate a transport error path by signalling upload-error.
          manager.onUploadError(fileId, "upload_failed");
          return;
        }
        await manager.onComplete(fileId, opts.parts ?? [{ part_number: 1, etag: "e" }]);
      })();
    },
  };
}

function file(name = "a.pdf", size = 10): File {
  return new File([new Uint8Array(size)], name, { type: "application/pdf" });
}

const createOk = (docId = "doc1") =>
  http.post("/api/ingestion/uploads", () =>
    HttpResponse.json({
      doc_id: docId,
      key: `tenants/t/${docId}`,
      upload_id: "u1",
      part_size_bytes: 8388608,
      part_url_ttl_secs: 900,
    }),
  );
const signOk = http.post(
  "/api/ingestion/uploads/:id/sign-part",
  () => HttpResponse.json({ url: "http://s3/part" }),
);

describe("UploadManager", () => {
  let manager: UploadManager;
  beforeEach(() => {
    manager = new UploadManager();
  });

  it("advances queued → creating → uploading → validating → ready", async () => {
    server.use(
      createOk("doc1"),
      signOk,
      http.post("/api/ingestion/uploads/:id/complete", () =>
        HttpResponse.json({ result: "ready", doc_id: "document:doc1" }),
      ),
    );
    manager.setDriver(fakeDriver(manager));
    manager.enqueue([file()], {});
    // queued synchronously.
    expect(manager.getSnapshot()[0].state).toBe("queued");
    // Let the async create→complete chain settle.
    await vi.waitFor(() => expect(manager.getSnapshot()[0].state).toBe("ready"));
    const t = manager.getSnapshot()[0];
    expect(t.docId).toBe("document:doc1");
    expect(t.progress).toBe(1);
  });

  it("maps a rejected /complete to failed with the reason", async () => {
    server.use(
      createOk("doc2"),
      signOk,
      http.post("/api/ingestion/uploads/:id/complete", () =>
        HttpResponse.json({ result: "rejected", reason: "content_type_mismatch" }),
      ),
    );
    manager.setDriver(fakeDriver(manager));
    manager.enqueue([file()], {});
    await vi.waitFor(() => expect(manager.getSnapshot()[0].state).toBe("failed"));
    expect(manager.getSnapshot()[0].reason).toBe("content_type_mismatch");
  });

  it("marks failed when create errors", async () => {
    server.use(
      http.post("/api/ingestion/uploads", () =>
        HttpResponse.json({ error: "nope" }, { status: 400 }),
      ),
    );
    manager.setDriver(fakeDriver(manager));
    manager.enqueue([file()], {});
    await vi.waitFor(() => expect(manager.getSnapshot()[0].state).toBe("failed"));
    expect(manager.getSnapshot()[0].reason).toBe("create_failed");
  });

  it("marks failed on an upload-error", async () => {
    server.use(createOk("doc3"), signOk);
    manager.setDriver(fakeDriver(manager, { failComplete: true }));
    manager.enqueue([file()], {});
    await vi.waitFor(() => expect(manager.getSnapshot()[0].state).toBe("failed"));
    expect(manager.getSnapshot()[0].reason).toBe("upload_failed");
  });

  it("recovers via poll when /complete throws, then succeeds", async () => {
    vi.useFakeTimers();
    server.use(
      createOk("doc4"),
      signOk,
      http.post("/api/ingestion/uploads/:id/complete", () =>
        HttpResponse.error(),
      ),
      http.get("/api/ingestion/uploads/:id", () =>
        HttpResponse.json({ state: "ready", doc_id: "document:doc4" }),
      ),
    );
    manager.setDriver(fakeDriver(manager));
    manager.enqueue([file()], {});
    // Let create+complete-throw settle (real microtasks under fake timers).
    await vi.waitFor(
      () => expect(manager.getSnapshot()[0].state).toBe("validating"),
      { interval: 1 },
    );
    // Advance the poll timer; the GET resolves ready.
    await vi.advanceTimersByTimeAsync(1100);
    await vi.waitFor(
      () => expect(manager.getSnapshot()[0].state).toBe("ready"),
      { interval: 1 },
    );
    vi.useRealTimers();
  });

  it("auto-dismisses a ready task after ~5s", async () => {
    vi.useFakeTimers();
    server.use(
      createOk("doc5"),
      signOk,
      http.post("/api/ingestion/uploads/:id/complete", () =>
        HttpResponse.json({ result: "ready", doc_id: "document:doc5" }),
      ),
    );
    manager.setDriver(fakeDriver(manager));
    manager.enqueue([file()], {});
    await vi.waitFor(
      () => expect(manager.getSnapshot()[0]?.state).toBe("ready"),
      { interval: 1 },
    );
    await vi.advanceTimersByTimeAsync(5100);
    expect(manager.getSnapshot()).toHaveLength(0);
    vi.useRealTimers();
  });

  it("dismiss removes a task", () => {
    manager.setDriver({ addFile: () => {} });
    manager.enqueue([file()], {});
    const id = manager.getSnapshot()[0].id;
    expect(manager.getSnapshot()).toHaveLength(1);
    manager.dismiss(id);
    expect(manager.getSnapshot()).toHaveLength(0);
  });

  it("fails (not stuck in queued) when the driver can't add the file", async () => {
    // Mirror uppyDriver: addFile throws (duplicate) → onAddFileError.
    manager.setDriver({
      addFile: (_file, meta) =>
        manager.onAddFileError(meta.taskId as string, "duplicate_file"),
    });
    manager.enqueue([file()], {});
    await vi.waitFor(() =>
      expect(manager.getSnapshot()[0].state).toBe("failed"),
    );
    expect(manager.getSnapshot()[0].reason).toBe("duplicate_file");
  });

  it("releases the uploader's file on success and on dismiss", async () => {
    const removed: string[] = [];
    server.use(
      createOk("doc6"),
      signOk,
      http.post("/api/ingestion/uploads/:id/complete", () =>
        HttpResponse.json({ result: "ready", doc_id: "document:doc6" }),
      ),
    );
    const base = fakeDriver(manager);
    manager.setDriver({
      addFile: base.addFile,
      removeFile: (id) => removed.push(id),
    });
    manager.enqueue([file()], {});
    await vi.waitFor(() => expect(manager.getSnapshot()[0].state).toBe("ready"));
    // The file was released to the uploader when the task reached ready, so
    // a re-upload of the same file won't collide as a duplicate.
    expect(removed).toContain("f0");
  });
});
