/**
 * Tests for `useChatStream` — the EventSource-driven multi-tab chat
 * hook. Stubs `window.EventSource` so the test can push named events
 * synchronously and assert state transitions.
 *
 * The reset rule is the load-bearing invariant: every `user_message`
 * event must clear the assistant overlay. The most important test
 * below is `replay-on-reconnect doesn't double-up text` — without the
 * reset rule it produces `hellohelloworld`.
 */

import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { useChatStream } from "./useChatStream";

type Handler = (ev: MessageEvent) => void;

/** Minimal stub matching the surface `useChatStream` consumes. */
class FakeEventSource {
  static instances: FakeEventSource[] = [];
  static reset() {
    this.instances = [];
  }

  readonly url: string;
  closed = false;
  private listeners = new Map<string, Set<Handler>>();

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  addEventListener(name: string, handler: Handler) {
    if (!this.listeners.has(name)) this.listeners.set(name, new Set());
    this.listeners.get(name)!.add(handler);
  }
  removeEventListener(name: string, handler: Handler) {
    this.listeners.get(name)?.delete(handler);
  }
  close() {
    this.closed = true;
  }

  /** Push a named SSE event. Bypasses the network entirely. */
  emit(event: string, data: unknown) {
    const handlers = this.listeners.get(event);
    if (!handlers) return;
    const ev = { data: JSON.stringify(data) } as MessageEvent;
    for (const h of handlers) h(ev);
  }
  emitOpen() {
    const handlers = this.listeners.get("open");
    if (!handlers) return;
    const ev = {} as MessageEvent;
    for (const h of handlers) h(ev);
  }
}

beforeEach(() => {
  FakeEventSource.reset();
  // @ts-expect-error — stub the global
  globalThis.EventSource = FakeEventSource;
});

afterEach(() => {
  // @ts-expect-error — drop the stub
  delete globalThis.EventSource;
});

function lastEs() {
  return FakeEventSource.instances[FakeEventSource.instances.length - 1];
}

describe("useChatStream", () => {
  it("opens an EventSource on mount and closes it on unmount", () => {
    const { unmount } = renderHook(() => useChatStream("conv-key"));
    const es = lastEs();
    expect(es.url).toBe("/api/chat/conversations/conv-key/stream");
    unmount();
    expect(es.closed).toBe(true);
  });

  it("seeds messages from initialMessages and tracks tail as parent_id", () => {
    const { result } = renderHook(() =>
      useChatStream("k", {
        initialMessages: [
          { id: "message:u1", role: "user", content: "hi" },
          { id: "message:a1", role: "assistant", content: "hello" },
        ],
      }),
    );
    expect(result.current.messages).toHaveLength(2);
    expect(result.current.status).toBe("ready");
  });

  it("user_message + text + finish drives the full happy path", () => {
    const { result } = renderHook(() => useChatStream("k"));
    const es = lastEs();
    act(() => {
      es.emit("user_message", { id: "message:u1", content: "hi" });
    });
    expect(result.current.status).toBe("submitted");
    expect(result.current.messages).toEqual([
      { id: "message:u1", role: "user", content: "hi" },
    ]);

    act(() => {
      es.emit("text", "hel");
    });
    expect(result.current.status).toBe("streaming");
    expect(result.current.messages[1]).toMatchObject({
      role: "assistant",
      content: "hel",
    });

    act(() => {
      es.emit("text", "lo");
    });
    expect(result.current.messages[1].content).toBe("hello");

    act(() => {
      es.emit("finish", {
        finishReason: "stop",
        assistantMessageId: "message:a1",
      });
    });
    expect(result.current.status).toBe("ready");
    expect(result.current.messages).toEqual([
      { id: "message:u1", role: "user", content: "hi" },
      { id: "message:a1", role: "assistant", content: "hello" },
    ]);
  });

  it("reset rule: replay-on-reconnect doesn't double-up text", () => {
    // Server delivered user_message + "hello" before the connection
    // dropped, then on reconnect replays user_message + "hello" + "world".
    // Without the reset rule the overlay would accumulate to
    // "hellohelloworld".
    const { result } = renderHook(() => useChatStream("k"));
    const es = lastEs();
    act(() => es.emit("user_message", { id: "message:u1", content: "hi" }));
    act(() => es.emit("text", "hello"));
    expect(result.current.messages[1].content).toBe("hello");

    // Reconnect: same user_message replayed → overlay reset.
    act(() => es.emit("user_message", { id: "message:u1", content: "hi" }));
    expect(result.current.messages).toEqual([
      { id: "message:u1", role: "user", content: "hi" },
    ]);

    act(() => es.emit("text", "hello"));
    act(() => es.emit("text", "world"));
    expect(result.current.messages[1].content).toBe("helloworld");
  });

  it("late-join: replay burst layers onto a committed seed", () => {
    // Switch-back scenario: the fresh mount seeds from committed history
    // (which does NOT contain the in-flight turn), then the new
    // EventSource replays the buffered burst — user_message + text —
    // which must appear on top of the committed messages.
    const { result } = renderHook(() =>
      useChatStream("k", {
        initialMessages: [{ id: "message:a0", role: "assistant", content: "prior" }],
      }),
    );
    const es = lastEs();
    // One network chunk → EventSource dispatches both frames in the same
    // task. React batches the setState calls into a single commit.
    act(() => {
      es.emit("user_message", { id: "message:u1", content: "hi" });
      es.emit("text", "partial");
    });
    expect(result.current.status).toBe("streaming");
    expect(result.current.messages).toEqual([
      { id: "message:a0", role: "assistant", content: "prior" },
      { id: "message:u1", role: "user", content: "hi" },
      { id: "__streaming-assistant__", role: "assistant", content: "partial" },
    ]);
  });

  it("late-join: a mid-stream seed refetch does not wipe the overlay", () => {
    // On remount, useConversation refetches (refetchOnMount). The resolved
    // committed history is re-passed as initialMessages WHILE the replay
    // stream is mid-flight. The seed-reset must not clobber the overlay.
    const seed = [{ id: "message:a0", role: "assistant" as const, content: "prior" }];
    const { result, rerender } = renderHook(
      ({ init }) => useChatStream("k", { initialMessages: init }),
      { initialProps: { init: seed } },
    );
    const es = lastEs();
    act(() => {
      es.emit("user_message", { id: "message:u1", content: "hi" });
      es.emit("text", "partial");
    });
    expect(result.current.messages).toHaveLength(3);

    // Background refetch resolves with the SAME committed history but a
    // fresh array identity (caller forgot to memoise / TanStack new ref).
    act(() => {
      rerender({ init: [{ id: "message:a0", role: "assistant", content: "prior" }] });
    });
    expect(result.current.status).toBe("streaming");
    expect(result.current.messages).toEqual([
      { id: "message:a0", role: "assistant", content: "prior" },
      { id: "message:u1", role: "user", content: "hi" },
      { id: "__streaming-assistant__", role: "assistant", content: "partial" },
    ]);
  });

  it("clear drops the optimistic user row and any overlay", () => {
    const { result } = renderHook(() => useChatStream("k"));
    const es = lastEs();
    act(() => es.emit("user_message", { id: "message:u1", content: "hi" }));
    act(() => es.emit("text", "partial"));
    expect(result.current.messages).toHaveLength(2);

    act(() => es.emit("clear", null));
    expect(result.current.messages).toEqual([]);
    expect(result.current.status).toBe("ready");
  });

  it("citations event sets the citations array", () => {
    const { result } = renderHook(() => useChatStream("k"));
    const es = lastEs();
    act(() =>
      es.emit("citations", [
        { n: 1, chunk_id: "chunk:1", doc_id: "document:x", page: 3 },
      ]),
    );
    expect(result.current.citations).toEqual([
      { n: 1, chunk_id: "chunk:1", doc_id: "document:x", page: 3 },
    ]);
  });

  it("named error event sets status=error and error message", () => {
    const { result } = renderHook(() => useChatStream("k"));
    const es = lastEs();
    act(() => es.emit("error", "llm stream error"));
    expect(result.current.status).toBe("error");
    expect(result.current.error).toBe("llm stream error");
  });

  it("reconciles committed history on a fresh connect (empty overlay)", () => {
    // Switch-back-in-same-tab scenario: the fresh mount has an empty
    // overlay and ready status. A turn may have committed while this
    // surface was unmounted (the tab that would have invalidated the
    // cache is gone), so the late joiner must refetch on connect rather
    // than trust its possibly-stale seed. Pre-fix this did NOT fire.
    const onTurnEnd = vi.fn();
    renderHook(() => useChatStream("k", { onTurnEnd }));
    const es = lastEs();
    act(() => es.emitOpen());
    expect(onTurnEnd).toHaveBeenCalledTimes(1);
  });

  it("fires onTurnEnd on reopen when overlay is non-empty", () => {
    const onTurnEnd = vi.fn();
    renderHook(() => useChatStream("k", { onTurnEnd }));
    const es = lastEs();
    // Simulate a turn in progress.
    act(() => es.emit("user_message", { id: "message:u1", content: "hi" }));
    act(() => es.emit("text", "partial"));
    onTurnEnd.mockClear();

    // Reopen (e.g. after transient disconnect): overlay non-empty →
    // caller should refetch.
    act(() => es.emitOpen());
    expect(onTurnEnd).toHaveBeenCalledTimes(1);
  });
});
