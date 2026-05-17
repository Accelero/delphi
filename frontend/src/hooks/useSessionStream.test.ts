/**
 * Parser tests for `StreamParser` — the AI SDK data-stream format
 * machine that drives `useSessionStream`.
 *
 * The wire format is line-prefixed (`0:`, `2:`, `3:`, `d:` followed by
 * a JSON payload and `\n`). The interesting failure mode in practice is
 * "the bytes arrived split somewhere in the middle of a JSON payload" —
 * `fetch().getReader().read()` makes no promises about chunk boundaries.
 * The tests below feed records at every conceivable split.
 */

import { describe, expect, it } from "vitest";

import { StreamParser, type ParsedRecord } from "./useSessionStream";

const enc = new TextEncoder();

function feedAt(parser: StreamParser, full: string, splits: number[]) {
  // splits is the list of byte offsets at which to cut the chunk.
  const bytes = enc.encode(full);
  let start = 0;
  for (const i of splits) {
    parser.push(bytes.slice(start, i));
    start = i;
  }
  parser.push(bytes.slice(start));
}

function collect(parser: StreamParser): ParsedRecord[] {
  return parser.take();
}

describe("StreamParser", () => {
  it("parses a single text record", () => {
    const p = new StreamParser();
    p.push(enc.encode('0:"hello"\n'));
    expect(collect(p)).toEqual([{ type: "text", value: "hello" }]);
  });

  it("parses a full turn end-to-end", () => {
    const p = new StreamParser();
    p.push(
      enc.encode(
        '2:[{"type":"citations","chunks":[{"n":1}]}]\n0:"part 1 "\n0:"part 2"\nd:{"finishReason":"stop"}\n',
      ),
    );
    expect(collect(p)).toEqual([
      { type: "data", value: [{ type: "citations", chunks: [{ n: 1 }] }] },
      { type: "text", value: "part 1 " },
      { type: "text", value: "part 2" },
      { type: "finish", value: { finishReason: "stop" } },
    ]);
  });

  it("buffers a split across the newline", () => {
    const p = new StreamParser();
    p.push(enc.encode('0:"hello'));
    expect(collect(p)).toEqual([]);
    p.push(enc.encode(' world"\n'));
    expect(collect(p)).toEqual([{ type: "text", value: "hello world" }]);
  });

  it("survives a split inside a JSON escape sequence", () => {
    const p = new StreamParser();
    p.push(enc.encode('0:"line1\\'));
    expect(collect(p)).toEqual([]);
    p.push(enc.encode('nline2"\n'));
    expect(collect(p)).toEqual([{ type: "text", value: "line1\nline2" }]);
  });

  it("handles many tiny chunk splits", () => {
    const full =
      '0:"hello "\n0:"world"\nd:{"finishReason":"stop"}\n';
    const p = new StreamParser();
    // Split at every single byte.
    const splits = Array.from({ length: full.length - 1 }, (_, i) => i + 1);
    feedAt(p, full, splits);
    expect(collect(p)).toEqual([
      { type: "text", value: "hello " },
      { type: "text", value: "world" },
      { type: "finish", value: { finishReason: "stop" } },
    ]);
  });

  it("preserves multi-byte UTF-8 across chunk boundaries", () => {
    // "héllo" = 0x68 0xc3 0xa9 0x6c 0x6c 0x6f when JSON-encoded as a
    // string. We cut between the two bytes of `é` to verify the
    // TextDecoder { stream: true } mode holds the partial char.
    const full = '0:"héllo"\n';
    const bytes = enc.encode(full);
    const eIndex = full.indexOf("é"); // char index
    // The string `0:"h` is 4 bytes; é begins at byte 4 (UTF-16→UTF-8
    // because all preceding chars are ASCII).
    const splitAfterFirstByteOfE = eIndex + 1;
    const p = new StreamParser();
    p.push(bytes.slice(0, splitAfterFirstByteOfE));
    expect(collect(p)).toEqual([]);
    p.push(bytes.slice(splitAfterFirstByteOfE));
    expect(collect(p)).toEqual([{ type: "text", value: "héllo" }]);
  });

  it("ignores malformed lines without crashing", () => {
    const p = new StreamParser();
    p.push(enc.encode("garbage\n0:not-json\n0:\"ok\"\n"));
    expect(collect(p)).toEqual([{ type: "text", value: "ok" }]);
  });

  it("ignores unknown tags but keeps consuming the stream", () => {
    const p = new StreamParser();
    p.push(enc.encode('9:"future"\n0:"hi"\n'));
    expect(collect(p)).toEqual([{ type: "text", value: "hi" }]);
  });

  it("parses error records", () => {
    const p = new StreamParser();
    p.push(enc.encode('3:"something broke"\n'));
    expect(collect(p)).toEqual([
      { type: "error", value: "something broke" },
    ]);
  });

  it("take() drains and resets the buffer", () => {
    const p = new StreamParser();
    p.push(enc.encode('0:"a"\n'));
    expect(collect(p)).toEqual([{ type: "text", value: "a" }]);
    // No more records — second take returns empty even though we
    // didn't push anything new.
    expect(collect(p)).toEqual([]);
    p.push(enc.encode('0:"b"\n'));
    expect(collect(p)).toEqual([{ type: "text", value: "b" }]);
  });

  it("flush discards an incomplete trailing line", () => {
    const p = new StreamParser();
    p.push(enc.encode('0:"good"\n0:"incomplete'));
    expect(collect(p)).toEqual([{ type: "text", value: "good" }]);
    p.flush();
    expect(collect(p)).toEqual([]);
  });
});
