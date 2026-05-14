/**
 * MessageBody — `[N]` citation marker resolution.
 *
 * Pure-function test on `rewriteCitations`; we avoid rendering the
 * full `MessageBody` here because Streamdown pulls in shiki + mermaid
 * + math at import time and bloats the harness. The rewrite is the
 * load-bearing piece — once `[N]` markers are rewritten to anchor
 * markup, markdown's HTML passthrough does the rest.
 */
import { describe, expect, test } from "vitest";

import { rewriteCitations, type CitationEntry } from "./MessageBody";

const TABLE: CitationEntry[] = [
  { n: 1, chunk_id: "chunk:abc", doc_id: "document:xyz", doc_title: "T1" },
  { n: 2, chunk_id: "chunk:def", doc_id: "document:xyz" },
];

describe("rewriteCitations", () => {
  test("resolved markers become anchor markup pointing at /feed", () => {
    const out = rewriteCitations("a [1] b [2] c", TABLE);
    expect(out).toContain(
      `<a class="citation" href="/feed?doc=document%3Axyz&chunk=chunk%3Aabc">[1]</a>`,
    );
    expect(out).toContain(
      `<a class="citation" href="/feed?doc=document%3Axyz&chunk=chunk%3Adef">[2]</a>`,
    );
  });

  test("unresolved markers pass through as plain text", () => {
    const out = rewriteCitations("foo [1] bar [99] baz [2]", TABLE);
    expect(out).toContain("[99]"); // unchanged
    expect(out).not.toContain('href="/feed?doc=&chunk="');
  });

  test("empty table is a no-op", () => {
    expect(rewriteCitations("[1] hello [2]", [])).toBe("[1] hello [2]");
  });

  test("multiple occurrences of the same marker all resolve", () => {
    const out = rewriteCitations("[1] and again [1]", TABLE);
    const occurrences = (out.match(/href="\/feed/g) ?? []).length;
    expect(occurrences).toBe(2);
  });
});
