/**
 * Renders the *body* of a chat message — splits `<think>…</think>` reasoning
 * blocks out into the AI Elements `<Reasoning>` collapsible, and renders the
 * rest as streaming-safe markdown via `Streamdown`.
 *
 * Note: this is the inner content. The styled bubble wrapper comes from AI
 * Elements' `<MessageContent>` (see `components/ai-elements/message.tsx`).
 *
 * `isStreaming` should be true only for the tail assistant message that's
 * still being generated; it makes the trailing reasoning block shimmer.
 */

import { useEffect, useRef, useState } from "react";
import { Streamdown } from "streamdown";
import { cjk } from "@streamdown/cjk";
import { code } from "@streamdown/code";
import { math } from "@streamdown/math";
import { mermaid } from "@streamdown/mermaid";

import {
  Reasoning,
  ReasoningContent,
  ReasoningTrigger,
} from "@/components/ai-elements/reasoning";

const staticPlugins = { cjk, code, math, mermaid };

/**
 * Drips a streamed string into the consumer at a controlled rate. The drip
 * itself is the only visual effect — content is fed to Streamdown
 * unanimated, so each unit (char or word) just appears as it's released.
 *
 * Rate ramps linearly with backlog: caught-up = `min` units/sec, larger
 * buffer = faster (unbounded). Runs continuously, so when the upstream
 * stream ends the buffer keeps draining to completion rather than snapping.
 */
type Granularity = "char" | "word";

const RATES: Record<Granularity, { min: number; k: number }> = {
  char: { min: 40, k: 2 },
  word: { min: 8, k: 1 },
};

function advanceByWords(target: string, fromLen: number, n: number): number {
  let pos = fromLen;
  let count = 0;
  while (pos < target.length && /\s/.test(target[pos])) pos++;
  while (count < n && pos < target.length) {
    while (pos < target.length && !/\s/.test(target[pos])) pos++;
    count++;
    while (pos < target.length && /\s/.test(target[pos])) pos++;
  }
  return pos;
}

function bufferUnits(
  target: string,
  shownLen: number,
  granularity: Granularity,
): number {
  if (granularity === "char") return target.length - shownLen;
  const rest = target.slice(shownLen).trim();
  if (!rest) return 0;
  return rest.split(/\s+/).length;
}

function useSmoothedContent(
  target: string,
  isStreaming: boolean,
  granularity: Granularity = "char",
): { shown: string } {
  // If already complete on first render (e.g. loaded from history),
  // skip the drip entirely.
  const initial = isStreaming ? "" : target;
  const [shown, setShown] = useState(initial);
  const targetRef = useRef(target);
  targetRef.current = target;
  const shownRef = useRef(initial);

  useEffect(() => {
    let raf = 0;
    let lastTs = 0;
    let acc = 0;
    const { min, k } = RATES[granularity];
    const tick = (ts: number) => {
      if (lastTs === 0) lastTs = ts;
      const dt = (ts - lastTs) / 1000;
      lastTs = ts;
      const t = targetRef.current;
      const s = shownRef.current;
      const buffer = bufferUnits(t, s.length, granularity);
      if (buffer > 0) {
        const rate = Math.max(min, min + k * buffer);
        acc += dt * rate;
        const units = Math.floor(acc);
        if (units > 0) {
          acc -= units;
          const newLen =
            granularity === "char"
              ? Math.min(s.length + units, t.length)
              : advanceByWords(t, s.length, units);
          const next = t.slice(0, newLen);
          shownRef.current = next;
          setShown(next);
        }
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [granularity]);

  return { shown };
}

type Segment =
  | { kind: "text"; content: string }
  | { kind: "think"; content: string; closed: boolean };

function parseSegments(text: string): Segment[] {
  const out: Segment[] = [];
  let i = 0;
  while (i < text.length) {
    const open = text.indexOf("<think>", i);
    if (open === -1) {
      out.push({ kind: "text", content: text.slice(i) });
      break;
    }
    if (open > i) {
      out.push({ kind: "text", content: text.slice(i, open) });
    }
    const contentStart = open + "<think>".length;
    const close = text.indexOf("</think>", contentStart);
    if (close === -1) {
      out.push({
        kind: "think",
        content: text.slice(contentStart),
        closed: false,
      });
      break;
    }
    out.push({
      kind: "think",
      content: text.slice(contentStart, close),
      closed: true,
    });
    i = close + "</think>".length;
  }
  return out.map((seg, idx) =>
    seg.kind === "text" && idx > 0
      ? { ...seg, content: seg.content.replace(/^\s+/, "") }
      : seg,
  );
}

export function MessageBody({
  content,
  isStreaming = false,
}: {
  content: string;
  isStreaming?: boolean;
}) {
  const { shown } = useSmoothedContent(content, isStreaming);
  if (!shown) return null;
  const segments = parseSegments(shown);
  return (
    <div className="space-y-2">
      {segments.map((seg, i) => {
        if (seg.kind === "think") {
          // The trailing reasoning block is "streaming" only when it's the
          // last segment AND not yet closed AND the message itself is
          // still streaming.
          const isLast = i === segments.length - 1;
          const reasoningStreaming = isStreaming && isLast && !seg.closed;
          return (
            <Reasoning
              key={i}
              isStreaming={reasoningStreaming}
              defaultOpen={false}
            >
              <ReasoningTrigger />
              <ReasoningContent>{seg.content}</ReasoningContent>
            </Reasoning>
          );
        }
        if (!seg.content.trim()) return null;
        return (
          <Streamdown
            key={i}
            plugins={staticPlugins}
            className="text-base leading-7"
          >
            {seg.content}
          </Streamdown>
        );
      })}
    </div>
  );
}
