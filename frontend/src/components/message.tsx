/**
 * Render a chat message body using AI Elements primitives:
 *
 * - `<think>…</think>` blocks (MiniMax / DeepSeek / o1-style reasoning) are
 *   collapsed into the AI Elements `<Reasoning>` component.
 * - Everything else is rendered as streaming-safe markdown via `Streamdown`
 *   (the renderer that AI Elements itself uses internally).
 *
 * The `isStreaming` prop tells `Reasoning` whether to show a shimmer / count
 * up the duration; pass it from `useChat`'s `isLoading` for the tail message.
 */

import { Streamdown } from "streamdown";

import {
  Reasoning,
  ReasoningContent,
  ReasoningTrigger,
} from "@/components/ai-elements/reasoning";

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

export function MessageContent({
  content,
  isStreaming = false,
}: {
  content: string;
  isStreaming?: boolean;
}) {
  if (!content) return null;
  const segments = parseSegments(content);
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
          <Streamdown key={i} className="text-sm leading-relaxed">
            {seg.content}
          </Streamdown>
        );
      })}
    </div>
  );
}
