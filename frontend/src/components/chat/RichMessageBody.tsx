import { cjk } from "@streamdown/cjk";
import { code } from "@streamdown/code";
import { createMathPlugin } from "@streamdown/math";
import { mermaid } from "@streamdown/mermaid";
import { CheckIcon, CopyIcon } from "lucide-react";
import { useMemo } from "react";
import { Streamdown } from "streamdown";
import { useSmoothedContent } from "../../hooks/useSmoothedContent";
import type { CitationEntry } from "../../lib/types";
import { ReasoningBlock } from "./ReasoningBlock";

const markdownPlugins = {
  cjk,
  code,
  math: createMathPlugin({ singleDollarTextMath: true }),
  mermaid
};

const streamdownIcons = {
  CheckIcon,
  CopyIcon
};

type Segment =
  | { kind: "text"; content: string }
  | { kind: "think"; content: string; closed: boolean };

export default function RichMessageBody({
  content,
  streaming,
  citations = []
}: {
  content: string;
  streaming: boolean;
  citations?: CitationEntry[];
}) {
  const shown = useSmoothedContent(content, streaming);
  const segments = useMemo(() => parseSegments(shown), [shown]);

  if (!shown) return null;

  return (
    <div className="text-[15px] leading-7 text-[var(--color-text)]">
      {segments.map((segment, index) => {
        if (segment.kind === "think") {
          return (
            <ReasoningBlock
              key={`${index}-think`}
              content={segment.content}
              streaming={streaming && index === segments.length - 1 && !segment.closed}
            />
          );
        }

        const rendered = rewriteCitations(segment.content, citations);
        if (!rendered.trim()) return null;
        return (
          <Streamdown
            key={`${index}-text`}
            className="delphi-markdown"
            mode={streaming ? "streaming" : "static"}
            isAnimating={streaming}
            parseIncompleteMarkdown
            normalizeHtmlIndentation
            plugins={markdownPlugins}
            controls={{
              code: { copy: true, download: false },
              mermaid: { copy: true, download: true, fullscreen: true, panZoom: true },
              table: { copy: true, download: true, fullscreen: false }
            }}
            dir="auto"
            lineNumbers={false}
            caret="block"
            icons={streamdownIcons}
          >
            {rendered}
          </Streamdown>
        );
      })}
    </div>
  );
}

export function parseSegments(text: string): Segment[] {
  const segments: Segment[] = [];
  let cursor = 0;

  while (cursor < text.length) {
    const open = text.indexOf("<think>", cursor);
    if (open === -1) {
      segments.push({ kind: "text", content: text.slice(cursor) });
      break;
    }

    if (open > cursor) {
      segments.push({ kind: "text", content: text.slice(cursor, open) });
    }

    const contentStart = open + "<think>".length;
    const close = text.indexOf("</think>", contentStart);
    if (close === -1) {
      segments.push({ kind: "think", content: text.slice(contentStart), closed: false });
      break;
    }

    segments.push({
      kind: "think",
      content: text.slice(contentStart, close),
      closed: true
    });
    cursor = close + "</think>".length;
  }

  return segments.map((segment, index) =>
    segment.kind === "text" && index > 0
      ? { ...segment, content: segment.content.replace(/^\s+/, "") }
      : segment
  );
}

export function rewriteCitations(text: string, citations: CitationEntry[]): string {
  if (!citations.length) return text;

  const byIndex = new Map<number, CitationEntry>();
  for (const citation of citations) {
    byIndex.set(citation.index, citation);
  }

  return text.replace(/\[(\d+)]/g, (match, value) => {
    const citation = byIndex.get(Number(value));
    if (!citation?.url) return match;
    return `[${value}](${citation.url} "${escapeMarkdownTitle(citation.label)}")`;
  });
}

function escapeMarkdownTitle(value: string) {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}
