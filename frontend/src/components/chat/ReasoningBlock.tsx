import { Brain, ChevronDown } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Streamdown } from "streamdown";
import { cjk } from "@streamdown/cjk";
import { code } from "@streamdown/code";
import { createMathPlugin } from "@streamdown/math";
import { mermaid } from "@streamdown/mermaid";
import { cn } from "../../lib/utils";

const reasoningPlugins = {
  cjk,
  code,
  math: createMathPlugin({ singleDollarTextMath: true }),
  mermaid
};

export function ReasoningBlock({
  content,
  streaming
}: {
  content: string;
  streaming: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [duration, setDuration] = useState<number | null>(null);
  const [startedAt, setStartedAt] = useState<number | null>(() => (streaming ? Date.now() : null));

  useEffect(() => {
    if (streaming && startedAt === null) {
      setStartedAt(Date.now());
      setDuration(null);
    }
    if (!streaming && startedAt !== null && duration === null) {
      setDuration(Math.max(1, Math.ceil((Date.now() - startedAt) / 1000)));
    }
  }, [duration, startedAt, streaming]);

  const label = useMemo(() => {
    if (streaming) return "Thinking...";
    if (duration === null) return "Reasoning";
    return `Thought for ${duration} second${duration === 1 ? "" : "s"}`;
  }, [duration, streaming]);

  return (
    <section className="not-prose mb-4">
      <button
        type="button"
        onClick={() => setOpen((current) => !current)}
        className="flex items-center gap-2 text-sm text-[var(--color-text-muted)] transition-colors hover:text-[var(--color-text)]"
        aria-expanded={open}
      >
        <Brain className="h-4 w-4" />
        <span className={streaming ? "animate-pulse" : undefined}>{label}</span>
        <ChevronDown className={cn("h-4 w-4 transition-transform", open && "rotate-180")} />
      </button>
      {open ? (
        <div className="mt-2 rounded-md border border-[var(--color-border)] bg-[var(--color-surface-muted)] px-3 py-2 text-sm italic text-[var(--color-text-muted)]">
          <Streamdown
            mode={streaming ? "streaming" : "static"}
            isAnimating={streaming}
            parseIncompleteMarkdown
            plugins={reasoningPlugins}
            controls={{ code: { copy: true, download: false }, mermaid: false, table: false }}
            dir="auto"
          >
            {content}
          </Streamdown>
        </div>
      ) : null}
    </section>
  );
}
