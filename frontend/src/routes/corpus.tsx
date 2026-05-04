import { createFileRoute } from "@tanstack/react-router";
import { useChat } from "@ai-sdk/react";

export const Route = createFileRoute("/corpus")({
  component: Corpus,
});

function Corpus() {
  const { messages, input, handleInputChange, handleSubmit, isLoading, error } =
    useChat({ api: "/api/chat" });

  return (
    <div className="flex flex-col h-full max-w-3xl mx-auto gap-4">
      <h1 className="text-2xl font-semibold">Chat with corpus</h1>

      <div className="flex-1 overflow-auto space-y-4 pr-2">
        {messages.length === 0 && (
          <p className="text-sm text-[var(--muted-foreground)]">
            Ask anything. v1 is plain LLM passthrough — corpus retrieval comes next.
          </p>
        )}
        {messages.map((m) => (
          <div
            key={m.id}
            className="rounded-md p-3 border border-[var(--border)]"
          >
            <div className="text-xs text-[var(--muted-foreground)] mb-1 capitalize">
              {m.role}
            </div>
            <div className="whitespace-pre-wrap text-sm">{m.content}</div>
          </div>
        ))}
        {error && (
          <div className="rounded-md p-3 border border-red-500/50 text-sm text-red-500">
            {error.message}
          </div>
        )}
      </div>

      <form onSubmit={handleSubmit} className="flex gap-2 pb-2">
        <input
          value={input}
          onChange={handleInputChange}
          placeholder="Type a message…"
          disabled={isLoading}
          className="flex-1 rounded-md border border-[var(--border)] bg-transparent px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-[var(--foreground)]"
        />
        <button
          type="submit"
          disabled={isLoading || !input.trim()}
          className="rounded-md border border-[var(--border)] px-4 py-2 text-sm hover:bg-[var(--muted)] disabled:opacity-50"
        >
          {isLoading ? "…" : "Send"}
        </button>
      </form>
    </div>
  );
}
