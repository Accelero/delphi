import { createFileRoute } from "@tanstack/react-router";
import { Chat } from "@/components/chat";

export const Route = createFileRoute("/corpus")({
  component: Corpus,
});

function Corpus() {
  return (
    <div className="flex flex-col h-full max-w-3xl mx-auto gap-4">
      <h1 className="text-2xl font-semibold">Chat with corpus</h1>
      <Chat
        api="/api/chat"
        emptyTitle="Chat with corpus"
        emptyDescription="Ask anything. v1 is plain LLM passthrough — corpus retrieval comes next."
      />
    </div>
  );
}
