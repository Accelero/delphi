import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/feed")({
  component: Feed,
});

function Feed() {
  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-semibold">Feed</h1>
      <p className="text-sm text-[var(--muted-foreground)]">
        Daily digest of new papers per project. Coming soon.
      </p>
    </div>
  );
}
