import { createFileRoute } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

export const Route = createFileRoute("/")({
  component: Home,
});

function Home() {
  const health = useQuery({
    queryKey: ["healthz"],
    queryFn: async () => {
      const r = await fetch("/healthz");
      if (!r.ok) throw new Error("backend unhealthy");
      return r.json() as Promise<{ status: string }>;
    },
  });

  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-semibold">delphi</h1>
      <p className="text-sm text-[var(--muted-foreground)]">
        Backend status: {health.isLoading ? "..." : health.data?.status ?? "unreachable"}
      </p>
    </div>
  );
}
