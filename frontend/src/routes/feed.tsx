import { createFileRoute } from "@tanstack/react-router";

import { Feed } from "@/components/discovery/Feed";

export const Route = createFileRoute("/feed")({
  component: Feed,
});
