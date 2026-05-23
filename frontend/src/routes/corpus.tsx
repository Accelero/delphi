/**
 * `/corpus` — parent (layout) route for the corpus-chat surface. It is a
 * transparent passthrough: its component is just `<Outlet />`.
 *
 * Bare `/corpus` renders the **draft** chat (`corpus.index.tsx`) — opening
 * this page always starts a fresh, session-less chat rather than dropping
 * you back into the most-recent conversation; existing conversations stay
 * reachable from the sidebar. The draft mints a conversation lazily on the
 * first message, so visiting `/corpus` never leaves a junk empty
 * conversation behind. Per-conversation state lives at
 * `/corpus/$sessionId` — see `corpus.$sessionId.tsx`.
 */
import { Outlet, createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/corpus")({
  component: Outlet,
});
