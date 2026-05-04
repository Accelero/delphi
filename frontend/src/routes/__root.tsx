import { Outlet, createRootRoute, Link } from "@tanstack/react-router";
import { ThemeToggle } from "@/components/theme-toggle";

export const Route = createRootRoute({
  component: RootLayout,
});

function RootLayout() {
  return (
    <div className="flex h-full">
      <aside className="w-56 border-r border-[var(--border)] p-4 text-sm flex flex-col">
        <div className="font-semibold mb-4">delphi</div>
        <nav className="flex flex-col space-y-1">
          <Link to="/" className="hover:underline">Home</Link>
          <Link to="/feed" className="hover:underline">Feed</Link>
          <Link to="/corpus" className="hover:underline">Chat with corpus</Link>
        </nav>
        <div className="mt-auto pt-4">
          <ThemeToggle />
        </div>
      </aside>
      <main className="flex-1 p-6 overflow-auto">
        <Outlet />
      </main>
    </div>
  );
}
