import { type ReactNode, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useAppState } from "../AppState";
import type { Network } from "../lib/types";

export function Layout({ children }: { children: ReactNode }) {
  const { network, setNetwork } = useAppState();
  const [query, setQuery] = useState("");
  const navigate = useNavigate();

  function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    const q = query.trim();
    if (!q) return;
    navigate(`/search?q=${encodeURIComponent(q)}`);
  }

  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b border-border bg-panel/60 backdrop-blur sticky top-0 z-10">
        <div className="max-w-6xl mx-auto px-3 sm:px-4 py-2 sm:py-3 flex flex-wrap items-center gap-2 sm:gap-4">
          <Link
            to="/"
            className="font-semibold text-fg text-lg no-underline whitespace-nowrap"
          >
            <span className="text-accent">Massa</span> Explorer
          </Link>
          <nav className="hidden md:flex gap-3 text-sm">
            <Link to="/" className="text-muted hover:text-fg no-underline">
              Home
            </Link>
            <Link to="/blocks" className="text-muted hover:text-fg no-underline">
              Blocks
            </Link>
            <Link to="/operations" className="text-muted hover:text-fg no-underline">
              Ops
            </Link>
            <Link to="/denunciations" className="text-muted hover:text-fg no-underline">
              Denunciations
            </Link>
            <Link to="/charts" className="text-muted hover:text-fg no-underline">
              Charts
            </Link>
            <Link to="/api" className="text-muted hover:text-fg no-underline">
              API
            </Link>
            <Link
              to="/settings"
              className="text-muted hover:text-fg no-underline"
            >
              Settings
            </Link>
          </nav>
          <form
            onSubmit={onSubmit}
            className="order-last sm:order-none w-full sm:flex-1 sm:max-w-xl sm:ml-2 flex gap-2"
            role="search"
          >
            <input
              type="text"
              aria-label="Search"
              placeholder="Block, op, address, or p,t…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="flex-1 min-w-0 bg-bg border border-border rounded-md px-3 py-1.5 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-accent2"
            />
            <button type="submit" className="btn" aria-label="Search">
              Go
            </button>
          </form>
          <select
            aria-label="Network"
            value={network}
            onChange={(e) => setNetwork(e.target.value as Network)}
            className="bg-bg border border-border rounded-md text-sm px-2 py-1.5 ml-auto sm:ml-0"
          >
            <option value="mainnet">Mainnet</option>
            <option value="buildnet">Buildnet</option>
          </select>
        </div>
      </header>
      <main className="flex-1">
        <div className="max-w-6xl mx-auto px-3 sm:px-4 py-4 sm:py-6">
          {children}
        </div>
      </main>
      <footer className="border-t border-border text-muted text-[11px] py-2 text-center px-3">
        <Link to="/settings" className="text-muted hover:text-fg no-underline">
          settings
        </Link>
        {" · "}
        <Link to="/api" className="text-muted hover:text-fg no-underline">
          api
        </Link>
      </footer>
    </div>
  );
}
