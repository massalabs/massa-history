import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { HelmetProvider } from "react-helmet-async";
import "./styles.css";
import App from "./App";
import { ErrorBoundary } from "./components/ErrorBoundary";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5_000,
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
});

// Surface unhandled promise rejections in the console; they usually indicate
// an indexer request that errored but wasn't wired through a useQuery hook.
if (typeof window !== "undefined") {
  window.addEventListener("unhandledrejection", (e) => {
    // eslint-disable-next-line no-console
    console.error("[massa-explorer] unhandled promise rejection", e.reason);
  });
}

// React Router needs a basename when the SPA is served from a sub-path
// (e.g. https://host/explorer/). Vite injects the configured `--base` here
// as `import.meta.env.BASE_URL` (always trailing-slashed, defaults to `/`).
const routerBasename = import.meta.env.BASE_URL.replace(/\/$/, "") || "/";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ErrorBoundary>
      <HelmetProvider>
        <QueryClientProvider client={queryClient}>
          <BrowserRouter basename={routerBasename}>
            <App />
          </BrowserRouter>
        </QueryClientProvider>
      </HelmetProvider>
    </ErrorBoundary>
  </React.StrictMode>,
);
