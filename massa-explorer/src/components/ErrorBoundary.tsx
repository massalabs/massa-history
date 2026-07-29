import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
  children: ReactNode;
}

interface State {
  error: Error | null;
}

/**
 * Top-level error boundary. Any render-time exception in the component tree
 * gets caught here so the app shows an actionable "Something went wrong"
 * screen instead of a blank white page. The user can retry without losing
 * their URL / route state.
 */
export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // Production builds want this in the browser console for support;
    // an operator can hook in Sentry/Highlight later without changing
    // the render path.
    // eslint-disable-next-line no-console
    console.error("[massa-explorer] uncaught render error", error, info);
  }

  private reset = () => this.setState({ error: null });

  render() {
    if (!this.state.error) return this.props.children;
    const msg = this.state.error.message || "Unknown error";
    return (
      <div className="min-h-screen bg-bg text-fg font-sans p-8">
        <div className="max-w-2xl mx-auto space-y-4">
          <h1 className="text-2xl font-semibold">Something went wrong</h1>
          <p className="text-fg2">
            The explorer hit an unexpected error while rendering this page.
            Your indexer may be unreachable or on an incompatible schema.
          </p>
          <pre className="whitespace-pre-wrap break-words rounded bg-surface p-4 text-sm text-red-300">
            {msg}
          </pre>
          <div className="flex gap-3">
            <button
              type="button"
              onClick={this.reset}
              className="rounded bg-accent px-4 py-2 text-bg hover:opacity-90"
            >
              Retry
            </button>
            <button
              type="button"
              onClick={() => window.location.reload()}
              className="rounded border border-fg2 px-4 py-2 hover:bg-surface"
            >
              Reload page
            </button>
          </div>
        </div>
      </div>
    );
  }
}
