import { useQuery } from "@tanstack/react-query";
import { Helmet } from "react-helmet-async";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import { ErrorMsg, Loading, Panel } from "../components/Bits";

/**
 * Lightweight API documentation page. We intentionally render the OpenAPI
 * doc ourselves rather than embedding Swagger UI or Redoc — both would add
 * ~300 kb gzipped and we already serve the JSON at /v1/openapi.json for
 * anyone who wants a full-featured client.
 */
export function ApiDocs() {
  const { client, network } = useAppState();
  const q = useQuery({
    queryKey: ["openapi", network, client.endpoints()],
    queryFn: () => api.openapi(client),
    staleTime: Infinity,
  });

  const spec = (q.data ?? null) as {
    info?: { title?: string; version?: string; description?: string };
    paths?: Record<string, Record<string, { summary?: string; tags?: string[] }>>;
  } | null;

  // Group paths by first tag so related endpoints are next to each other.
  const grouped: Record<string, Array<{ path: string; method: string; summary: string }>> = {};
  if (spec?.paths) {
    for (const [p, methods] of Object.entries(spec.paths)) {
      for (const [m, info] of Object.entries(methods)) {
        const tag = info.tags?.[0] ?? "Other";
        grouped[tag] ??= [];
        grouped[tag].push({ path: p, method: m.toUpperCase(), summary: info.summary ?? "" });
      }
    }
  }
  const tags = Object.keys(grouped).sort();

  return (
    <>
      <Helmet>
        <title>{`API — ${network}`}</title>
      </Helmet>
      <Panel
        title="API reference"
        action={
          <a
            className="btn text-xs"
            href={`${client.endpoints()[0] ?? ""}/v1/openapi.json`}
            target="_blank"
            rel="noreferrer"
          >
            Raw OpenAPI ↗
          </a>
        }
      >
        {q.isLoading ? (
          <Loading />
        ) : q.isError ? (
          <ErrorMsg err={q.error} />
        ) : !spec ? (
          <div className="text-muted text-sm">No spec returned.</div>
        ) : (
          <>
            <header className="mb-4">
              <h3 className="text-lg font-semibold">{spec.info?.title}</h3>
              <div className="text-muted text-sm">
                Version {spec.info?.version}
              </div>
              {spec.info?.description && (
                <p className="text-sm mt-2 text-muted whitespace-pre-wrap">
                  {spec.info.description}
                </p>
              )}
            </header>
            <div className="space-y-6">
              {tags.map((tag) => (
                <section key={tag}>
                  <h4 className="text-sm uppercase tracking-wide text-accent2 mb-2">
                    {tag}
                  </h4>
                  <table className="w-full text-sm">
                    <tbody>
                      {grouped[tag]
                        .sort((a, b) => a.path.localeCompare(b.path))
                        .map(({ path, method, summary }) => (
                          <tr
                            key={`${method}-${path}`}
                            className="border-t border-border"
                          >
                            <td className="py-1.5 px-2 w-16 align-top">
                              <span className="inline-block px-1.5 py-0.5 rounded bg-panel border border-border text-[10px] font-mono text-accent">
                                {method}
                              </span>
                            </td>
                            <td className="px-2 font-mono align-top break-all">
                              {path}
                            </td>
                            <td className="px-2 align-top text-muted">
                              {summary}
                            </td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </section>
              ))}
            </div>
          </>
        )}
      </Panel>
    </>
  );
}
