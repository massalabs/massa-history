import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Helmet } from "react-helmet-async";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import { ErrorMsg, Loading, Panel } from "../components/Bits";

export function Search() {
  const [params] = useSearchParams();
  const q = (params.get("q") ?? "").trim();
  const { client, network } = useAppState();
  const navigate = useNavigate();

  const query = useQuery({
    queryKey: ["search", network, q],
    queryFn: () => api.search(client, q),
    enabled: q.length > 0,
  });

  useEffect(() => {
    if (!query.data) return;
    const data = query.data.data as any;
    const kind = data.kind as string;
    const hit = data.hit;
    if (kind === "block" && hit) {
      navigate(`/block/${hit.id}`, { replace: true });
    } else if (kind === "operation" && hit) {
      navigate(`/op/${hit.id}`, { replace: true });
    } else if (kind === "address") {
      const a = data.address;
      if (a) {
        // If the address came back via the on-chain MNS lookup, preserve
        // the name as a query param so the address page can display a
        // "resolved from <name>.massa" badge.
        const via = data.mns_name as string | undefined;
        const target = via
          ? `/address/${a}?via=${encodeURIComponent(via)}`
          : `/address/${a}`;
        navigate(target, { replace: true });
      }
    } else if (kind === "slot" && hit) {
      const s = hit.slot;
      navigate(`/slot/${s.period}/${s.thread}`, { replace: true });
    }
  }, [query.data, navigate]);

  return (
    <>
      <Helmet>
        <title>Search — Massa</title>
      </Helmet>
      <Panel title="Search">
        <div className="mb-3 text-sm">
          Query: <span className="font-mono">{q || "(empty)"}</span>
        </div>
        {q.length === 0 ? (
          <div className="text-muted">
            Type a block id, op id, address, MNS name (e.g.{" "}
            <span className="font-mono">damip.massa</span>), or{" "}
            <span className="font-mono">period,thread</span>.
          </div>
        ) : query.isLoading ? (
          <Loading />
        ) : query.isError ? (
          <ErrorMsg err={query.error} />
        ) : (
          <pre className="text-xs bg-bg p-3 rounded border border-border overflow-auto">
            {JSON.stringify(query.data, null, 2)}
          </pre>
        )}
      </Panel>
    </>
  );
}
