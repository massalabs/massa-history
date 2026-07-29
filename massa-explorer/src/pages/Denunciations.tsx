import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Helmet } from "react-helmet-async";
import { Link } from "react-router-dom";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import {
  AddrLink,
  ErrorMsg,
  Loading,
  Panel,
  SlotRef,
} from "../components/Bits";
import { formatRelative, slotTimestampMs } from "../lib/format";
import { useChainParams } from "../hooks/useChainParams";
import { Paginator, usePaged } from "../components/Paginator";

/** Newest-first denunciations. Links into the per-hash detail. */
export function Denunciations() {
  const { client, network } = useAppState();
  const params = useChainParams();
  const paged = usePaged(50);

  const q = useQuery({
    queryKey: [
      "recent-denunciations",
      network,
      client.endpoints(),
      paged.cursor,
      paged.limit,
    ],
    queryFn: () => api.recentDenunciations(client, paged.limit, paged.cursor),
    refetchInterval: paged.page === 0 ? 30_000 : false,
  });
  useEffect(() => {
    paged.setLastResponse(q.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q.data]);

  return (
    <>
      <Helmet>
        <title>{`Denunciations — ${network}`}</title>
      </Helmet>
      <Panel title="Denunciations">
        {q.isLoading ? (
          <Loading />
        ) : q.isError ? (
          <ErrorMsg err={q.error} />
        ) : (q.data?.data.length ?? 0) === 0 ? (
          <div className="text-muted text-sm">No denunciations yet.</div>
        ) : (
          <div className="overflow-x-auto -mx-3 sm:mx-0">
            <table className="w-full text-sm min-w-[560px]">
              <thead className="text-muted text-[11px] uppercase">
                <tr>
                  <th className="text-left py-1 px-2">Hash</th>
                  <th className="text-left py-1 px-2">Kind</th>
                  <th className="text-left py-1 px-2">Denounced</th>
                  <th className="text-left py-1 px-2">Slot</th>
                  <th className="text-right py-1 px-2 whitespace-nowrap">
                    Slot time
                  </th>
                </tr>
              </thead>
              <tbody>
                {q.data!.data.map((d) => {
                  const slotTs = d.slot
                    ? slotTimestampMs(d.slot, params)
                    : null;
                  return (
                    <tr key={d.hash} className="border-t border-border align-top">
                      <td className="py-1.5 px-2">
                        <Link
                          to={`/denunciation/${d.hash}`}
                          className="font-mono whitespace-nowrap text-accent2 no-underline hover:underline"
                        >
                          {d.hash.slice(0, 8)}…
                        </Link>
                      </td>
                      <td className="px-2 text-xs">{d.kind}</td>
                      <td className="px-2">
                        {d.denounced_addr ? (
                          <AddrLink addr={d.denounced_addr} />
                        ) : (
                          <span className="text-muted">—</span>
                        )}
                      </td>
                      <td className="px-2">
                        <SlotRef
                          slot={d.slot}
                          params={params}
                          timeMode="none"
                        />
                      </td>
                      <td className="px-2 text-right text-muted text-xs whitespace-nowrap">
                        {slotTs != null
                          ? formatRelative(slotTs)
                          : formatRelative(d.first_seen_ts_ms)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        <Paginator
          page={paged.page}
          pageSize={paged.pageSize}
          hasMore={paged.hasMore}
          loading={q.isFetching}
          count={q.data?.data.length ?? 0}
          onPrev={paged.prev}
          onNext={paged.next}
        />
      </Panel>
    </>
  );
}
