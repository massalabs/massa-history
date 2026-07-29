import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Helmet } from "react-helmet-async";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import {
  AddrLink,
  ErrorMsg,
  Loading,
  OpKindPill,
  OpLink,
  Panel,
  SlotRef,
} from "../components/Bits";
import { LiveDag } from "../components/LiveDag";
import {
  firstIncludedSlot,
  formatRelative,
  slotTimestampMs,
} from "../lib/format";
import { useChainParams } from "../hooks/useChainParams";
import { useSseSlots } from "../hooks/useSseSlots";
import { Paginator, usePaged } from "../components/Paginator";

export function Home() {
  const { client, network } = useAppState();
  const params = useChainParams();
  const status = useQuery({
    queryKey: ["status", network, client.endpoints()],
    queryFn: () => api.status(client),
    refetchInterval: 10_000,
  });
  const paged = usePaged(25);
  const recentOps = useQuery({
    queryKey: [
      "recent-ops",
      network,
      client.endpoints(),
      paged.cursor,
      paged.limit,
    ],
    queryFn: () => api.recentOps(client, paged.limit, paged.cursor, 2048),
    refetchInterval: paged.page === 0 ? 10_000 : false,
  });
  useEffect(() => {
    paged.setLastResponse(recentOps.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [recentOps.data]);
  const { connected } = useSseSlots(client, 4); // just used for the status indicator

  const lastFinal = status.data?.data.last_final_slot ?? null;

  return (
    <>
      <Helmet>
        <title>{`Massa Explorer — ${network}`}</title>
      </Helmet>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-6">
        <Panel title="Network">
          <div className="text-2xl font-semibold capitalize">{network}</div>
          <div className="text-muted text-xs mt-1 break-all">
            {status.isLoading ? null : status.isError ? (
              <ErrorMsg err={status.error} />
            ) : (
              <>
                <span className="font-mono">
                  {status.data?.data.build_version}
                </span>{" "}
                @ {client.endpoints().join(", ") || "—"} · SSE{" "}
                <span className={connected ? "text-ok" : "text-bad"}>
                  {connected ? "live" : "down"}
                </span>
              </>
            )}
          </div>
        </Panel>
        <Panel title="Last final slot">
          {lastFinal ? (
            <div>
              <SlotRef slot={lastFinal} params={params} timeMode="both" />
            </div>
          ) : (
            <div className="text-2xl font-mono">—</div>
          )}
          <div className="text-muted text-xs mt-1">
            updated {formatRelative(status.data?.data.meta.updated_at_ms)}
          </div>
        </Panel>
      </div>

      <div className="mb-4">
        <LiveDag client={client} params={params} windowSec={90} />
      </div>

      <Panel title="Latest operations">
        {recentOps.isLoading ? (
          <Loading />
        ) : recentOps.isError ? (
          <ErrorMsg err={recentOps.error} />
        ) : (recentOps.data?.data.length ?? 0) === 0 ? (
          <div className="text-muted text-sm">No operations yet.</div>
        ) : (
          <div className="overflow-x-auto -mx-3 sm:mx-0">
            <table className="w-full text-sm min-w-[520px]">
              <thead className="text-muted text-[11px] uppercase">
                <tr>
                  <th className="text-left py-1 px-2">Op</th>
                  <th className="text-left py-1 px-2">Kind</th>
                  <th className="text-left py-1 px-2">From</th>
                  <th className="text-left py-1 px-2">To</th>
                  <th
                    className="text-right py-1 px-2 whitespace-nowrap"
                    title="Wall-clock time of the slot in which the operation was first included in a block."
                  >
                    Included
                  </th>
                </tr>
              </thead>
              <tbody>
                {recentOps.data!.data.map((op) => {
                  const incSlot = firstIncludedSlot(op);
                  const incTs = incSlot
                    ? slotTimestampMs(incSlot, params)
                    : null;
                  return (
                    <tr key={op.id} className="border-t border-border align-top">
                      <td className="py-1.5 px-2">
                        <OpLink id={op.id} />
                      </td>
                      <td className="px-2">
                        <OpKindPill kind={op.kind} />
                      </td>
                      <td className="px-2">
                        <AddrLink addr={op.creator} />
                      </td>
                      <td className="px-2">
                        {op.target ? <AddrLink addr={op.target} /> : "—"}
                      </td>
                      <td className="px-2 text-right text-muted text-xs whitespace-nowrap">
                        {incTs != null ? formatRelative(incTs) : "pending"}
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
          loading={recentOps.isFetching}
          count={recentOps.data?.data.length ?? 0}
          onPrev={paged.prev}
          onNext={paged.next}
        />
      </Panel>
    </>
  );
}

