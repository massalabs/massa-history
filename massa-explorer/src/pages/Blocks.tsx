import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Helmet } from "react-helmet-async";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import {
  AddrLink,
  BlockLink,
  BlockStatusBadge,
  ErrorMsg,
  Loading,
  Panel,
  SlotRef,
} from "../components/Bits";
import { formatRelative, slotTimestampMs } from "../lib/format";
import { useChainParams } from "../hooks/useChainParams";
import { Paginator, usePaged } from "../components/Paginator";

/**
 * Newest-first list of blocks (candidate + final, deduped per block id).
 *
 * Backend: `/v1/blocks` — walks the slot index in desc order and dereferences
 * block ids. We intentionally don't poll aggressively (10s) because the DAG
 * widget on the home page already delivers a real-time view of the head.
 */
export function Blocks() {
  const { client, network } = useAppState();
  const params = useChainParams();
  const paged = usePaged(25);

  const q = useQuery({
    queryKey: [
      "recent-blocks",
      network,
      client.endpoints(),
      paged.cursor,
      paged.limit,
    ],
    queryFn: () => api.recentBlocks(client, paged.limit, paged.cursor),
    refetchInterval: paged.page === 0 ? 10_000 : false,
  });
  useEffect(() => {
    paged.setLastResponse(q.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q.data]);

  return (
    <>
      <Helmet>
        <title>{`Blocks — ${network}`}</title>
      </Helmet>
      <Panel title="Recent blocks">
        {q.isLoading ? (
          <Loading />
        ) : q.isError ? (
          <ErrorMsg err={q.error} />
        ) : (q.data?.data.length ?? 0) === 0 ? (
          <div className="text-muted text-sm">No blocks yet.</div>
        ) : (
          <div className="overflow-x-auto -mx-3 sm:mx-0">
            <table className="w-full text-sm min-w-[600px]">
              <thead className="text-muted text-[11px] uppercase">
                <tr>
                  <th className="text-left py-1 px-2">Block</th>
                  <th className="text-left py-1 px-2">Slot</th>
                  <th className="text-left py-1 px-2">Creator</th>
                  <th className="text-right py-1 px-2">Ops</th>
                  <th className="text-right py-1 px-2">End.</th>
                  <th className="text-left py-1 px-2">Status</th>
                  <th className="text-right py-1 px-2 whitespace-nowrap">
                    Produced
                  </th>
                </tr>
              </thead>
              <tbody>
                {q.data!.data.map((b) => {
                  const slotTs = b.slot
                    ? slotTimestampMs(b.slot, params)
                    : null;
                  return (
                    <tr key={b.id} className="border-t border-border align-top">
                      <td className="py-1.5 px-2">
                        <BlockLink id={b.id} />
                      </td>
                      <td className="px-2">
                        <SlotRef
                          slot={b.slot}
                          params={params}
                          timeMode="none"
                        />
                      </td>
                      <td className="px-2">
                        <AddrLink addr={b.creator} />
                      </td>
                      <td className="px-2 text-right font-mono">
                        {b.operation_ids.length}
                      </td>
                      <td className="px-2 text-right font-mono">
                        {b.endorsement_ids.length}
                      </td>
                      <td className="px-2">
                        <BlockStatusBadge status={b.status} />
                      </td>
                      <td className="px-2 text-right text-muted text-xs whitespace-nowrap">
                        {slotTs != null
                          ? formatRelative(slotTs)
                          : formatRelative(b.first_seen_ts_ms)}
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
