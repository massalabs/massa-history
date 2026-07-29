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
import {
  firstIncludedSlot,
  fmtMas,
  formatRelative,
  slotTimestampMs,
} from "../lib/format";
import { useChainParams } from "../hooks/useChainParams";
import { Paginator, usePaged } from "../components/Paginator";

/**
 * Newest-first recent operations. Same backend as the Home panel but on its
 * own route — lets users deep-link and paginate freely without the rest of
 * the Home context.
 */
export function Operations() {
  const { client, network } = useAppState();
  const params = useChainParams();
  const paged = usePaged(50);

  const q = useQuery({
    queryKey: [
      "ops-page",
      network,
      client.endpoints(),
      paged.cursor,
      paged.limit,
    ],
    queryFn: () => api.recentOps(client, paged.limit, paged.cursor, 4096),
    refetchInterval: paged.page === 0 ? 10_000 : false,
  });
  useEffect(() => {
    paged.setLastResponse(q.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q.data]);

  return (
    <>
      <Helmet>
        <title>{`Operations — ${network}`}</title>
      </Helmet>
      <Panel title="Recent operations">
        {q.isLoading ? (
          <Loading />
        ) : q.isError ? (
          <ErrorMsg err={q.error} />
        ) : (q.data?.data.length ?? 0) === 0 ? (
          <div className="text-muted text-sm">No operations yet.</div>
        ) : (
          <div className="overflow-x-auto -mx-3 sm:mx-0">
            <table className="w-full text-sm min-w-[640px]">
              <thead className="text-muted text-[11px] uppercase">
                <tr>
                  <th className="text-left py-1 px-2">Op</th>
                  <th className="text-left py-1 px-2">Kind</th>
                  <th className="text-left py-1 px-2">From</th>
                  <th className="text-left py-1 px-2">To</th>
                  <th
                    className="text-left py-1 px-2 whitespace-nowrap"
                    title="Slot where the operation was first included in a block."
                  >
                    Slot
                  </th>
                  <th className="text-right py-1 px-2">Fee</th>
                  <th
                    className="text-right py-1 px-2 whitespace-nowrap"
                    title="Time when the slot was produced (or 'pending' if not yet in a block)."
                  >
                    Included
                  </th>
                </tr>
              </thead>
              <tbody>
                {q.data!.data.map((op) => {
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
                      <td className="px-2">
                        {incSlot ? (
                          <SlotRef
                            slot={incSlot}
                            params={params}
                            timeMode="none"
                          />
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="px-2 text-right font-mono whitespace-nowrap">
                        {fmtMas(op.fee_nmas)}
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
          loading={q.isFetching}
          count={q.data?.data.length ?? 0}
          onPrev={paged.prev}
          onNext={paged.next}
        />
      </Panel>
    </>
  );
}
