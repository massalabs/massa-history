import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Helmet } from "react-helmet-async";
import { useParams } from "react-router-dom";
import { useEffect } from "react";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import {
  AddrLink,
  BlockLink,
  CompletenessBadges,
  ErrorMsg,
  KV,
  Loading,
  NotFound,
  OpLink,
  Panel,
  SlotTimestamp,
  StatusBadge,
} from "../components/Bits";
import { fmtSlot, formatTs } from "../lib/format";
import { useChainParams } from "../hooks/useChainParams";
import { useSseSlots } from "../hooks/useSseSlots";
import { TransfersTable } from "../components/TransfersTable";
import { Paginator, usePaged, useLocalPaged } from "../components/Paginator";

export function SlotDetail() {
  const { period: ps, thread: ts } = useParams();
  const period = Number(ps);
  const thread = Number(ts);
  const { client, network } = useAppState();
  const params = useChainParams();
  const qc = useQueryClient();

  // While a slot is not yet final, poll every 3s. Once final, stop polling.
  const slot = useQuery({
    queryKey: ["slot", network, period, thread],
    queryFn: () => api.slot(client, period, thread),
    enabled: Number.isFinite(period) && Number.isFinite(thread),
    refetchInterval: (q) => (q.state.data?.data?.status === "final" ? false : 3_000),
  });

  const eventsPaged = usePaged(25);
  const events = useQuery({
    queryKey: [
      "slot-events",
      network,
      period,
      thread,
      eventsPaged.cursor,
      eventsPaged.limit,
    ],
    queryFn: () =>
      api.slotEvents(
        client,
        period,
        thread,
        eventsPaged.limit,
        eventsPaged.cursor,
      ),
    enabled: Number.isFinite(period) && Number.isFinite(thread),
    refetchInterval: () =>
      slot.data?.data?.status === "final" ? false : 5_000,
  });

  const transfersPaged = usePaged(25);
  const transfers = useQuery({
    queryKey: [
      "slot-transfers",
      network,
      period,
      thread,
      transfersPaged.cursor,
      transfersPaged.limit,
    ],
    queryFn: () =>
      api.slotTransfers(
        client,
        period,
        thread,
        transfersPaged.limit,
        transfersPaged.cursor,
      ),
    enabled: Number.isFinite(period) && Number.isFinite(thread),
    refetchInterval: () =>
      slot.data?.data?.completeness?.transfers_stored ? false : 5_000,
  });
  useEffect(() => {
    eventsPaged.setLastResponse(events.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [events.data]);
  useEffect(() => {
    transfersPaged.setLastResponse(transfers.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [transfers.data]);
  const execOpsPaged = useLocalPaged(50);

  // Subscribe to SSE: when the current slot updates, invalidate immediately
  // so the user sees finalization as it happens.
  const { events: sseEvents } = useSseSlots(client, 16);
  useEffect(() => {
    if (!sseEvents.length) return;
    const hit = sseEvents.find(
      (e) => e.slot.period === period && e.slot.thread === thread,
    );
    if (hit) {
      qc.invalidateQueries({ queryKey: ["slot", network, period, thread] });
      qc.invalidateQueries({
        queryKey: ["slot-events", network, period, thread],
      });
      qc.invalidateQueries({
        queryKey: ["slot-transfers", network, period, thread],
      });
    }
  }, [sseEvents, period, thread, network, qc]);

  if (!Number.isFinite(period) || !Number.isFinite(thread)) {
    return <ErrorMsg err="Bad slot coordinates" />;
  }
  if (slot.isLoading) return <Loading />;
  if (slot.isError) return <ErrorMsg err={slot.error} />;
  if (!slot.data) return <NotFound what={`slot ${fmtSlot({ period, thread })}`} />;

  const s = slot.data.data;
  const title = `Slot ${fmtSlot({ period, thread })} — Massa`;
  return (
    <>
      <Helmet>
        <title>{title}</title>
      </Helmet>
      <Panel
        title={`Slot ${fmtSlot({ period, thread })}`}
        action={<StatusBadge status={s.status} />}
      >
        <dl className="kv">
          <KV label="Timestamp">
            <SlotTimestamp slot={s.slot} params={params} mode="both" />
          </KV>
          <KV label="Status">
            <span className="inline-flex items-center gap-2 flex-wrap">
              <StatusBadge status={s.status} />
              {s.is_miss && (
                <span
                  className="text-muted text-xs"
                  title="No block was produced in this slot — the elected producer either was offline or didn't emit a block in time. The slot still exists and contributes to the chain timeline."
                >
                  no block produced (missed)
                </span>
              )}
            </span>
          </KV>
          <KV label="Final block">
            {s.final_block_id ? (
              <BlockLink id={s.final_block_id} short={false} />
            ) : s.status === "final" ? (
              <span className="text-muted">— (no block, slot missed)</span>
            ) : (
              <span className="text-muted">— (pending finalization)</span>
            )}
          </KV>
          <KV label="Candidates">
            {!s.candidate_block_ids?.length ? (
              "—"
            ) : (
              <div className="space-y-0.5">
                {s.candidate_block_ids.map((id) => (
                  <div key={id} className="break-all">
                    <BlockLink id={id} short={false} />
                    {s.final_block_id === id ? (
                      <span className="ml-2 text-xs text-ok">(winner)</span>
                    ) : s.status === "final" ? (
                      <span className="ml-2 text-xs text-bad">(discarded)</span>
                    ) : null}
                  </div>
                ))}
              </div>
            )}
          </KV>
          <KV label="Execution trace hash">
            <span
              className="font-mono break-all text-xs"
              title="Cumulative hash of the executed-output trail up to this slot — used by the node to detect divergent ledgers across peers."
            >
              {s.execution_trail_hash ?? "—"}
            </span>
          </KV>
          <KV label="First indexed">{formatTs(s.first_seen_ts_ms)}</KV>
          <KV label="Last updated">{formatTs(s.last_updated_ts_ms)}</KV>
          <KV label="Ingested data">
            <CompletenessBadges c={s.completeness} />
          </KV>
        </dl>
      </Panel>

      {s.executed_op_ids?.length ? (
        <>
          <div className="h-4" />
          <Panel
            title={`Executed operations (${s.executed_op_ids.length})`}
          >
            <ul className="divide-y divide-border text-sm">
              {s.executed_op_ids
                .slice(
                  execOpsPaged.offset,
                  execOpsPaged.offset + execOpsPaged.pageSize,
                )
                .map((id) => (
                  <li key={id} className="py-1 break-all">
                    <OpLink id={id} short={false} />
                  </li>
                ))}
            </ul>
            <Paginator
              page={execOpsPaged.page}
              pageSize={execOpsPaged.pageSize}
              hasMore={
                execOpsPaged.offset + execOpsPaged.pageSize <
                s.executed_op_ids.length
              }
              count={Math.max(
                0,
                Math.min(
                  execOpsPaged.pageSize,
                  s.executed_op_ids.length - execOpsPaged.offset,
                ),
              )}
              onPrev={execOpsPaged.prev}
              onNext={execOpsPaged.next}
            />
          </Panel>
        </>
      ) : null}

      <div className="h-4" />

      <Panel title="Transfers">
        {transfers.isLoading ? (
          <Loading />
        ) : transfers.isError ? (
          <ErrorMsg err={transfers.error} />
        ) : (
          <>
            <TransfersTable
              transfers={transfers.data?.data ?? []}
              showSlot={false}
              emptyLabel={
                s.completeness.transfers_stored
                  ? "No transfers in this slot."
                  : "Transfers not yet ingested for this slot."
              }
            />
            <Paginator
              page={transfersPaged.page}
              pageSize={transfersPaged.pageSize}
              hasMore={transfersPaged.hasMore}
              loading={transfers.isFetching}
              count={transfers.data?.data.length ?? 0}
              onPrev={transfersPaged.prev}
              onNext={transfersPaged.next}
            />
          </>
        )}
      </Panel>

      <div className="h-4" />

      <Panel title={`SC events (${s.sc_event_count})`}>
        {events.isLoading ? (
          <Loading />
        ) : events.isError ? (
          <ErrorMsg err={events.error} />
        ) : !events.data?.data.length ? (
          <div className="text-muted">No SC events.</div>
        ) : (
          <ul className="divide-y divide-border text-sm">
            {events.data.data.map((ev, i) => (
              <li key={i} className="py-2">
                <div className="flex items-center gap-2 mb-1 flex-wrap">
                  <span className="text-muted font-mono">
                    #{ev.index_in_slot}
                  </span>
                  <StatusBadge status={ev.status} />
                  {ev.op_id ? <OpLink id={ev.op_id} /> : null}
                </div>
                <div className="font-mono break-all text-xs">{ev.data}</div>
                {ev.emitter_addrs?.length ? (
                  <div className="text-xs text-muted mt-1 break-all">
                    emitters:{" "}
                    {ev.emitter_addrs.map((a, ai) => (
                      <span key={a}>
                        {ai > 0 ? ", " : ""}
                        <AddrLink addr={a} />
                      </span>
                    ))}
                  </div>
                ) : null}
                {ev.caller_addrs?.length ? (
                  <div className="text-xs text-muted break-all">
                    callers:{" "}
                    {ev.caller_addrs.map((a, ai) => (
                      <span key={a}>
                        {ai > 0 ? ", " : ""}
                        <AddrLink addr={a} />
                      </span>
                    ))}
                  </div>
                ) : null}
              </li>
            ))}
          </ul>
        )}
        <Paginator
          page={eventsPaged.page}
          pageSize={eventsPaged.pageSize}
          hasMore={eventsPaged.hasMore}
          loading={events.isFetching}
          count={events.data?.data.length ?? 0}
          onPrev={eventsPaged.prev}
          onNext={eventsPaged.next}
        />
      </Panel>
    </>
  );
}
