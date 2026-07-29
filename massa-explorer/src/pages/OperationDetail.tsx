import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Helmet } from "react-helmet-async";
import { useParams } from "react-router-dom";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import {
  AddrLink,
  BlockLink,
  ErrorMsg,
  KV,
  Loading,
  NotFound,
  Panel,
  SlotRef,
} from "../components/Bits";
import { fmtMas, formatTs, shortId } from "../lib/format";
import { useChainParams } from "../hooks/useChainParams";
import { TransfersTable } from "../components/TransfersTable";
import { Paginator, usePaged } from "../components/Paginator";
import type {
  ExecStatus,
  OperationDetails,
  OperationKind,
  StoredOperation,
} from "../lib/types";

function fmtGas(g: number | null | undefined): string {
  if (g == null) return "—";
  return g.toLocaleString();
}

function ExecBadge({ status }: { status: ExecStatus | null | undefined }) {
  if (status == null)
    return <span className="text-muted">pending</span>;
  const cls =
    status === "ok"
      ? "badge-final"
      : "badge-miss";
  return <span className={`badge ${cls}`}>{status}</span>;
}

function HexBlob({
  hex,
  totalLen,
}: {
  hex: string;
  totalLen: number | null | undefined;
}) {
  const [open, setOpen] = useState(false);
  const byteLen = hex.length / 2;
  const truncated =
    typeof totalLen === "number" && totalLen > byteLen;
  if (hex.length === 0) {
    return <span className="text-muted">(empty)</span>;
  }
  const display = open || hex.length <= 160 ? hex : `${hex.slice(0, 160)}…`;
  return (
    <div className="space-y-1">
      <div className="font-mono text-xs break-all bg-panel rounded-md border border-border px-2 py-1">
        0x{display}
      </div>
      <div className="flex items-center gap-3 text-xs text-muted">
        <span>
          {byteLen.toLocaleString()} bytes
          {truncated && (
            <>
              {" "}
              (of {(totalLen as number).toLocaleString()}; remainder elided)
            </>
          )}
        </span>
        {hex.length > 160 && (
          <button
            className="underline"
            onClick={() => setOpen((v) => !v)}
          >
            {open ? "collapse" : "expand"}
          </button>
        )}
        <button
          className="underline"
          onClick={() => navigator.clipboard?.writeText(`0x${hex}`)}
        >
          copy
        </button>
      </div>
    </div>
  );
}

/** Render the kind-specific block (amount / function+params / roll count / …). */
function OpKindDetails({
  kind,
  details,
}: {
  kind: OperationKind;
  details: OperationDetails | undefined | null;
}) {
  const d = details ?? {};
  switch (kind) {
    case "transaction":
      return <KV label="Amount">{fmtMas(d.amount_nmas)}</KV>;
    case "roll_buy":
    case "roll_sell":
      return <KV label="Roll count">{d.roll_count ?? "—"}</KV>;
    case "call_sc":
      return (
        <>
          <KV label="Function">
            <span className="font-mono">
              {d.target_function && d.target_function.length > 0
                ? d.target_function
                : <span className="text-muted italic">(none)</span>}
            </span>
          </KV>
          <KV label="Parameter (hex)">
            <HexBlob
              hex={d.parameter_hex ?? ""}
              totalLen={d.parameter_len ?? null}
            />
          </KV>
          <KV label="Coins forwarded">{fmtMas(d.coins_nmas)}</KV>
          <KV label="Max gas">{fmtGas(d.max_gas)}</KV>
        </>
      );
    case "execute_sc":
      return (
        <>
          <KV label="Bytecode size">
            {d.bytecode_size != null
              ? `${d.bytecode_size.toLocaleString()} bytes`
              : "—"}
          </KV>
          <KV label="Max coins">{fmtMas(d.max_coins_nmas)}</KV>
          <KV label="Max gas">{fmtGas(d.max_gas)}</KV>
          {d.datastore_keys != null && d.datastore_keys > 0 && (
            <KV label="Datastore keys">{d.datastore_keys}</KV>
          )}
        </>
      );
    default:
      return null;
  }
}

export function OperationDetail() {
  const { id } = useParams();
  const { client, network } = useAppState();
  const params = useChainParams();
  const q = useQuery({
    queryKey: ["op", network, id],
    queryFn: () => api.operation(client, id!),
    enabled: !!id,
    refetchInterval: (query) =>
      query.state.data?.data?.final_exec_status ? false : 5_000,
  });
  const opFinal = q.data?.data?.final_exec_status ?? null;
  const paged = usePaged(25);
  // Remember the wall-clock at which the op was first observed as finalized.
  // `NewTransfersInfoServer` may arrive a few seconds *after* the exec-output
  // message flips `final_exec_status`, so we keep polling briefly afterwards
  // to pick up the finalized transfer rows. Without this the user sees a
  // stale empty table until they manually refresh the page.
  const finalizedAtRef = useRef<number | null>(null);
  if (opFinal && finalizedAtRef.current === null) {
    finalizedAtRef.current = Date.now();
  }
  const eventsPaged = usePaged(25);
  const events = useQuery({
    queryKey: [
      "op-events",
      network,
      id,
      eventsPaged.cursor,
      eventsPaged.limit,
    ],
    queryFn: () =>
      api.opEvents(client, id!, eventsPaged.limit, eventsPaged.cursor),
    enabled: !!id,
    refetchInterval: (query) => {
      if (!opFinal) return 3_000;
      const rows = (query.state.data?.data ?? []).length;
      const elapsed = finalizedAtRef.current
        ? Date.now() - finalizedAtRef.current
        : 0;
      if (rows === 0 && eventsPaged.page === 0 && elapsed < 60_000) return 3_000;
      return false;
    },
  });
  const transfers = useQuery({
    queryKey: ["op-transfers", network, id, paged.cursor, paged.limit],
    queryFn: () => api.opTransfers(client, id!, paged.limit, paged.cursor),
    // The node backfills transfers lazily, so we start loading as soon as we
    // know which op we're on. An empty page just renders the empty-state.
    enabled: !!id,
    refetchInterval: (query) => {
      if (!opFinal) return 3_000;
      // Post-finalization grace period (~60s) to let the transfer stream
      // catch up. After that we assume the op genuinely produced 0 rows
      // (e.g. failed before any ABI side-effect) and stop pinging.
      const rows = (query.state.data?.data ?? []).length;
      const elapsed = finalizedAtRef.current
        ? Date.now() - finalizedAtRef.current
        : 0;
      if (rows === 0 && paged.page === 0 && elapsed < 60_000) return 3_000;
      return false;
    },
  });
  useEffect(() => {
    paged.setLastResponse(transfers.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [transfers.data]);
  useEffect(() => {
    eventsPaged.setLastResponse(events.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [events.data]);
  if (!id) return <ErrorMsg err="missing id" />;
  if (q.isLoading) return <Loading />;
  if (q.isError) return <ErrorMsg err={q.error} />;
  if (!q.data) return <NotFound what={`operation ${shortId(id)}`} />;

  const op: StoredOperation = q.data.data;
  const title = `Op ${shortId(id)} — Massa`;
  return (
    <>
      <Helmet>
        <title>{title}</title>
      </Helmet>
      <Panel title={`Operation ${shortId(id)}`}>
        <dl className="kv">
          <KV label="ID">
            <span className="font-mono break-all">{op.id}</span>
          </KV>
          <KV label="Kind">
            <span className="inline-block px-1.5 py-0.5 rounded-full bg-panel border border-border text-[11px] uppercase tracking-wide">
              {op.kind.replace("_", " ")}
            </span>
          </KV>
          <KV label="From (creator)">
            <span title="The address that signed and broadcast this operation.">
              <AddrLink addr={op.creator} short={false} />
            </span>
          </KV>
          <KV
            label={
              op.kind === "call_sc"
                ? "To (called contract)"
                : op.kind === "transaction"
                ? "To (recipient)"
                : op.kind === "execute_sc"
                ? "Target (deployer)"
                : "Target"
            }
          >
            {op.target ? (
              <span
                title={
                  op.kind === "call_sc"
                    ? "Smart contract whose function this operation calls."
                    : op.kind === "transaction"
                    ? "Address that will receive the transferred coins."
                    : "Address this operation acts on."
                }
              >
                <AddrLink addr={op.target} short={false} />
              </span>
            ) : (
              <span
                className="text-muted"
                title="This operation kind has no explicit target (e.g. roll buy/sell affects only the creator)."
              >
                —
              </span>
            )}
          </KV>
          {/* kind-specific fields */}
          <OpKindDetails kind={op.kind} details={op.details} />
          <KV label="Fee">{fmtMas(op.fee_nmas)}</KV>
          <KV label="Expire period">{op.expire_period ?? "—"}</KV>
          <KV label="Thread">{op.thread}</KV>
          {(() => {
            // Prefer the explicit `inclusions` array (schema v2.1+); fall
            // back to the legacy singletons for rows ingested before that.
            // Same operation can legitimately land in several blocks — both
            // during normal multi-thread propagation AND inside competing
            // forks — so we always render a list, even when it's length 1.
            const fromList = op.inclusions ?? [];
            const incs =
              fromList.length > 0
                ? fromList
                : op.first_included_block_id && op.first_included_slot
                ? [
                    {
                      slot: op.first_included_slot,
                      block_id: op.first_included_block_id,
                    },
                  ]
                : [];
            const blockLabel = incs.length > 1 ? "Included in blocks" : "Included in block";
            const slotLabel = incs.length > 1 ? "Included in slots" : "Included in slot";
            return (
              <>
                <KV label={blockLabel}>
                  {incs.length === 0 ? (
                    <span className="text-muted">—</span>
                  ) : incs.length === 1 ? (
                    <BlockLink id={incs[0].block_id} short={false} />
                  ) : (
                    <ul className="space-y-0.5">
                      {incs.map((inc) => (
                        <li key={inc.block_id}>
                          <BlockLink id={inc.block_id} short={false} />
                        </li>
                      ))}
                    </ul>
                  )}
                </KV>
                <KV label={slotLabel}>
                  {incs.length === 0 ? (
                    <span className="text-muted">—</span>
                  ) : incs.length === 1 ? (
                    <SlotRef slot={incs[0].slot} params={params} timeMode="both" />
                  ) : (
                    <ul className="space-y-0.5">
                      {incs.map((inc) => (
                        <li key={`${inc.slot.period}-${inc.slot.thread}-${inc.block_id}`}>
                          <SlotRef slot={inc.slot} params={params} timeMode="both" />
                        </li>
                      ))}
                    </ul>
                  )}
                </KV>
              </>
            );
          })()}
          <KV label="Candidate exec">
            <ExecBadge status={op.candidate_exec_status} />
          </KV>
          <KV label="Final exec">
            <ExecBadge status={op.final_exec_status} />
          </KV>
          <KV label="First seen">{formatTs(op.first_seen_ts_ms)}</KV>
        </dl>
      </Panel>

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
              emptyLabel={
                op.final_exec_status
                  ? "No transfers emitted by this operation."
                  : "Waiting for final execution to load transfers."
              }
            />
            <Paginator
              page={paged.page}
              pageSize={paged.pageSize}
              hasMore={paged.hasMore}
              loading={transfers.isFetching}
              count={transfers.data?.data.length ?? 0}
              onPrev={paged.prev}
              onNext={paged.next}
            />
          </>
        )}
      </Panel>

      <div className="h-4" />

      <Panel title="SC events">
        {events.isLoading ? (
          <Loading />
        ) : events.isError ? (
          <ErrorMsg err={events.error} />
        ) : (events.data?.data.length ?? 0) === 0 ? (
          <div className="text-muted text-sm">
            {op.final_exec_status
              ? "No smart-contract events emitted."
              : "Waiting for final execution to load events."}
          </div>
        ) : (
          <>
            <ul className="space-y-1 text-sm">
              {events.data!.data.map((e) => (
                <li
                  key={`${e.slot.period}-${e.slot.thread}-${e.index_in_slot}`}
                  className="border-t border-border py-1"
                >
                  <div className="flex items-start gap-3">
                    <span className="text-muted text-xs whitespace-nowrap">
                      <SlotRef slot={e.slot} params={params} timeMode="none" /> #
                      {e.index_in_slot}
                    </span>
                    <span className="font-mono break-all text-xs flex-1">
                      {e.data}
                    </span>
                  </div>
                  {(e.emitter_addrs.length > 0 || e.caller_addrs.length > 0) && (
                    <div className="text-[11px] text-muted mt-0.5 flex flex-wrap gap-2">
                      {e.emitter_addrs.length > 0 && (
                        <span>
                          emitter:{" "}
                          {e.emitter_addrs.map((a, i) => (
                            <span key={a}>
                              {i > 0 && ", "}
                              <AddrLink addr={a} />
                            </span>
                          ))}
                        </span>
                      )}
                      {e.caller_addrs.length > 0 && (
                        <span>
                          caller:{" "}
                          {e.caller_addrs.map((a, i) => (
                            <span key={a}>
                              {i > 0 && ", "}
                              <AddrLink addr={a} />
                            </span>
                          ))}
                        </span>
                      )}
                    </div>
                  )}
                </li>
              ))}
            </ul>
            <Paginator
              page={eventsPaged.page}
              pageSize={eventsPaged.pageSize}
              hasMore={eventsPaged.hasMore}
              loading={events.isFetching}
              count={events.data?.data.length ?? 0}
              onPrev={eventsPaged.prev}
              onNext={eventsPaged.next}
            />
          </>
        )}
      </Panel>
    </>
  );
}
