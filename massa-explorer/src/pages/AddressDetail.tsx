import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Helmet } from "react-helmet-async";
import { useParams, useSearchParams } from "react-router-dom";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import {
  AddrLink,
  BlockLink,
  BlockStatusBadge,
  ErrorMsg,
  KV,
  Loading,
  OpKindPill,
  OpLink,
  Panel,
  SlotRef,
} from "../components/Bits";
import {
  firstIncludedSlot,
  fmtMas,
  fmtMasString,
  formatRelative,
  shortId,
  slotTimestampMs,
} from "../lib/format";
import { useChainParams } from "../hooks/useChainParams";
import { TransfersTable } from "../components/TransfersTable";
import { Paginator, usePaged } from "../components/Paginator";
import { BytecodePanel } from "../components/BytecodePanel";

type Tab =
  | "blocks"
  | "ops_out"
  | "ops_in"
  | "transfers"
  | "deferred"
  | "bytecode";
type DeferredRole = "sender" | "target";

/** Massa user addresses start with `AU`, smart contract addresses with `AS`. */
function isSmartContract(addr: string): boolean {
  return addr.startsWith("AS");
}

/**
 * The set of tabs that make sense for a given address kind. EOAs (`AU…`)
 * can produce blocks and sign operations; smart contracts (`AS…`) can't,
 * so we hide those tabs and surface the bytecode panel instead.
 */
function tabsFor(addr: string): { id: Tab; label: string }[] {
  if (isSmartContract(addr)) {
    return [
      { id: "ops_in",    label: "Operations targeting" },
      { id: "transfers", label: "Transfers" },
      { id: "deferred",  label: "Deferred calls" },
      { id: "bytecode",  label: "Bytecode" },
    ];
  }
  return [
    { id: "blocks",    label: "Blocks produced" },
    { id: "ops_out",   label: "Operations sent" },
    { id: "ops_in",    label: "Operations targeting" },
    { id: "transfers", label: "Transfers" },
    { id: "deferred",  label: "Deferred calls" },
  ];
}

export function AddressDetail() {
  const { addr } = useParams();
  const [searchParams] = useSearchParams();
  // `via` is set by /search when the user reached this page through a
  // resolved MNS name. We surface it as a badge so the lookup is
  // visible (and shareable via URL).
  const viaName = searchParams.get("via");
  const { client, network } = useAppState();
  const params = useChainParams();
  // Default tab depends on address kind: EOAs land on "blocks produced",
  // smart contracts land on "operations targeting" (the only history
  // tab that's actually meaningful for them; bytecode is one click away).
  const defaultTab: Tab = addr && isSmartContract(addr) ? "ops_in" : "blocks";
  const [tab, setTab] = useState<Tab>(defaultTab);
  const [deferredRole, setDeferredRole] = useState<DeferredRole>("sender");
  // If the URL changes from one kind to another (rare but happens via
  // search), snap back to the legal default for the new kind so we don't
  // show an empty tab that's been hidden.
  useEffect(() => {
    if (!addr) return;
    const legal = new Set(tabsFor(addr).map((t) => t.id));
    if (!legal.has(tab)) setTab(addr.startsWith("AS") ? "ops_in" : "blocks");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [addr]);

  // Each tab owns its own page counter so switching tabs doesn't lose state.
  const blocksPaged = usePaged(25);
  const opsOutPaged = usePaged(25);
  const opsInPaged = usePaged(25);
  const transfersPaged = usePaged(25);
  const deferredPaged = usePaged(25);

  const blocks = useQuery({
    queryKey: [
      "addr-blocks",
      network,
      addr,
      blocksPaged.cursor,
      blocksPaged.limit,
    ],
    queryFn: () =>
      api.addressBlocks(client, addr!, blocksPaged.limit, blocksPaged.cursor),
    enabled: !!addr && tab === "blocks",
  });
  const opsOut = useQuery({
    queryKey: [
      "addr-ops-creator",
      network,
      addr,
      opsOutPaged.cursor,
      opsOutPaged.limit,
    ],
    queryFn: () =>
      api.addressOps(
        client,
        addr!,
        "creator",
        opsOutPaged.limit,
        opsOutPaged.cursor,
      ),
    enabled: !!addr && tab === "ops_out",
  });
  const opsIn = useQuery({
    queryKey: [
      "addr-ops-target",
      network,
      addr,
      opsInPaged.cursor,
      opsInPaged.limit,
    ],
    queryFn: () =>
      api.addressOps(
        client,
        addr!,
        "target",
        opsInPaged.limit,
        opsInPaged.cursor,
      ),
    enabled: !!addr && tab === "ops_in",
  });
  const transfers = useQuery({
    queryKey: [
      "addr-transfers",
      network,
      addr,
      transfersPaged.cursor,
      transfersPaged.limit,
    ],
    queryFn: () =>
      api.addressTransfers(
        client,
        addr!,
        transfersPaged.limit,
        transfersPaged.cursor,
      ),
    enabled: !!addr && tab === "transfers",
  });
  const deferred = useQuery({
    queryKey: [
      "addr-deferred",
      network,
      addr,
      deferredRole,
      deferredPaged.cursor,
      deferredPaged.limit,
    ],
    queryFn: () =>
      api.addressDeferred(
        client,
        addr!,
        deferredRole,
        deferredPaged.limit,
        deferredPaged.cursor,
      ),
    enabled: !!addr && tab === "deferred",
  });

  // Live MAS balance + roll count fetched from the local node (bypasses the
  // indexer's RocksDB so it reflects the chain's current ledger). Refreshes
  // periodically so the panel keeps up with new finalised blocks.
  const nodeState = useQuery({
    queryKey: ["addr-node-state", network, addr],
    queryFn: () => api.addressNodeState(client, addr!),
    enabled: !!addr,
    refetchInterval: 15_000,
    staleTime: 5_000,
  });
  useEffect(() => {
    blocksPaged.setLastResponse(blocks.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [blocks.data]);
  useEffect(() => {
    opsOutPaged.setLastResponse(opsOut.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [opsOut.data]);
  useEffect(() => {
    opsInPaged.setLastResponse(opsIn.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [opsIn.data]);
  useEffect(() => {
    transfersPaged.setLastResponse(transfers.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [transfers.data]);
  useEffect(() => {
    deferredPaged.setLastResponse(deferred.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [deferred.data]);
  useEffect(() => {
    // Flipping the sender/target sub-toggle is effectively a different
    // query, so reset the cursor stack instead of carrying it over.
    deferredPaged.reset();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [deferredRole]);

  if (!addr) return <ErrorMsg err="missing address" />;
  const isSc = isSmartContract(addr);

  function TabBtn({ id, label }: { id: Tab; label: string }) {
    return (
      <button
        className={`btn ${tab === id ? "bg-border" : ""}`}
        onClick={() => setTab(id)}
      >
        {label}
      </button>
    );
  }

  return (
    <>
      <Helmet>
        <title>{`Address ${shortId(addr, 6, 6)} — Massa`}</title>
      </Helmet>
      <Panel title="Address">
        <dl className="kv">
          <KV label="Address">
            <span className="inline-flex items-center gap-2 flex-wrap">
              <AddrLink addr={addr} short={false} />
              <span
                className="inline-block px-1.5 py-0.5 rounded-full bg-panel border border-border text-[10px] uppercase tracking-wide text-muted"
                title={
                  isSc
                    ? "Smart contract address (AS prefix) — created by an ExecuteSC operation, holds bytecode and datastore."
                    : "User address (AU prefix) — derived from a keypair held by an account holder."
                }
              >
                {isSc ? "smart contract" : "user"}
              </span>
              {viaName && (
                <span
                  className="inline-block px-1.5 py-0.5 rounded-full border border-border text-[10px] tracking-wide text-muted"
                  title="Resolved via the Massa Name Service (MNS) on-chain registry."
                >
                  aka <span className="font-mono">{viaName}</span>
                </span>
              )}
            </span>
          </KV>
          {(() => {
            // The node call is best-effort: if the local node is briefly
            // unreachable we just show a dash rather than scaring the user.
            const ns = nodeState.data?.data ?? null;
            const finalBal = ns ? fmtMasString(ns.final_balance_nmas) : null;
            const candBal = ns ? fmtMasString(ns.candidate_balance_nmas) : null;
            const sameBal =
              ns && ns.final_balance_nmas === ns.candidate_balance_nmas;
            const hasRolls =
              !!ns &&
              (ns.final_rolls > 0 ||
                ns.candidate_rolls > 0 ||
                ns.active_rolls > 0);
            const dc = ns?.deferred_credits_final ?? [];
            const dcTotalNmas = dc.reduce(
              (acc, e) => acc + BigInt(e.nmas),
              0n,
            );
            return (
              <>
                <KV label="Balance (final)">
                  {nodeState.isLoading && !ns ? (
                    <span className="text-muted">…</span>
                  ) : nodeState.isError ? (
                    <span
                      className="text-muted"
                      title="Could not reach the node"
                    >
                      —
                    </span>
                  ) : (
                    <span className="font-mono">{finalBal ?? "—"}</span>
                  )}
                </KV>
                {ns && !sameBal && (
                  <KV label="Balance (candidate)">
                    <span
                      className="font-mono text-muted"
                      title="Pending un-finalised balance"
                    >
                      {candBal}
                    </span>
                  </KV>
                )}
                {hasRolls && ns && (
                  <KV label="Rolls">
                    <span className="font-mono">
                      {ns.active_rolls.toLocaleString()}
                      <span
                        className="text-muted"
                        title="Active rolls are the rolls that have aged enough to actively produce blocks in the current PoS cycle. Final rolls is the total roll holding in the latest finalised ledger; candidate may differ briefly when a roll buy/sell is in a non-final block."
                      >
                        {" "}
                        active
                      </span>
                      {(ns.final_rolls !== ns.active_rolls ||
                        ns.candidate_rolls !== ns.final_rolls) && (
                        <span className="text-muted">
                          {" "}
                          · {ns.final_rolls.toLocaleString()} final
                          {ns.candidate_rolls !== ns.final_rolls && (
                            <>
                              {" "}
                              · {ns.candidate_rolls.toLocaleString()} candidate
                            </>
                          )}
                        </span>
                      )}
                    </span>
                  </KV>
                )}
                {ns && dc.length > 0 && (
                  <KV label="Deferred credits">
                    <div className="flex flex-col gap-1">
                      <div className="font-mono">
                        {fmtMasString(dcTotalNmas.toString())}
                        <span
                          className="text-muted"
                          title="Sum of MAS scheduled to be returned to this address after a roll sell. Each entry is released automatically at a future slot."
                        >
                          {" "}
                          · {dc.length}{" "}
                          {dc.length === 1 ? "entry" : "entries"}
                        </span>
                      </div>
                      {dc.length <= 6 ? (
                        <div className="text-xs text-muted flex flex-col gap-0.5">
                          {dc.map((e, i) => (
                            <div
                              key={`${e.slot.period}-${e.slot.thread}-${i}`}
                              className="flex items-center gap-2 flex-wrap"
                            >
                              <SlotRef
                                slot={e.slot}
                                params={params}
                                timeMode="both"
                              />
                              <span className="font-mono">
                                → {fmtMasString(e.nmas)}
                              </span>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <details className="text-xs text-muted">
                          <summary className="cursor-pointer select-none">
                            Show {dc.length} releases
                          </summary>
                          <div className="mt-1 flex flex-col gap-0.5 max-h-64 overflow-auto">
                            {dc.map((e, i) => (
                              <div
                                key={`${e.slot.period}-${e.slot.thread}-${i}`}
                                className="flex items-center gap-2 flex-wrap"
                              >
                                <SlotRef
                                  slot={e.slot}
                                  params={params}
                                  timeMode="both"
                                />
                                <span className="font-mono">
                                  → {fmtMasString(e.nmas)}
                                </span>
                              </div>
                            ))}
                          </div>
                        </details>
                      )}
                    </div>
                  </KV>
                )}
              </>
            );
          })()}
        </dl>
      </Panel>

      <div className="h-4" />

      <div className="flex gap-2 mb-3 flex-wrap">
        {tabsFor(addr).map((t) => (
          <TabBtn key={t.id} id={t.id} label={t.label} />
        ))}
      </div>

      {tab === "blocks" && (
        <Panel title="Blocks">
          {blocks.isLoading ? (
            <Loading />
          ) : blocks.isError ? (
            <ErrorMsg err={blocks.error} />
          ) : blocks.data?.data.length === 0 && blocksPaged.page === 0 ? (
            <div className="text-muted">No blocks indexed yet.</div>
          ) : (
            <div className="overflow-x-auto -mx-3 sm:mx-0">
              <table className="w-full text-sm min-w-[560px]">
                <thead className="text-muted text-xs uppercase">
                  <tr>
                    <th className="text-left py-1 px-2">Slot</th>
                    <th className="text-left py-1 px-2">Status</th>
                    <th className="text-left py-1 px-2">Block</th>
                    <th className="text-right py-1 px-2">Seen</th>
                  </tr>
                </thead>
                <tbody>
                  {blocks.data?.data.map((b) => (
                    <tr key={b.id} className="border-t border-border">
                      <td className="py-1.5 px-2">
                        <SlotRef
                          slot={b.slot}
                          params={params}
                          timeMode="relative"
                        />
                      </td>
                      <td className="px-2">
                        <BlockStatusBadge status={b.status} />
                      </td>
                      <td className="px-2">
                        <BlockLink id={b.id} />
                      </td>
                      <td className="px-2 text-right text-muted text-xs whitespace-nowrap">
                        {formatRelative(b.first_seen_ts_ms)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <Paginator
            page={blocksPaged.page}
            pageSize={blocksPaged.pageSize}
            hasMore={blocksPaged.hasMore}
            loading={blocks.isFetching}
            count={blocks.data?.data.length ?? 0}
            onPrev={blocksPaged.prev}
            onNext={blocksPaged.next}
          />
        </Panel>
      )}

      {tab === "transfers" && (
        <Panel title="Transfers">
          {transfers.isLoading ? (
            <Loading />
          ) : transfers.isError ? (
            <ErrorMsg err={transfers.error} />
          ) : (
            <>
              <TransfersTable
                transfers={transfers.data?.data ?? []}
                highlight={addr}
                emptyLabel="No transfers recorded for this address (node may not be running with execution-trace)."
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
      )}

      {(tab === "ops_out" || tab === "ops_in") && (
        <Panel
          title={
            tab === "ops_out"
              ? "Operations sent (as creator)"
              : "Operations targeting (as recipient / SC callee)"
          }
        >
          {(() => {
            const q = tab === "ops_out" ? opsOut : opsIn;
            const pager = tab === "ops_out" ? opsOutPaged : opsInPaged;
            return (
              <>
                {q.isLoading ? (
                  <Loading />
                ) : q.isError ? (
                  <ErrorMsg err={q.error} />
                ) : (q.data?.data.length ?? 0) === 0 && pager.page === 0 ? (
                  <div className="text-muted">No operations indexed yet.</div>
                ) : (
                  <div className="overflow-x-auto -mx-3 sm:mx-0">
                    <table className="w-full text-sm min-w-[560px]">
                      <thead className="text-muted text-xs uppercase">
                        <tr>
                          <th className="text-left py-1 px-2">Op</th>
                          <th className="text-left py-1 px-2">Kind</th>
                          <th className="text-left py-1 px-2">Counterparty</th>
                          <th
                            className="text-right py-1 px-2 whitespace-nowrap"
                            title="Wall-clock time of the slot in which the operation was first included."
                          >
                            Included
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {q.data?.data.map((op) => {
                          const incSlot = firstIncludedSlot(op);
                          const incTs = incSlot
                            ? slotTimestampMs(incSlot, params)
                            : null;
                          return (
                            <tr key={op.id} className="border-t border-border">
                              <td className="py-1.5 px-2">
                                <OpLink id={op.id} />
                              </td>
                              <td className="px-2">
                                <OpKindPill kind={op.kind} />
                              </td>
                              <td className="px-2">
                                {tab === "ops_out" ? (
                                  op.target ? (
                                    <AddrLink addr={op.target} />
                                  ) : (
                                    <span className="text-muted">—</span>
                                  )
                                ) : (
                                  <AddrLink addr={op.creator} />
                                )}
                              </td>
                              <td className="px-2 text-right text-muted text-xs whitespace-nowrap">
                                {incTs != null
                                  ? formatRelative(incTs)
                                  : "pending"}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
                <Paginator
                  page={pager.page}
                  pageSize={pager.pageSize}
                  hasMore={pager.hasMore}
                  loading={q.isFetching}
                  count={q.data?.data.length ?? 0}
                  onPrev={pager.prev}
                  onNext={pager.next}
                />
              </>
            );
          })()}
        </Panel>
      )}

      {tab === "deferred" && (
        <Panel
          title="Deferred calls"
          action={
            <div className="flex gap-1 text-xs">
              <button
                className={`btn ${deferredRole === "sender" ? "bg-border" : ""}`}
                onClick={() => setDeferredRole("sender")}
                title="Deferred calls that were scheduled BY this address."
              >
                scheduled by
              </button>
              <button
                className={`btn ${deferredRole === "target" ? "bg-border" : ""}`}
                onClick={() => setDeferredRole("target")}
                title="Deferred calls that target this address (callee of the scheduled call)."
              >
                targeting
              </button>
            </div>
          }
        >
          {deferred.isLoading ? (
            <Loading />
          ) : deferred.isError ? (
            <ErrorMsg err={deferred.error} />
          ) : (deferred.data?.data.length ?? 0) === 0 &&
            deferredPaged.page === 0 ? (
            <div className="text-muted text-sm">
              No deferred calls{" "}
              {deferredRole === "sender"
                ? "scheduled by this address."
                : "targeting this address."}
            </div>
          ) : (
            <div className="overflow-x-auto -mx-3 sm:mx-0">
              <table className="w-full text-sm min-w-[640px]">
                <thead className="text-muted text-xs uppercase">
                  <tr>
                    <th className="text-left py-1 px-2">Call</th>
                    <th className="text-left py-1 px-2">State</th>
                    <th className="text-left py-1 px-2">
                      {deferredRole === "sender" ? "Target" : "Sender"}
                    </th>
                    <th className="text-left py-1 px-2">Function</th>
                    <th className="text-left py-1 px-2">Scheduled for</th>
                    <th className="text-right py-1 px-2">Coins</th>
                  </tr>
                </thead>
                <tbody>
                  {deferred.data?.data.map((dc) => (
                    <tr key={dc.id} className="border-t border-border align-top">
                      <td className="py-1.5 px-2 font-mono break-all">
                        {shortId(dc.id, 8, 6)}
                      </td>
                      <td className="px-2">
                        <span className="inline-block px-1.5 py-0.5 rounded-full bg-panel border border-border text-[10px] uppercase tracking-wide text-muted">
                          {dc.state}
                        </span>
                      </td>
                      <td className="px-2">
                        {deferredRole === "sender" ? (
                          dc.target_address ? (
                            <AddrLink addr={dc.target_address} />
                          ) : (
                            <span className="text-muted">—</span>
                          )
                        ) : dc.sender ? (
                          <AddrLink addr={dc.sender} />
                        ) : (
                          <span className="text-muted">—</span>
                        )}
                      </td>
                      <td className="px-2 font-mono text-xs break-all">
                        {dc.target_function || (
                          <span className="text-muted">—</span>
                        )}
                      </td>
                      <td className="px-2">
                        {dc.target_slot ? (
                          <SlotRef
                            slot={dc.target_slot}
                            params={params}
                            timeMode="relative"
                          />
                        ) : (
                          <span className="text-muted">—</span>
                        )}
                      </td>
                      <td className="px-2 text-right font-mono whitespace-nowrap">
                        {fmtMas(dc.coins_nmas)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <Paginator
            page={deferredPaged.page}
            pageSize={deferredPaged.pageSize}
            hasMore={deferredPaged.hasMore}
            loading={deferred.isFetching}
            count={deferred.data?.data.length ?? 0}
            onPrev={deferredPaged.prev}
            onNext={deferredPaged.next}
          />
        </Panel>
      )}

      {tab === "bytecode" && isSc && <BytecodePanel addr={addr} />}
    </>
  );
}
