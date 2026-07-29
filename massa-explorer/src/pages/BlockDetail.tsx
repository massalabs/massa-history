import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Helmet } from "react-helmet-async";
import { Link, useParams } from "react-router-dom";
import { useEffect } from "react";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import {
  AddrLink,
  BlockLink,
  BlockStatusBadge,
  ErrorMsg,
  KV,
  Loading,
  NotFound,
  OpLink,
  Panel,
  SlotRef,
} from "../components/Bits";
import { formatTs, shortId } from "../lib/format";
import { useChainParams } from "../hooks/useChainParams";
import { useSseSlots } from "../hooks/useSseSlots";
import { TransfersTable } from "../components/TransfersTable";
import { Paginator, usePaged, useLocalPaged } from "../components/Paginator";

export function BlockDetail() {
  const { id } = useParams();
  const { client, network } = useAppState();
  const params = useChainParams();
  const qc = useQueryClient();

  const q = useQuery({
    queryKey: ["block", network, id],
    queryFn: () => api.block(client, id!),
    enabled: !!id,
    // Keep refetching while the block is candidate OR while operations /
    // endorsements haven't been filled in yet. The node occasionally emits
    // a `FilledBlock` with an empty `operations` list when it references the
    // block before the body has fully propagated; the body is re-emitted on
    // reconnect / backfill and we want the page to pick that up without the
    // user having to manually reload. A 60s cap keeps us from pinning a
    // legitimately empty block forever.
    refetchInterval: (q) => {
      const b = q.state.data?.data;
      const status = b?.status;
      const settled = status === "final" || status === "discarded";
      const bodyLoaded = (b?.operation_ids?.length ?? 0) > 0
        || (b?.endorsement_ids?.length ?? 0) > 0;
      if (!settled) return 3_000;
      const firstSeenTs = b?.first_seen_ts_ms ?? 0;
      const age = firstSeenTs ? Date.now() - firstSeenTs : 0;
      if (!bodyLoaded && age < 60_000) return 3_000;
      return false;
    },
  });

  // All transfers executed inside this block (slot + block scoped).
  const transfersPaged = usePaged(25);
  const blockStatus = q.data?.data?.status ?? null;
  const transfers = useQuery({
    queryKey: [
      "block-transfers",
      network,
      id,
      transfersPaged.cursor,
      transfersPaged.limit,
    ],
    queryFn: () =>
      api.blockTransfers(
        client,
        id!,
        transfersPaged.limit,
        transfersPaged.cursor,
      ),
    enabled: !!id,
    // Transfers land with *final* execution of the block's slot, which can
    // arrive a few seconds after the block itself flips to final (the
    // transfer stream is separate from the block stream). We therefore poll
    // while the block is still candidate AND for a short window after
    // finalization when no rows have appeared yet, mirroring what we do on
    // the Operation detail page.
    refetchInterval: (q) => {
      if (blockStatus !== "final" && blockStatus !== "discarded") return 3_000;
      const rows = (q.state.data?.data ?? []).length;
      if (rows === 0 && transfersPaged.page === 0) return 3_000;
      return false;
    },
  });
  const opsPaged = useLocalPaged(50);
  const parentsPaged = useLocalPaged(50);
  const endoPaged = usePaged(32);
  const denPaged = usePaged(32);

  // Block endorsements — authoritative list from the block's embedded
  // `endorsements` array (which the REST returns as `StoredEndorsement`).
  const endorsements = useQuery({
    queryKey: [
      "block-endorsements",
      network,
      id,
      endoPaged.cursor,
      endoPaged.limit,
    ],
    queryFn: () =>
      api.blockEndorsements(client, id!, endoPaged.limit, endoPaged.cursor),
    enabled: !!id,
  });
  useEffect(() => {
    endoPaged.setLastResponse(endorsements.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [endorsements.data]);
  const denunciations = useQuery({
    queryKey: [
      "block-denunciations",
      network,
      id,
      denPaged.cursor,
      denPaged.limit,
    ],
    queryFn: () =>
      api.blockDenunciations(client, id!, denPaged.limit, denPaged.cursor),
    enabled: !!id,
  });
  useEffect(() => {
    denPaged.setLastResponse(denunciations.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [denunciations.data]);
  useEffect(() => {
    transfersPaged.setLastResponse(transfers.data ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [transfers.data]);

  // Listen to SSE for the slot this block lives in — when the slot finalizes
  // we want the block status to flip immediately AND transfers to refresh.
  const { events: sseEvents } = useSseSlots(client, 16);
  const slot = q.data?.data.slot;
  useEffect(() => {
    if (!slot || !sseEvents.length) return;
    if (
      sseEvents.some(
        (e) =>
          e.slot.period === slot.period && e.slot.thread === slot.thread,
      )
    ) {
      qc.invalidateQueries({ queryKey: ["block", network, id] });
      qc.invalidateQueries({ queryKey: ["block-transfers", network, id] });
    }
  }, [sseEvents, slot, id, network, qc]);

  if (!id) return <ErrorMsg err="missing id" />;
  if (q.isLoading) return <Loading />;
  if (q.isError) return <ErrorMsg err={q.error} />;
  if (!q.data) return <NotFound what={`block ${shortId(id)}`} />;

  const b = q.data.data;
  const title = `Block ${shortId(id)} — Massa`;
  return (
    <>
      <Helmet>
        <title>{title}</title>
      </Helmet>
      <Panel
        title={`Block ${shortId(id)}`}
        action={<BlockStatusBadge status={b.status} />}
      >
        <dl className="kv">
          <KV label="ID">
            <span className="font-mono break-all">{b.id}</span>
          </KV>
          <KV label="Slot">
            <SlotRef slot={b.slot} params={params} timeMode="both" />
          </KV>
          <KV label="Creator">
            <AddrLink addr={b.creator} short={false} />
          </KV>
          <KV label="Operations">{b.operation_ids?.length ?? 0}</KV>
          <KV label="Endorsements">{b.endorsement_ids?.length ?? 0}</KV>
          <KV label="Denunciations">{b.denunciation_hashes?.length ?? 0}</KV>
          <KV label="First seen">{formatTs(b.first_seen_ts_ms)}</KV>
        </dl>
      </Panel>

      <div className="h-4" />

      <Panel title={`Parents (${b.parents?.length ?? 0})`}>
        {!b.parents?.length ? (
          <div className="text-muted text-sm">
            No parents recorded (this is genesis or the block body has not
            been ingested yet).
          </div>
        ) : (
          <>
            <ul className="text-sm divide-y divide-border">
              {b.parents
                .slice(
                  parentsPaged.offset,
                  parentsPaged.offset + parentsPaged.pageSize,
                )
                .map((p) => (
                  <li key={p} className="py-1 break-all">
                    <BlockLink id={p} short={false} />
                  </li>
                ))}
            </ul>
            <Paginator
              page={parentsPaged.page}
              pageSize={parentsPaged.pageSize}
              hasMore={
                parentsPaged.offset + parentsPaged.pageSize < b.parents.length
              }
              count={Math.max(
                0,
                Math.min(
                  parentsPaged.pageSize,
                  b.parents.length - parentsPaged.offset,
                ),
              )}
              onPrev={parentsPaged.prev}
              onNext={parentsPaged.next}
            />
          </>
        )}
      </Panel>

      <div className="h-4" />

      <Panel title={`Operations (${b.operation_ids?.length ?? 0})`}>
        {!b.operation_ids?.length ? (
          <div className="text-muted text-sm">
            No operations in this block.
          </div>
        ) : (
          <>
            <ul className="text-sm divide-y divide-border">
              {b.operation_ids
                .slice(opsPaged.offset, opsPaged.offset + opsPaged.pageSize)
                .map((op) => (
                  <li key={op} className="py-1 break-all">
                    <OpLink id={op} short={false} />
                  </li>
                ))}
            </ul>
            <Paginator
              page={opsPaged.page}
              pageSize={opsPaged.pageSize}
              hasMore={
                opsPaged.offset + opsPaged.pageSize < b.operation_ids.length
              }
              count={Math.max(
                0,
                Math.min(
                  opsPaged.pageSize,
                  b.operation_ids.length - opsPaged.offset,
                ),
              )}
              onPrev={opsPaged.prev}
              onNext={opsPaged.next}
            />
          </>
        )}
      </Panel>

      <div className="h-4" />

      <Panel title={`Endorsements (${b.endorsement_ids?.length ?? 0})`}>
        {endorsements.isLoading ? (
          <Loading />
        ) : endorsements.isError ? (
          <ErrorMsg err={endorsements.error} />
        ) : (endorsements.data?.data.length ?? 0) === 0 ? (
          <div className="text-muted text-sm">
            No endorsements in this block.
          </div>
        ) : (
          <>
            <table className="w-full text-sm">
              <thead className="text-muted text-[11px] uppercase">
                <tr>
                  <th className="text-left py-1 px-2">#</th>
                  <th className="text-left py-1 px-2">Endorsement</th>
                  <th className="text-left py-1 px-2">Creator</th>
                  <th className="text-left py-1 px-2">Endorsed</th>
                </tr>
              </thead>
              <tbody>
                {endorsements.data!.data.map((e) => (
                  <tr key={e.id} className="border-t border-border align-top">
                    <td className="py-1.5 px-2 font-mono">{e.index}</td>
                    <td className="px-2">
                      <Link
                        to={`/endorsement/${encodeURIComponent(e.id)}`}
                        className="font-mono break-all text-accent2 no-underline hover:underline"
                      >
                        {e.id.slice(0, 16)}…
                      </Link>
                    </td>
                    <td className="px-2">
                      <AddrLink addr={e.content_creator_address} short={false} />
                    </td>
                    <td className="px-2">
                      <BlockLink id={e.endorsed_block_id} short={false} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <Paginator
              page={endoPaged.page}
              pageSize={endoPaged.pageSize}
              hasMore={endoPaged.hasMore}
              loading={endorsements.isFetching}
              count={endorsements.data?.data.length ?? 0}
              onPrev={endoPaged.prev}
              onNext={endoPaged.next}
            />
          </>
        )}
      </Panel>

      <div className="h-4" />

      <Panel title={`Denunciations (${b.denunciation_hashes?.length ?? 0})`}>
        {denunciations.isLoading ? (
          <Loading />
        ) : denunciations.isError ? (
          <ErrorMsg err={denunciations.error} />
        ) : (denunciations.data?.data.length ?? 0) === 0 ? (
          <div className="text-muted text-sm">
            No denunciations in this block.
          </div>
        ) : (
          <ul className="text-sm divide-y divide-border">
            {denunciations.data!.data.map((d) => (
              <li key={d.hash} className="py-1 break-all">
                <Link
                  to={`/denunciation/${d.hash}`}
                  className="font-mono text-accent2 no-underline hover:underline"
                >
                  {d.hash.slice(0, 16)}…
                </Link>{" "}
                <span className="text-muted text-xs">{d.kind}</span>
                {d.denounced_addr && (
                  <>
                    {" · "}
                    <AddrLink addr={d.denounced_addr} short={false} />
                  </>
                )}
              </li>
            ))}
          </ul>
        )}
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
              showSlot={false}
              showBlock={false}
              emptyLabel={
                b.status === "final"
                  ? "No transfers executed inside this block."
                  : "Transfers will appear once the block finalizes."
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
    </>
  );
}
