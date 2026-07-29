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
import { formatTs } from "../lib/format";
import { useChainParams } from "../hooks/useChainParams";

export function EndorsementDetail() {
  const { id } = useParams();
  const { client, network } = useAppState();
  const params = useChainParams();

  const q = useQuery({
    queryKey: ["endorsement", network, id],
    queryFn: () => api.endorsement(client, id!),
    enabled: !!id,
  });

  const e = q.data?.data ?? null;

  return (
    <>
      <Helmet>
        <title>{`Endorsement ${id} — ${network}`}</title>
      </Helmet>
      <Panel title="Endorsement">
        {q.isLoading ? (
          <Loading />
        ) : q.isError ? (
          <ErrorMsg err={q.error} />
        ) : e == null ? (
          <NotFound what="endorsement" />
        ) : (
          <dl className="kv">
            <KV label="Id">
              <span className="font-mono break-all">{e.id}</span>
            </KV>
            <KV label="Index">
              <span className="font-mono">{e.index}</span>
            </KV>
            <KV label="Endorsed block">
              <BlockLink id={e.endorsed_block_id} short={false} />
            </KV>
            <KV label="Endorsed slot">
              <SlotRef slot={e.slot} params={params} timeMode="both" />
            </KV>
            <KV label="Included in block">
              <BlockLink id={e.included_block_id} short={false} />
            </KV>
            <KV label="Included slot">
              <SlotRef slot={e.included_slot} params={params} timeMode="both" />
            </KV>
            <KV label="Creator">
              <AddrLink addr={e.content_creator_address} short={false} />
            </KV>
            <KV label="Creator pubkey">
              <span className="font-mono break-all">
                {e.content_creator_pub_key}
              </span>
            </KV>
            <KV label="Signature">
              <span className="font-mono break-all text-xs">{e.signature}</span>
            </KV>
            <KV label="Serialized size">{`${e.serialized_size} B`}</KV>
            <KV label="First seen">{formatTs(e.first_seen_ts_ms)}</KV>
          </dl>
        )}
      </Panel>
    </>
  );
}
