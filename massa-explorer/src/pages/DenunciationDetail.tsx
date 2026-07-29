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
import type { StoredDenunciation } from "../lib/types";

function KindSpecific({ d }: { d: StoredDenunciation }) {
  switch (d.kind) {
    case "block_header":
      return (
        <>
          <KV label="Public key">
            <span className="font-mono break-all">{d.public_key}</span>
          </KV>
          <KV label="Hash 1">
            <span className="font-mono break-all text-xs">{d.hash_1}</span>
          </KV>
          <KV label="Hash 2">
            <span className="font-mono break-all text-xs">{d.hash_2}</span>
          </KV>
          <KV label="Signature 1">
            <span className="font-mono break-all text-xs">{d.signature_1}</span>
          </KV>
          <KV label="Signature 2">
            <span className="font-mono break-all text-xs">{d.signature_2}</span>
          </KV>
        </>
      );
    case "endorsement":
      return (
        <>
          <KV label="Public key">
            <span className="font-mono break-all">{d.public_key}</span>
          </KV>
          <KV label="Endorsement index">
            <span className="font-mono">{d.index}</span>
          </KV>
          <KV label="Hash 1">
            <span className="font-mono break-all text-xs">{d.hash_1}</span>
          </KV>
          <KV label="Hash 2">
            <span className="font-mono break-all text-xs">{d.hash_2}</span>
          </KV>
          <KV label="Signature 1">
            <span className="font-mono break-all text-xs">{d.signature_1}</span>
          </KV>
          <KV label="Signature 2">
            <span className="font-mono break-all text-xs">{d.signature_2}</span>
          </KV>
        </>
      );
    case "address":
      return (
        <>
          <KV label="Address denounced">
            <AddrLink addr={d.address_denounced} short={false} />
          </KV>
          <KV label="Slashed (nMAS)">
            <span className="font-mono">
              {d.slashed_nmas.toLocaleString()}
            </span>
          </KV>
        </>
      );
    case "unknown":
      return <KV label="Kind">unknown (forward-compat payload)</KV>;
  }
}

export function DenunciationDetail() {
  const { hash } = useParams();
  const { client, network } = useAppState();
  const params = useChainParams();

  const q = useQuery({
    queryKey: ["denunciation", network, hash],
    queryFn: () => api.denunciation(client, hash!),
    enabled: !!hash,
  });

  const d = q.data?.data ?? null;

  return (
    <>
      <Helmet>
        <title>{`Denunciation ${hash?.slice(0, 8)} — ${network}`}</title>
      </Helmet>
      <Panel title={`Denunciation ${d?.kind ?? ""}`}>
        {q.isLoading ? (
          <Loading />
        ) : q.isError ? (
          <ErrorMsg err={q.error} />
        ) : d == null ? (
          <NotFound what="denunciation" />
        ) : (
          <dl className="kv">
            <KV label="Hash">
              <span className="font-mono break-all">{d.hash}</span>
            </KV>
            <KV label="Kind">{d.kind}</KV>
            <KV label="Slot">
              <SlotRef slot={d.slot} params={params} timeMode="both" />
            </KV>
            <KV label="Denounced address">
              {d.denounced_addr ? (
                <AddrLink addr={d.denounced_addr} short={false} />
              ) : (
                "—"
              )}
            </KV>
            <KV label="Included in">
              {d.included_block_id ? (
                <BlockLink id={d.included_block_id} short={false} />
              ) : (
                "—"
              )}
            </KV>
            <KV label="Included slot">
              {d.included_slot ? (
                <SlotRef slot={d.included_slot} params={params} timeMode="both" />
              ) : (
                "—"
              )}
            </KV>
            <KV label="First seen">{formatTs(d.first_seen_ts_ms)}</KV>
            <KindSpecific d={d.denunciation} />
          </dl>
        )}
      </Panel>
    </>
  );
}
