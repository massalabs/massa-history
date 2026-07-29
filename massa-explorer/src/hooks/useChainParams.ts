import { useQuery } from "@tanstack/react-query";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import type { ChainParams } from "../lib/format";

// Mainnet defaults so slot ↔ time math always works even before the status
// query resolves (indexer mainnet config).
const FALLBACK: ChainParams = {
  genesisTimestampMs: 1705312800000,
  t0Ms: 16_000,
  threadCount: 32,
};

/**
 * Fetches chain parameters (genesis, t0, thread_count) from the indexer's
 * /v1/status endpoint. Cached aggressively — these values don't change
 * for the lifetime of a network.
 */
export function useChainParams(): ChainParams {
  const { client, network } = useAppState();
  const q = useQuery({
    queryKey: ["chain-params", network],
    queryFn: async () => {
      const resp = await api.status(client);
      const meta = resp.data.meta;
      return {
        genesisTimestampMs: meta.genesis_timestamp_ms || FALLBACK.genesisTimestampMs,
        t0Ms: meta.t0_ms || FALLBACK.t0Ms,
        threadCount: meta.thread_count || FALLBACK.threadCount,
      } satisfies ChainParams;
    },
    staleTime: 60 * 60 * 1000,
    refetchOnWindowFocus: false,
  });
  return q.data ?? FALLBACK;
}
