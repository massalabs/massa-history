import { useState } from "react";
import { Helmet } from "react-helmet-async";
import { useQuery } from "@tanstack/react-query";
import {
  DEFAULT_ENDPOINTS,
  getEndpoints,
  resetEndpoints,
  setEndpoints,
} from "../lib/config";
import type { Network } from "../lib/types";
import { useAppState } from "../AppState";
import { api, makeApiClient } from "../lib/api";
import { ErrorMsg, Panel } from "../components/Bits";

export function Settings() {
  const { network, client, bumpConfigVersion } = useAppState();
  const [draft, setDraft] = useState<Record<Network, string>>({
    mainnet: getEndpoints("mainnet").join("\n"),
    buildnet: getEndpoints("buildnet").join("\n"),
  });

  const save = (net: Network) => {
    const lines = draft[net]
      .split("\n")
      .map((s) => s.trim())
      .filter(Boolean);
    setEndpoints(net, lines);
    bumpConfigVersion();
  };
  const reset = (net: Network) => {
    resetEndpoints(net);
    setDraft((d) => ({ ...d, [net]: DEFAULT_ENDPOINTS[net].join("\n") }));
    bumpConfigVersion();
  };

  const status = useQuery({
    queryKey: ["settings-status", network, client.endpoints()],
    queryFn: () => api.status(client),
  });
  const probeHealth = useQuery({
    queryKey: ["settings-health", network, client.endpoints()],
    queryFn: async () => {
      const per: Record<string, "ok" | "down" | "error"> = {};
      for (const ep of client.endpoints()) {
        try {
          const c = makeApiClient(network, { endpoints: [ep], perAttemptTimeoutMs: 3000 });
          await api.health(c);
          per[ep] = "ok";
        } catch {
          per[ep] = "down";
        }
      }
      return per;
    },
  });

  return (
    <>
      <Helmet>
        <title>Settings — Massa Explorer</title>
      </Helmet>
      <Panel title="Indexer endpoints">
        <p className="text-sm text-muted mb-4">
          One URL per line. The explorer will try them in order and fail over to
          the next one if a request fails. Leave empty to use the bundled
          defaults. Changes are saved in this browser only.
        </p>
        {(["mainnet", "buildnet"] as Network[]).map((net) => (
          <div key={net} className="mb-4">
            <label className="block text-sm uppercase tracking-wide text-muted mb-1">
              {net}
            </label>
            <textarea
              rows={3}
              className="w-full bg-bg border border-border rounded-md px-3 py-2 font-mono text-sm"
              value={draft[net]}
              onChange={(e) =>
                setDraft((d) => ({ ...d, [net]: e.target.value }))
              }
            />
            <div className="flex gap-2 mt-1">
              <button className="btn" onClick={() => save(net)}>
                Save {net}
              </button>
              <button className="btn" onClick={() => reset(net)}>
                Reset to defaults
              </button>
            </div>
          </div>
        ))}
      </Panel>

      <div className="h-4" />

      <Panel title={`Current network health (${network})`}>
        {probeHealth.isLoading ? (
          <div className="text-muted">Probing…</div>
        ) : probeHealth.isError ? (
          <ErrorMsg err={probeHealth.error} />
        ) : (
          <ul className="text-sm">
            {Object.entries(probeHealth.data ?? {}).map(([ep, s]) => (
              <li key={ep} className="font-mono flex items-center gap-2">
                <span
                  className={
                    s === "ok" ? "text-ok" : "text-bad"
                  }
                >
                  {s}
                </span>
                <span>{ep}</span>
              </li>
            ))}
          </ul>
        )}
      </Panel>

      <div className="h-4" />

      <Panel title="Indexer status">
        {status.isLoading ? (
          <div className="text-muted">Loading…</div>
        ) : status.isError ? (
          <ErrorMsg err={status.error} />
        ) : (
          <pre className="text-xs bg-bg p-3 rounded border border-border overflow-auto">
            {JSON.stringify(status.data, null, 2)}
          </pre>
        )}
      </Panel>
    </>
  );
}
