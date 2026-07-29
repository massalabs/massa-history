import { createContext, useContext, useMemo, useState, type ReactNode } from "react";
import { getNetwork, setNetwork as persistNetwork } from "./lib/config";
import { makeApiClient, type ApiClient } from "./lib/api";
import type { Network } from "./lib/types";

interface AppCtx {
  network: Network;
  setNetwork: (n: Network) => void;
  client: ApiClient;
  // bumps when endpoint config is changed in Settings, so client is rebuilt
  configVersion: number;
  bumpConfigVersion: () => void;
}

const Ctx = createContext<AppCtx | null>(null);

export function AppStateProvider({ children }: { children: ReactNode }) {
  const [network, setNetworkState] = useState<Network>(() => getNetwork());
  const [configVersion, setConfigVersion] = useState(0);
  const client = useMemo(
    () => makeApiClient(network),
    // rebuild when network or saved endpoint overrides change
    [network, configVersion],
  );
  const value: AppCtx = {
    network,
    setNetwork: (n) => {
      persistNetwork(n);
      setNetworkState(n);
    },
    client,
    configVersion,
    bumpConfigVersion: () => setConfigVersion((v) => v + 1),
  };
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useAppState(): AppCtx {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("useAppState outside of AppStateProvider");
  return ctx;
}
