import type { Network } from "./types";

// Runtime-injected configuration contract.
//
// A Docker/nginx/DeWeb operator can surface per-deployment overrides without
// a rebuild by populating `window.__MASSA_EXPLORER_CONFIG__` before the app
// boots (see `public/config.js` + the `docker-entrypoint.sh` that ships with
// the nginx image). The shape is intentionally loose; every field is optional.
interface RuntimeConfig {
  defaultNetwork?: Network;
  endpoints?: Partial<Record<Network, string[]>>;
}

declare global {
  // eslint-disable-next-line no-var
  var __MASSA_EXPLORER_CONFIG__: RuntimeConfig | undefined;
}

function runtimeCfg(): RuntimeConfig {
  if (typeof window === "undefined") return {};
  return window.__MASSA_EXPLORER_CONFIG__ ?? {};
}

// Bundled defaults (spec §12.2). User can override per-network in Settings.
// For a production DeWeb build you'd point these to your public indexers.
const BUNDLED_DEFAULTS: Record<Network, string[]> = {
  mainnet: [
    "http://127.0.0.1:8080",
    // v1: extra redundant indexers go here
  ],
  buildnet: [
    "http://127.0.0.1:8081",
  ],
};

/** Effective defaults after merging in `window.__MASSA_EXPLORER_CONFIG__`. */
export const DEFAULT_ENDPOINTS: Record<Network, string[]> = (() => {
  const overrides = runtimeCfg().endpoints ?? {};
  return {
    mainnet: overrides.mainnet?.length
      ? overrides.mainnet
      : BUNDLED_DEFAULTS.mainnet,
    buildnet: overrides.buildnet?.length
      ? overrides.buildnet
      : BUNDLED_DEFAULTS.buildnet,
  };
})();

const STORAGE_KEY = "massa-explorer/config/v1";
const DEFAULT_NETWORK: Network = (() => {
  const n = runtimeCfg().defaultNetwork;
  if (n === "mainnet" || n === "buildnet") return n;
  return "mainnet";
})();

interface PersistedConfig {
  network?: Network;
  endpointOverrides?: Partial<Record<Network, string[]>>;
}

function load(): PersistedConfig {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed == null) return {};
    return parsed as PersistedConfig;
  } catch {
    return {};
  }
}

function save(cfg: PersistedConfig) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(cfg));
  } catch {
    // quota / disabled localStorage — silent fallback to defaults
  }
}

export function getNetwork(): Network {
  const c = load();
  return c.network ?? DEFAULT_NETWORK;
}

export function setNetwork(net: Network) {
  const c = load();
  save({ ...c, network: net });
}

export function getEndpoints(net: Network): string[] {
  const c = load();
  const override = c.endpointOverrides?.[net];
  if (override && override.length > 0) return override;
  return DEFAULT_ENDPOINTS[net];
}

export function setEndpoints(net: Network, endpoints: string[]) {
  const c = load();
  const trimmed = endpoints.map((e) => e.trim()).filter(Boolean);
  const next: PersistedConfig = {
    ...c,
    endpointOverrides: {
      ...(c.endpointOverrides ?? {}),
      [net]: trimmed.length ? trimmed : undefined,
    },
  };
  save(next);
}

export function resetEndpoints(net: Network) {
  const c = load();
  if (!c.endpointOverrides) return;
  const { [net]: _dropped, ...rest } = c.endpointOverrides;
  save({ ...c, endpointOverrides: rest });
}
