import type {
  AddressNodeState,
  ChartPoint,
  Envelope,
  HealthResp,
  Network,
  SlotState,
  Status,
  StoredBlock,
  StoredDeferredCall,
  StoredDenunciationEntry,
  StoredEndorsement,
  StoredOperation,
  StoredScEvent,
  StoredTransfer,
} from "./types";
import { getEndpoints } from "./config";

// ---------------------------------------------------------------------------
// Endpoint-failover fetch client (spec §12.2.1).
// ---------------------------------------------------------------------------
//
// Strategy: try each configured endpoint in order, with a short per-attempt
// timeout, until one returns 2xx (or a meaningful 4xx we want to surface).
// A 404 is treated as "not found" and returns `null` without falling back.
// A 5xx or network error falls through to the next endpoint. If all fail,
// throw a single ApiError.
// ---------------------------------------------------------------------------

export class ApiError extends Error {
  constructor(
    message: string,
    public status?: number,
    public endpointsTried?: string[],
    public lastBody?: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

interface ApiClient {
  get<T>(path: string, init?: RequestInit): Promise<T>;
  getMaybe<T>(path: string, init?: RequestInit): Promise<T | null>;
  /** Same failover semantics as `get` / `getMaybe`, but returns the raw
   *  response bytes (and the resolved URL we ended up reading from).
   *  Returns `null` on 404. Throws on transport failure on all endpoints
   *  or on any non-2xx, non-404 status. Use for WASM blobs, CSV
   *  exports, or anything where we don't want a JSON round-trip. */
  getBytes(
    path: string,
    init?: RequestInit,
  ): Promise<{ bytes: Uint8Array; url: string; contentType: string } | null>;
  endpoints(): string[];
  sseUrl(path: string): string | null;
}

export interface ApiClientOptions {
  endpoints?: string[];
  perAttemptTimeoutMs?: number;
  fetchImpl?: typeof fetch;
}

const DEFAULT_TIMEOUT_MS = 8_000;

export function makeApiClient(network: Network, opts: ApiClientOptions = {}): ApiClient {
  const endpoints = opts.endpoints ?? getEndpoints(network);
  const timeoutMs = opts.perAttemptTimeoutMs ?? DEFAULT_TIMEOUT_MS;
  const doFetch: typeof fetch =
    opts.fetchImpl ?? ((...a) => fetch(...a));

  async function attempt(
    base: string,
    path: string,
    init: RequestInit | undefined,
  ): Promise<Response> {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const url = `${base.replace(/\/$/, "")}${path}`;
      const resp = await doFetch(url, {
        ...init,
        signal: ctrl.signal,
        headers: {
          Accept: "application/json",
          ...(init?.headers ?? {}),
        },
      });
      return resp;
    } finally {
      clearTimeout(timer);
    }
  }

  async function run(
    path: string,
    init: RequestInit | undefined,
  ): Promise<Response> {
    if (endpoints.length === 0) {
      throw new ApiError("No indexer endpoints configured", undefined, []);
    }
    const tried: string[] = [];
    let lastBody = "";
    for (const base of endpoints) {
      tried.push(base);
      try {
        const resp = await attempt(base, path, init);
        // 2xx and 404 are "final" answers — do not fall through.
        if (resp.ok || resp.status === 404) return resp;
        lastBody = await resp.text().catch(() => "");
      } catch (_e) {
        // network / abort — fall through
      }
    }
    throw new ApiError(
      `All endpoints failed for ${path}`,
      undefined,
      tried,
      lastBody,
    );
  }

  async function get<T>(path: string, init?: RequestInit): Promise<T> {
    const resp = await run(path, init);
    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      throw new ApiError(
        `HTTP ${resp.status} for ${path}`,
        resp.status,
        endpoints,
        body,
      );
    }
    return (await resp.json()) as T;
  }

  async function getMaybe<T>(path: string, init?: RequestInit): Promise<T | null> {
    const resp = await run(path, init);
    if (resp.status === 404) return null;
    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      throw new ApiError(
        `HTTP ${resp.status} for ${path}`,
        resp.status,
        endpoints,
        body,
      );
    }
    return (await resp.json()) as T;
  }

  async function getBytes(
    path: string,
    init?: RequestInit,
  ): Promise<{ bytes: Uint8Array; url: string; contentType: string } | null> {
    // Reuse the failover engine but ask for octet-stream explicitly so
    // the server doesn't pick a JSON variant if it ever supports content
    // negotiation. The `run` helper already short-circuits on 404 and
    // falls through on transport errors.
    const resp = await run(path, {
      ...init,
      headers: { Accept: "application/octet-stream", ...(init?.headers ?? {}) },
    });
    if (resp.status === 404) return null;
    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      throw new ApiError(
        `HTTP ${resp.status} for ${path}`,
        resp.status,
        endpoints,
        body,
      );
    }
    const buf = await resp.arrayBuffer();
    return {
      bytes: new Uint8Array(buf),
      url: resp.url,
      contentType: resp.headers.get("content-type") ?? "application/octet-stream",
    };
  }

  function sseUrl(path: string): string | null {
    const base = endpoints[0];
    if (!base) return null;
    return `${base.replace(/\/$/, "")}${path}`;
  }

  return { get, getMaybe, getBytes, endpoints: () => endpoints.slice(), sseUrl };
}

// ---------------------------------------------------------------------------
// Typed endpoint wrappers
// ---------------------------------------------------------------------------

/** Build a `limit=X&cursor=...` query fragment for cursor-based pagination.
 *  The backend enforces a hard `max_page_size=100` cap, and each response
 *  includes a `cursor_next` token which should be echoed back here to fetch
 *  the next page. Offset-based pagination is no longer supported. */
function paging(limit: number, cursor?: string | null): string {
  const qs = new URLSearchParams();
  qs.set("limit", String(limit));
  if (cursor) qs.set("cursor", cursor);
  return qs.toString();
}

export const api = {
  health: (c: ApiClient) => c.get<HealthResp>("/v1/health"),
  status: (c: ApiClient) => c.get<Envelope<Status>>("/v1/status"),
  block: (c: ApiClient, id: string) =>
    c.getMaybe<Envelope<StoredBlock>>(`/v1/blocks/${encodeURIComponent(id)}`),
  operation: (c: ApiClient, id: string) =>
    c.getMaybe<Envelope<StoredOperation>>(`/v1/operations/${encodeURIComponent(id)}`),
  slot: (c: ApiClient, period: number, thread: number) =>
    c.getMaybe<Envelope<SlotState>>(`/v1/slots/${period}/${thread}`),
  slotEvents: (
    c: ApiClient,
    period: number,
    thread: number,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredScEvent[]>>(
      `/v1/slots/${period}/${thread}/events?${paging(limit, cursor)}`,
    ),
  slotsRange: (
    c: ApiClient,
    params: {
      fromPeriod?: number;
      fromThread?: number;
      limit?: number;
      cursor?: string | null;
    } = {},
  ) => {
    const qs = new URLSearchParams();
    if (params.fromPeriod !== undefined)
      qs.set("from_period", String(params.fromPeriod));
    if (params.fromThread !== undefined)
      qs.set("from_thread", String(params.fromThread));
    if (params.limit !== undefined) qs.set("limit", String(params.limit));
    if (params.cursor) qs.set("cursor", params.cursor);
    const q = qs.toString();
    return c.get<Envelope<SlotState[]>>(`/v1/slots/range${q ? `?${q}` : ""}`);
  },
  recentOps: (
    c: ApiClient,
    limit = 25,
    cursor?: string | null,
    maxSlots = 256,
  ) =>
    c.get<Envelope<StoredOperation[]>>(
      `/v1/operations/recent?${paging(limit, cursor)}&max_slots=${maxSlots}`,
    ),
  addressBlocks: (
    c: ApiClient,
    addr: string,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredBlock[]>>(
      `/v1/addresses/${encodeURIComponent(addr)}/blocks?${paging(limit, cursor)}`,
    ),
  addressOps: (
    c: ApiClient,
    addr: string,
    role: "creator" | "target" = "creator",
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredOperation[]>>(
      `/v1/addresses/${encodeURIComponent(addr)}/ops?role=${role}&${paging(limit, cursor)}`,
    ),
  addressNodeState: (c: ApiClient, addr: string) =>
    c.getMaybe<Envelope<AddressNodeState>>(
      `/v1/addresses/${encodeURIComponent(addr)}/node_state`,
    ),

  /** Fetch the on-chain bytecode of a smart-contract (`AS…`) address.
   *  Returns `null` if the address has no bytecode (EOAs, or an `AS…`
   *  whose ledger row is empty). The returned URL is the one we
   *  successfully read from — handy for the download button to keep
   *  a single source of truth between in-page analysis and "save as". */
  addressBytecode: (c: ApiClient, addr: string) =>
    c.getBytes(`/v1/addresses/${encodeURIComponent(addr)}/bytecode`),
  addressDeferred: (
    c: ApiClient,
    addr: string,
    role: "sender" | "target" = "sender",
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredDeferredCall[]>>(
      `/v1/addresses/${encodeURIComponent(addr)}/deferred?role=${role}&${paging(limit, cursor)}`,
    ),
  slotTransfers: (
    c: ApiClient,
    period: number,
    thread: number,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredTransfer[]>>(
      `/v1/slots/${period}/${thread}/transfers?${paging(limit, cursor)}`,
    ),
  opTransfers: (
    c: ApiClient,
    id: string,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredTransfer[]>>(
      `/v1/operations/${encodeURIComponent(id)}/transfers?${paging(limit, cursor)}`,
    ),
  addressTransfers: (
    c: ApiClient,
    addr: string,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredTransfer[]>>(
      `/v1/addresses/${encodeURIComponent(addr)}/transfers?${paging(limit, cursor)}`,
    ),
  blockTransfers: (
    c: ApiClient,
    id: string,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredTransfer[]>>(
      `/v1/blocks/${encodeURIComponent(id)}/transfers?${paging(limit, cursor)}`,
    ),
  search: (c: ApiClient, q: string) =>
    c.get<Envelope<{ kind: string; hit: unknown; [k: string]: unknown }>>(
      `/v1/search?q=${encodeURIComponent(q)}`,
    ),

  // --- v1 additions -------------------------------------------------------

  /** Newest-first recent blocks (candidate + final, duped on block id). */
  recentBlocks: (c: ApiClient, limit = 25, cursor?: string | null) =>
    c.get<Envelope<StoredBlock[]>>(`/v1/blocks?${paging(limit, cursor)}`),

  blockOperations: (
    c: ApiClient,
    id: string,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredOperation[]>>(
      `/v1/blocks/${encodeURIComponent(id)}/operations?${paging(limit, cursor)}`,
    ),

  blockEndorsements: (
    c: ApiClient,
    id: string,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredEndorsement[]>>(
      `/v1/blocks/${encodeURIComponent(id)}/endorsements?${paging(limit, cursor)}`,
    ),

  blockDenunciations: (
    c: ApiClient,
    id: string,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredDenunciationEntry[]>>(
      `/v1/blocks/${encodeURIComponent(id)}/denunciations?${paging(limit, cursor)}`,
    ),

  opEvents: (
    c: ApiClient,
    id: string,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredScEvent[]>>(
      `/v1/operations/${encodeURIComponent(id)}/events?${paging(limit, cursor)}`,
    ),

  endorsement: (c: ApiClient, id: string) =>
    c.getMaybe<Envelope<StoredEndorsement>>(
      `/v1/endorsements/${encodeURIComponent(id)}`,
    ),

  denunciation: (c: ApiClient, hash: string) =>
    c.getMaybe<Envelope<StoredDenunciationEntry>>(
      `/v1/denunciations/${encodeURIComponent(hash)}`,
    ),

  recentDenunciations: (
    c: ApiClient,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredDenunciationEntry[]>>(
      `/v1/denunciations?${paging(limit, cursor)}`,
    ),

  receivedOps: (
    c: ApiClient,
    addr: string,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredOperation[]>>(
      `/v1/addresses/${encodeURIComponent(addr)}/received_ops?${paging(limit, cursor)}`,
    ),

  addressEndorsements: (
    c: ApiClient,
    addr: string,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredEndorsement[]>>(
      `/v1/addresses/${encodeURIComponent(addr)}/endorsements?${paging(limit, cursor)}`,
    ),

  addressDenunciations: (
    c: ApiClient,
    addr: string,
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredDenunciationEntry[]>>(
      `/v1/addresses/${encodeURIComponent(addr)}/denunciations?${paging(limit, cursor)}`,
    ),

  addressEvents: (
    c: ApiClient,
    addr: string,
    role: "emitter" | "caller" = "emitter",
    limit = 25,
    cursor?: string | null,
  ) =>
    c.get<Envelope<StoredScEvent[]>>(
      `/v1/addresses/${encodeURIComponent(addr)}/events?role=${role}&${paging(limit, cursor)}`,
    ),

  chart: (
    c: ApiClient,
    name:
      | "throughput"
      | "blocks_per_slot"
      | "finality_lag"
      | "active_addresses",
    opts: { windowSecs?: number; bucketSecs?: number } = {},
  ) => {
    const qs = new URLSearchParams();
    if (opts.windowSecs !== undefined)
      qs.set("window_secs", String(opts.windowSecs));
    if (opts.bucketSecs !== undefined)
      qs.set("bucket_secs", String(opts.bucketSecs));
    const q = qs.toString();
    return c.get<Envelope<ChartPoint[]>>(
      `/v1/charts/${name}${q ? `?${q}` : ""}`,
    );
  },

  openapi: (c: ApiClient) =>
    c.get<Record<string, unknown>>(`/v1/openapi.json`),
};

export type { ApiClient };
