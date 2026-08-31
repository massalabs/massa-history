import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import timezone from "dayjs/plugin/timezone";
import advancedFormat from "dayjs/plugin/advancedFormat";
import type { Slot, StoredOperation } from "./types";

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(advancedFormat);

export function shortId(id: string | null | undefined, head = 6, tail = 4): string {
  if (!id) return "—";
  if (id.length <= head + tail + 2) return id;
  return `${id.slice(0, head)}…${id.slice(-tail)}`;
}

/**
 * Full ISO-8601 timestamp including the viewer's timezone offset, e.g.
 * `2026-04-22T17:36:00+02:00`. We deliberately include the offset so
 * timestamps are never ambiguous across locales / hosts / browser tabs.
 */
export function formatTs(ms: number | null | undefined): string {
  if (ms == null || ms === 0) return "—";
  return dayjs(ms).format("YYYY-MM-DDTHH:mm:ssZ");
}

/** Short UTC form, useful inline where the timezone suffix would be noisy. */
export function formatTsUtc(ms: number | null | undefined): string {
  if (ms == null || ms === 0) return "—";
  return dayjs(ms).utc().format("YYYY-MM-DD HH:mm:ss [UTC]");
}

export function formatRelative(ms: number | null | undefined): string {
  if (ms == null) return "—";
  const diff = Date.now() - ms;
  const abs = Math.abs(diff);
  const suffix = diff < 0 ? "from now" : "ago";
  if (abs < 1_000) return "just now";
  if (abs < 60_000) return `${Math.round(abs / 1000)}s ${suffix}`;
  if (abs < 3_600_000) return `${Math.round(abs / 60_000)}m ${suffix}`;
  if (abs < 86_400_000) return `${Math.round(abs / 3_600_000)}h ${suffix}`;
  return `${Math.round(abs / 86_400_000)}d ${suffix}`;
}

export function formatNumber(n: number | string | null | undefined): string {
  if (n == null) return "—";
  const num = typeof n === "string" ? Number(n) : n;
  if (!isFinite(num)) return String(n);
  return num.toLocaleString();
}

/** Human-readable MAS amount from a `nMAS` integer count (1 MAS = 1e9 nMAS). */
export function fmtMas(nmas: number | null | undefined): string {
  if (nmas == null) return "—";
  const mas = nmas / 1_000_000_000;
  return `${mas.toLocaleString(undefined, { maximumFractionDigits: 9 })} MAS`;
}

/** Format an nMAS amount supplied as a decimal-string. Preserves full
 *  precision for balances that overflow JS `Number` (>~9 M MAS) by going
 *  through string arithmetic instead of float division. */
export function fmtMasString(nmas: string | null | undefined): string {
  if (nmas == null) return "—";
  const s = nmas.replace(/^\+/, "");
  if (!/^\d+$/.test(s)) return "—";
  // Left-pad to at least 10 digits so we always have an integer part.
  const padded = s.padStart(10, "0");
  const intPart = padded.slice(0, -9).replace(/^0+(?=\d)/, "");
  const fracPart = padded.slice(-9).replace(/0+$/, "");
  const groupedInt = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return fracPart ? `${groupedInt}.${fracPart} MAS` : `${groupedInt} MAS`;
}

/** Canonical display of a Massa slot: `(period, thread)` */
export function fmtSlot(s: Slot | null | undefined): string {
  if (!s) return "—";
  return `(${s.period}, ${s.thread})`;
}

export interface ChainParams {
  genesisTimestampMs: number;
  t0Ms: number;
  threadCount: number;
}

/** Derive the wall-clock timestamp for the START of a slot. */
export function slotTimestampMs(s: Slot, params: ChainParams): number {
  const { genesisTimestampMs, t0Ms, threadCount } = params;
  if (!threadCount || !t0Ms) return 0;
  const threadOffset = Math.floor((t0Ms * s.thread) / threadCount);
  return genesisTimestampMs + s.period * t0Ms + threadOffset;
}

/** Earliest inclusion slot for an op. Prefers `inclusions[0]` (current
 *  indexer API); falls back to the legacy `first_included_slot` singleton
 *  for older responses. `null` means still pending / not seen in a block. */
export function firstIncludedSlot(op: StoredOperation): Slot | null {
  const fromList = op.inclusions?.[0]?.slot;
  if (fromList) return fromList;
  return op.first_included_slot ?? null;
}
