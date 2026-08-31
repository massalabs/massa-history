import { describe, expect, it } from "vitest";
import {
  shortId,
  formatRelative,
  fmtSlot,
  firstIncludedSlot,
  slotTimestampMs,
  formatTs,
  formatTsUtc,
  fmtToken,
} from "../lib/format";
import type { StoredOperation } from "../lib/types";

describe("format helpers", () => {
  it("shortens long ids", () => {
    expect(shortId("B12AAAAAAAAAAAAAAAAAAAAAAA")).toMatch(/^B12AAA…/);
  });
  it("keeps short ids", () => {
    expect(shortId("abc")).toBe("abc");
  });
  it("handles null", () => {
    expect(shortId(null)).toBe("—");
  });
  it("relative time", () => {
    expect(formatRelative(Date.now())).toMatch(/just now|s ago/);
  });
  it("formats slot as (period, thread)", () => {
    expect(fmtSlot({ period: 42, thread: 7 })).toBe("(42, 7)");
    expect(fmtSlot(null)).toBe("—");
  });
  it("formats ISO timestamp with timezone offset", () => {
    const ms = Date.UTC(2026, 3, 22, 17, 36, 0);
    const s = formatTs(ms);
    // Either +NN:NN or -NN:NN or Z suffix, and an uppercase T separator.
    expect(s).toMatch(/^2026-04-22T\d{2}:\d{2}:00(?:[+-]\d{2}:\d{2}|Z)$/);
  });
  it("formats UTC timestamp explicitly", () => {
    const ms = Date.UTC(2026, 3, 22, 17, 36, 0);
    expect(formatTsUtc(ms)).toBe("2026-04-22 17:36:00 UTC");
  });
  it("null timestamps render as dash", () => {
    expect(formatTs(null)).toBe("—");
    expect(formatTs(0)).toBe("—");
  });
  it("derives slot timestamp from chain params", () => {
    const params = { genesisTimestampMs: 1000, t0Ms: 16000, threadCount: 32 };
    expect(slotTimestampMs({ period: 0, thread: 0 }, params)).toBe(1000);
    expect(slotTimestampMs({ period: 1, thread: 0 }, params)).toBe(17000);
    // thread 16 is half a period into the period
    expect(slotTimestampMs({ period: 0, thread: 16 }, params)).toBe(9000);
  });
  it("firstIncludedSlot prefers inclusions[0] over legacy singleton", () => {
    const base = {
      schema_version: 2,
      id: "O1",
      creator: "AU1",
      target: null,
      kind: "transaction" as const,
      expire_period: 1,
      fee_nmas: 0,
      thread: 0,
      candidate_exec_status: null,
      final_exec_status: null,
      first_seen_ts_ms: 0,
    };
    const fromList: StoredOperation = {
      ...base,
      inclusions: [
        { slot: { period: 10, thread: 2 }, block_id: "B1" },
        { slot: { period: 11, thread: 3 }, block_id: "B2" },
      ],
      first_included_slot: { period: 99, thread: 0 },
    };
    expect(firstIncludedSlot(fromList)).toEqual({ period: 10, thread: 2 });

    const legacyOnly: StoredOperation = {
      ...base,
      first_included_slot: { period: 5, thread: 1 },
    };
    expect(firstIncludedSlot(legacyOnly)).toEqual({ period: 5, thread: 1 });

    const pending: StoredOperation = { ...base, inclusions: [] };
    expect(firstIncludedSlot(pending)).toBeNull();
  });
  it("formats token amounts with decimals", () => {
    expect(fmtToken("1500000", 6, "USDC.e")).toBe("1.5 USDC.e");
    expect(fmtToken("1000", 0, "X")).toBe("1,000 X");
    expect(fmtToken("0", 6, "USDC.e")).toBe("0 USDC.e");
  });
});
