import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "../lib/api";
import { fmtSlot, formatTs, slotTimestampMs, type ChainParams } from "../lib/format";
import type { ApiClient } from "../lib/api";
import type { Slot, SlotState, StoredBlock } from "../lib/types";
import { useSseSlots } from "../hooks/useSseSlots";

/**
 * Live multithreaded block DAG.
 *
 * Inspired by `block-explorer-old/massa-graph`, this widget renders a
 * horizontally-scrolling view of recent slots and blocks across every thread.
 *
 * Design choices driven by user feedback:
 *   - Parent links are drawn for ONLY the single most recently produced
 *     block — as soon as a newer block appears, the previous block's fan
 *     disappears instantly. This matches `explorerViewUpdate` in
 *     `massa-graph-old` which only animates `lastblc`.
 *   - Cross-period parent links are uniform cubic-bezier sigmoids so every
 *     arc shares the same shape — no mix of quadratic bows and straight
 *     lines.
 *   - Same-period lower-thread parents EXIT THE LEFT edge of the parent and
 *     loop around, so the visual "weaves" back to the child without
 *     overlapping intermediate block markers (old massa-graph look).
 *   - A periodic slot-range refresh heals stale finalization state even if
 *     the SSE stream misses an update.
 */
export function LiveDag({
  client,
  params,
  windowSec = 120,
}: {
  client: ApiClient;
  params: ChainParams;
  windowSec?: number;
}) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const navigate = useNavigate();

  const slotsRef = useRef<Map<string, SlotState>>(new Map());
  const blocksRef = useRef<Map<string, StoredBlock>>(new Map());
  // Wall-clock (ms) when we first placed each block in the cache. Doubles as
  // the anchor for the "spawn flash" animation — a block rendered within
  // SPAWN_MS of its firstSeen gets a brighter fill + halo that decays.
  const firstSeenRef = useRef<Map<string, number>>(new Map());
  // The single block whose parent fan is currently being animated. Kept in
  // a ref (not state) to avoid re-rendering on every tick; the 30fps
  // `draw()` loop reads it directly.
  //
  // Invariant: `latestBlockRef.current` is the block with the greatest slot
  // timestamp that has been firstSeen within the last FADE_MS. When a
  // newer block appears, we swap this ref and the previous block's lines
  // disappear on the NEXT frame.
  const latestBlockRef = useRef<{
    id: string;
    block: StoredBlock;
    startedAt: number;
  } | null>(null);
  const latestSeenTsRef = useRef<number>(-Infinity);
  const inflightRef = useRef<Set<string>>(new Set());
  const [, force] = useState(0);

  // Fetch enough newest-first slot pages to cover the rendered window. The
  // REST endpoint is server-capped at max_page_size=100 (≈ 3 periods ≈ 48 s
  // of history on a 32-thread chain), so a single request can't span the
  // typical 90 s viewport — we follow `cursor_next` until the oldest slot
  // in the page is older than the left edge, or the server runs out.
  const fetchWindow = async (): Promise<SlotState[]> => {
    const oldestEdgeMs = Date.now() - windowSec * 1000 - 2_000;
    const MAX_PAGES = 8;
    const out: SlotState[] = [];
    let cursor: string | null | undefined = null;
    for (let i = 0; i < MAX_PAGES; i++) {
      const env = await api.slotsRange(client, { limit: 100, cursor });
      if (!env.data.length) break;
      out.push(...env.data);
      const oldestTs = slotTimestampMs(
        env.data[env.data.length - 1].slot,
        params,
      );
      if (oldestTs <= oldestEdgeMs) break;
      if (!env.cursor_next) break;
      cursor = env.cursor_next;
    }
    return out;
  };

  const mergeSlotsIntoCache = (rows: SlotState[]): boolean => {
    const m = slotsRef.current;
    let changed = false;
    for (const s of rows) {
      const k = `${s.slot.period}:${s.slot.thread}`;
      const prev = m.get(k);
      if (
        !prev ||
        prev.last_updated_ts_ms !== s.last_updated_ts_ms ||
        prev.status !== s.status ||
        prev.final_block_id !== s.final_block_id
      ) {
        if (!prev || prev.last_updated_ts_ms <= s.last_updated_ts_ms) {
          m.set(k, s);
          changed = true;
        }
      }
    }
    return changed;
  };

  const initial = useQuery({
    queryKey: ["live-dag-initial", windowSec, params.threadCount, params.t0Ms],
    queryFn: fetchWindow,
    refetchOnWindowFocus: false,
    staleTime: 5_000,
  });

  useEffect(() => {
    if (!initial.data) return;
    if (mergeSlotsIntoCache(initial.data)) force((x) => x + 1);
  }, [initial.data]);

  // Periodic range refresh — heals stale finalisation state even if a couple
  // of SSE events got lost on the wire. Same pagination loop as the initial
  // fetch so a reconnect that lost 30+ s of history refills completely.
  useEffect(() => {
    const iv = setInterval(async () => {
      try {
        const rows = await fetchWindow();
        if (mergeSlotsIntoCache(rows)) force((x) => x + 1);
      } catch {
        /* transient — next tick will retry */
      }
    }, 4_000);
    return () => clearInterval(iv);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, windowSec, params.threadCount, params.t0Ms]);

  // Shared fetcher: resolves a single block id and, if it's a just-seen
  // block whose slot is newer than anything we've animated before, upgrades
  // it to the currently-animated `latestBlockRef` so its parent fan draws
  // in sync with the spawn flash started by the SSE path below.
  //
  // IMPORTANT: the flash animation is NOT started here — it's started the
  // moment the block id first shows up in a slot row via SSE (see below),
  // so the square appearing, the flash, and the parent fan are all driven
  // by the same tick. This function only *augments* the flash with parent
  // links once the block body is resolved (usually < 100 ms later on LAN).
  const fetchBlock = (id: string) => {
    const inflight = inflightRef.current;
    if (inflight.has(id)) return;
    if (blocksRef.current.has(id)) return;
    inflight.add(id);
    api
      .block(client, id)
      .then((env) => {
        if (!env?.data) return;
        blocksRef.current.set(id, env.data);
        const slotTs = slotTimestampMs(env.data.slot, params);
        // Only promote to animated latest when (a) we marked this block
        // as "just spawned" on the SSE tick, and (b) its slot is newer
        // than any previously-animated block. Without the firstSeen gate
        // we'd light up every block during the initial backfill.
        const spawnAt = firstSeenRef.current.get(id);
        if (spawnAt && slotTs > latestSeenTsRef.current) {
          latestSeenTsRef.current = slotTs;
          latestBlockRef.current = {
            id,
            block: env.data,
            // Anchor the fan fade to the SPAWN moment (SSE arrival), not
            // to `Date.now()`. The fan therefore shares the flash window
            // instead of starting after it.
            startedAt: spawnAt,
          };
        }
        force((x) => x + 1);
      })
      .catch(() => {
        /* ignore — periodic fetcher will retry */
      })
      .finally(() => {
        inflight.delete(id);
      });
  };

  // Live updates. When SSE brings us a slot row with a new block id we
  // haven't seen before, we (1) start the spawn-flash animation *now* so
  // the flash is locked to the moment the square appears, and (2) kick
  // off an immediate block-body fetch so parent links render inside the
  // same flash window rather than on the 1 s backstop interval below.
  const { events } = useSseSlots(client, 256);
  useEffect(() => {
    if (events.length === 0) return;
    const m = slotsRef.current;
    const fs = firstSeenRef.current;
    const now = Date.now();
    let changed = false;
    for (const e of events) {
      const k = `${e.slot.period}:${e.slot.thread}`;
      const prev = m.get(k);
      if (!prev || prev.last_updated_ts_ms < e.last_updated_ts_ms) {
        m.set(k, e);
        changed = true;
      }
      // Collect every block id the slot references right now. A slot can
      // move through candidate → finalised after its initial appearance,
      // and each new id is treated as its own spawn event.
      const ids = [
        ...(e.candidate_block_ids ?? []),
        ...(e.final_block_id ? [e.final_block_id] : []),
      ];
      const slotTs = slotTimestampMs(e.slot, params);
      // Only flash blocks whose slot is recent. Replayed history (e.g.
      // backfill on reconnect) must not retroactively strobe the canvas.
      const fresh = now - slotTs < 3_000;
      for (const id of ids) {
        if (!id) continue;
        if (!fs.has(id) && fresh) {
          fs.set(id, now);
          changed = true;
        }
        // Immediate high-priority fetch so parent links appear inside the
        // spawn-flash window. `fetchBlock` is a no-op if already loaded
        // or inflight.
        fetchBlock(id);
      }
    }
    if (changed) force((x) => x + 1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [events]);

  // Backstop fetcher — only catches block ids the SSE path missed (e.g.
  // slots populated by the periodic slot-range refresh, or SSE reconnect
  // replays where we deliberately suppressed the spawn flash). Runs at a
  // slow cadence since the SSE path covers the fresh tip of the DAG.
  useEffect(() => {
    const CONCURRENCY = 8;
    const iv = setInterval(() => {
      const inflight = inflightRef.current;
      const blocks = blocksRef.current;
      if (inflight.size >= CONCURRENCY) return;
      const now = Date.now();
      const cutoff = now - windowSec * 1000;

      const todo: { id: string; ts: number }[] = [];
      for (const s of slotsRef.current.values()) {
        const ts = slotTimestampMs(s.slot, params);
        if (ts < cutoff) continue;
        const ids = [
          ...(s.candidate_block_ids ?? []),
          ...(s.final_block_id ? [s.final_block_id] : []),
        ];
        for (const id of ids) {
          if (!id) continue;
          if (blocks.has(id)) continue;
          if (inflight.has(id)) continue;
          todo.push({ id, ts });
        }
      }
      todo.sort((a, b) => b.ts - a.ts);
      for (const { id } of todo.slice(0, CONCURRENCY - inflight.size)) {
        fetchBlock(id);
      }
    }, 1_000);
    return () => clearInterval(iv);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, params, windowSec]);

  // Periodically prune old slots/blocks to bound memory.
  useEffect(() => {
    const iv = setInterval(() => {
      const cutoff = Date.now() - (windowSec + 60) * 1000;
      const sm = slotsRef.current;
      let removed = 0;
      for (const [k, v] of sm) {
        const ts = slotTimestampMs(v.slot, params);
        if (ts < cutoff) {
          sm.delete(k);
          removed++;
        }
      }
      const bm = blocksRef.current;
      const fs = firstSeenRef.current;
      for (const [id, b] of bm) {
        const ts = slotTimestampMs(b.slot, params);
        if (ts < cutoff) {
          bm.delete(id);
          fs.delete(id);
          removed++;
        }
      }
      // If the currently-animated latest block aged out, drop it.
      const lb = latestBlockRef.current;
      if (lb && !bm.has(lb.id)) {
        latestBlockRef.current = null;
      }
      if (removed > 0) force((x) => x + 1);
    }, 10_000);
    return () => clearInterval(iv);
  }, [params, windowSec]);

  // Resize observer → keep canvas width matching its container.
  const [width, setWidth] = useState(800);
  useEffect(() => {
    if (!wrapperRef.current) return;
    const el = wrapperRef.current;
    const ro = new ResizeObserver(() => setWidth(el.clientWidth));
    ro.observe(el);
    setWidth(el.clientWidth);
    return () => ro.disconnect();
  }, []);

  // Hover hit-testing.
  const [hover, setHover] = useState<{
    x: number;
    y: number;
    slot: Slot;
    blockId?: string;
    final?: boolean;
    miss?: boolean;
    ts?: number;
  } | null>(null);

  // Draw loop — ~30 fps when visible, paused entirely when the tab is
  // hidden (no point spending CPU redrawing an offscreen canvas). We also
  // throttle to ~10 fps when no parent-link fade is active, since the
  // steady-state picture barely changes between frames.
  useEffect(() => {
    let raf = 0;
    let last = 0;
    const tick = (t: number) => {
      if (document.visibilityState === "hidden") {
        raf = requestAnimationFrame(tick);
        return;
      }
      const hasActiveFade =
        latestBlockRef.current !== null &&
        Date.now() - latestBlockRef.current.startedAt < 1000;
      const interval = hasActiveFade ? 33 : 100;
      if (t - last > interval) {
        last = t;
        draw();
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [width, params]);

  useEffect(() => {
    draw();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  });

  function hitTest(clientX: number, clientY: number) {
    const canvas = canvasRef.current;
    if (!canvas) return null;
    const rect = canvas.getBoundingClientRect();
    const x = clientX - rect.left;
    const y = clientY - rect.top;
    return hitPoint(x, y);
  }

  function hitPoint(x: number, y: number) {
    const layout = layoutCache.current;
    if (!layout) return null;
    const { laneHeight, slotSize, threadCount, rightEdgeTs, pxPerMs } = layout;
    // Blocks are drawn at y = (thread + 1) * laneHeight (rails are the
    // gridlines themselves). The inverse mapping is `thread = y/laneHeight
    // - 1`, rounded to the nearest integer. We widen the hit box to
    // include a half-lane above and below so users don't have to pixel-
    // hunt for tiny miss dots (slotSize/3 radius).
    const thread = Math.round(y / laneHeight - 1);
    if (thread < 0 || thread >= threadCount) return null;
    // Generous horizontal tolerance: at least one slot width OR half a
    // period in px, whichever is larger. Makes misses trivially clickable.
    const xTol = Math.max(slotSize * 1.2, laneHeight);
    let bestKey: string | null = null;
    let bestDist = xTol;
    for (const [k, s] of slotsRef.current) {
      if (s.slot.thread !== thread) continue;
      const ts = slotTimestampMs(s.slot, params);
      const sx = (rightEdgeTs - ts) * pxPerMs;
      const d = Math.abs(x - (layout.canvasWidth - sx));
      if (d < bestDist) {
        bestDist = d;
        bestKey = k;
      }
    }
    if (!bestKey) return null;
    const s = slotsRef.current.get(bestKey)!;
    const finalBlock = s.final_block_id ?? null;
    const candidate =
      (s.candidate_block_ids && s.candidate_block_ids[0]) ?? null;
    return {
      x,
      y,
      slot: s.slot,
      blockId: finalBlock ?? candidate ?? undefined,
      final: s.status === "final" && !!finalBlock,
      miss: s.status === "final" && s.is_miss,
      ts: slotTimestampMs(s.slot, params),
    };
  }

  const layoutCache = useRef<{
    canvasWidth: number;
    canvasHeight: number;
    laneHeight: number;
    slotSize: number;
    threadCount: number;
    rightEdgeTs: number;
    pxPerMs: number;
  } | null>(null);

  function draw() {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const threadCount = params.threadCount || 32;
    const dpr = window.devicePixelRatio || 1;
    const laneHeight = width < 500 ? 10 : width < 900 ? 13 : 15;
    const canvasHeight = laneHeight * (threadCount + 1);
    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(canvasHeight * dpr);
    canvas.style.height = `${canvasHeight}px`;
    canvas.style.width = `${width}px`;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, canvasHeight);

    const rightEdgeTs = Date.now();
    const leftEdgeTs = rightEdgeTs - windowSec * 1000;
    const pxPerMs = width / (rightEdgeTs - leftEdgeTs);
    const slotSize = laneHeight - 3;

    layoutCache.current = {
      canvasWidth: width,
      canvasHeight,
      laneHeight,
      slotSize,
      threadCount,
      rightEdgeTs,
      pxPerMs,
    };

    // Thread gridlines.
    ctx.lineWidth = 1;
    ctx.strokeStyle = "rgba(148, 163, 184, 0.12)";
    ctx.beginPath();
    for (let i = 0; i < threadCount; i++) {
      const y = (i + 1) * laneHeight;
      ctx.moveTo(0, y);
      ctx.lineTo(width, y);
    }
    ctx.stroke();

    // ---- Layer 1: parent connections -------------------------------------
    // We draw ONE fan — the latest block's. As soon as a newer block
    // claims the `latestBlockRef` ref the previous fan is dropped, exactly
    // like `massa-graph-old`'s single `lastblc` animation.
    const FADE_MS = 1000;
    const bm = blocksRef.current;
    const nowMs = Date.now();
    const latest = latestBlockRef.current;

    if (latest) {
      const age = nowMs - latest.startedAt;
      if (age < FADE_MS) {
        const fade = 1 - age / FADE_MS;
        const block = latest.block;
        const childTs = slotTimestampMs(block.slot, params);
        const cx = width - (rightEdgeTs - childTs) * pxPerMs;
        const cy = (block.slot.thread + 1) * laneHeight;
        const childInRange =
          childTs >= leftEdgeTs - 2000 && childTs <= rightEdgeTs + 2000;
        if (childInRange) {
          for (const pid of block.parents ?? []) {
            const parent = bm.get(pid);
            if (!parent) continue;
            const pts = slotTimestampMs(parent.slot, params);
            if (pts < leftEdgeTs - 5000) continue;
            const px = width - (rightEdgeTs - pts) * pxPerMs;
            const py = (parent.slot.thread + 1) * laneHeight;
            drawParentLink(
              ctx,
              px,
              py,
              cx,
              cy,
              slotSize,
              fade,
              parent.slot.period,
              parent.slot.thread,
              block.slot.period,
              block.slot.thread,
            );
          }
        }
      }
    }

    // ---- Layer 2: slot markers -------------------------------------------
    // When a block first lands in the cache (only for just-produced blocks,
    // not the initial backfill — see the fetcher) we "spawn" it with a
    // brighter fill + a soft halo that decays over SPAWN_MS. This gives a
    // professional-looking arrival pulse without any per-frame cost for
    // older blocks (they take the fast path unchanged).
    const SPAWN_MS = 600;
    const fs = firstSeenRef.current;
    for (const [, s] of slotsRef.current) {
      const ts = slotTimestampMs(s.slot, params);
      if (ts < leftEdgeTs || ts > rightEdgeTs + 2000) continue;
      const x = width - (rightEdgeTs - ts) * pxPerMs;
      const y = (s.slot.thread + 1) * laneHeight;

      const isFinal = s.status === "final";
      const isMiss = isFinal && s.is_miss;
      const hasBlock =
        (s.final_block_id && s.final_block_id.length > 0) ||
        (s.candidate_block_ids && s.candidate_block_ids.length > 0);

      if (isMiss) {
        // Miss dot — opaque gray, distinct from semi-transparent "no-data"
        // empty-slot ticks below. Slightly smaller than a block square so
        // missed slots still read as "different" at a glance.
        ctx.fillStyle = "rgba(148, 163, 184, 0.95)";
        ctx.beginPath();
        ctx.arc(x, y, slotSize / 2.6, 0, Math.PI * 2);
        ctx.fill();
        continue;
      }
      if (!hasBlock) {
        ctx.fillStyle = "rgba(148, 163, 184, 0.25)";
        ctx.beginPath();
        ctx.arc(x, y, slotSize / 4, 0, Math.PI * 2);
        ctx.fill();
        continue;
      }

      const primaryBlockId = s.final_block_id || s.candidate_block_ids?.[0];
      const spawnAt = primaryBlockId ? fs.get(primaryBlockId) : undefined;
      const spawnAge = spawnAt ? nowMs - spawnAt : Infinity;
      const spawning = spawnAge < SPAWN_MS;
      const fill = isFinal ? "#22c55e" : "#3b82f6";

      if (spawning) {
        // Ease-out: starts at 1, ends at 0. Non-linear so the flash fades
        // quickly and then settles to the steady-state look.
        const t = spawnAge / SPAWN_MS;
        const pulse = Math.pow(1 - t, 2);
        // Soft halo (additive white with alpha). Sized slightly larger than
        // the marker and fades along with the pulse.
        const haloR = slotSize * (0.75 + 1.2 * pulse);
        const grad = ctx.createRadialGradient(x, y, 0, x, y, haloR);
        grad.addColorStop(0, `rgba(255, 255, 255, ${(0.55 * pulse).toFixed(3)})`);
        grad.addColorStop(1, "rgba(255, 255, 255, 0)");
        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.arc(x, y, haloR, 0, Math.PI * 2);
        ctx.fill();
        // Brighter fill that lerps back to the steady colour.
        ctx.fillStyle = isFinal
          ? `rgba(167, 243, 208, ${(0.4 + 0.6 * (1 - pulse)).toFixed(3)})`
          : `rgba(191, 219, 254, ${(0.4 + 0.6 * (1 - pulse)).toFixed(3)})`;
        const grow = 1 + 0.35 * pulse;
        const g = slotSize * grow;
        ctx.fillRect(x - g / 2, y - g / 2, g, g);
        // Steady-state marker drawn on top so the edges settle crisp.
        ctx.fillStyle = fill;
        ctx.fillRect(x - slotSize / 2, y - slotSize / 2, slotSize, slotSize);
      } else {
        ctx.fillStyle = fill;
        ctx.fillRect(
          x - slotSize / 2,
          y - slotSize / 2,
          slotSize,
          slotSize,
        );
      }

      const extras = (s.candidate_block_ids?.length ?? 0) - 1;
      if (extras > 0) {
        ctx.fillStyle = "rgba(239, 68, 68, 0.7)";
        ctx.fillRect(
          x + slotSize / 2 + 1,
          y - slotSize / 2,
          Math.max(2, slotSize / 3),
          slotSize,
        );
      }
    }

    // "Now" cursor on the right edge.
    ctx.strokeStyle = "rgba(139, 92, 246, 0.9)";
    ctx.lineWidth = 1;
    ctx.setLineDash([3, 3]);
    ctx.beginPath();
    ctx.moveTo(width - 0.5, 0);
    ctx.lineTo(width - 0.5, canvasHeight);
    ctx.stroke();
    ctx.setLineDash([]);
  }

  return (
    <div className="card">
      <header className="flex flex-wrap items-center justify-between gap-2 mb-3">
        <h2 className="text-sm uppercase tracking-wide text-muted">
          Live multithreaded DAG
        </h2>
        <div className="text-xs text-muted flex gap-3 flex-wrap">
          <LegendDot color="#22c55e" label="final block" />
          <LegendDot color="#3b82f6" label="candidate" />
          <LegendDot color="rgba(239,68,68,0.7)" label="fork" />
          <LegendDot color="rgba(148,163,184,0.95)" label="miss" />
          <LegendDot color="rgba(126,158,232,0.85)" label="parent link" />
        </div>
      </header>
      <div ref={wrapperRef} className="relative w-full overflow-hidden">
        <canvas
          ref={canvasRef}
          className="w-full block cursor-pointer"
          onMouseMove={(e) => {
            const h = hitTest(e.clientX, e.clientY);
            setHover(h);
          }}
          onMouseLeave={() => setHover(null)}
          onClick={(e) => {
            const h = hitTest(e.clientX, e.clientY);
            if (!h) return;
            if (h.blockId) navigate(`/block/${h.blockId}`);
            else navigate(`/slot/${h.slot.period}/${h.slot.thread}`);
          }}
        />
        {hover && (
          <div
            className="pointer-events-none absolute bg-panel border border-border rounded-md px-2 py-1 text-xs font-mono shadow"
            style={{
              left: Math.min(hover.x + 10, Math.max(0, width - 260)),
              top: Math.max(hover.y - 42, 0),
              whiteSpace: "nowrap",
            }}
          >
            <div>{fmtSlot(hover.slot)}</div>
            {hover.ts != null && (
              <div className="text-muted">{formatTs(hover.ts)}</div>
            )}
            {hover.blockId ? (
              <div className="text-muted">
                {hover.blockId.slice(0, 10)}… · {hover.final ? "final" : "candidate"}
              </div>
            ) : hover.miss ? (
              <div className="text-muted">miss · click for slot</div>
            ) : null}
          </div>
        )}
      </div>
      <div className="flex justify-between text-[10px] text-muted mt-1">
        <span>{windowSec}s ago</span>
        <span>now</span>
      </div>
    </div>
  );
}

/**
 * Draw a single parent→child link.
 *
 * The only three visual cases we care about:
 *
 *   a) Same thread (parent is a previous slot in the child's thread):
 *      cubic bezier bowing ABOVE the thread rail so the curve doesn't
 *      overlap intermediate block markers. Enters the child at its LEFT
 *      edge, leaves the parent from its RIGHT edge (old massa-graph look).
 *
 *   b) Same period, parent on a LOWER thread: the parent is to the LEFT
 *      of the child on the x-axis but we want the line to exit the
 *      parent's LEFT edge and curl around. This yields the nice "weave"
 *      visual from `massa-graph-old` where same-period parents clearly
 *      pre-date the child. Modelled as a cubic bezier whose first
 *      control point is shifted far left of the parent so the curve
 *      swings away from the rails before coming back down to the child's
 *      left edge.
 *
 *   c) Previous-period parent (or any cross-period): UNIFORM horizontal
 *      cubic bezier from parent's RIGHT edge to child's LEFT edge. Every
 *      such link shares the same control-point layout so the fan looks
 *      like a bundle of identical sigmoids rather than a mix of arcs and
 *      straight lines.
 */
function drawParentLink(
  ctx: CanvasRenderingContext2D,
  fromx: number,
  fromy: number,
  tox: number,
  toy: number,
  blockSize: number,
  fade: number,
  parentPeriod: number,
  parentThread: number,
  childPeriod: number,
  childThread: number,
) {
  const a = Math.max(0, Math.min(1, fade));
  if (a <= 0) return;
  ctx.strokeStyle = `rgba(126, 158, 232, ${a.toFixed(3)})`;
  ctx.lineWidth = 1.5;
  ctx.beginPath();

  const endX = tox - blockSize / 2; // child's LEFT edge

  // Case A: same thread (time-chain along the child's rail).
  if (parentThread === childThread) {
    const startX = fromx + blockSize / 2; // parent RIGHT edge
    const dx = endX - startX;
    if (dx <= 0) {
      // Parent overlaps child horizontally — straight fallback.
      ctx.moveTo(fromx, fromy);
      ctx.lineTo(tox, toy);
      ctx.stroke();
      return;
    }
    // Bow upward, but clamp the arc so it never rises into the previous
    // thread's row (which would visually cross neighbouring blocks — see
    // explorer feedback about "the last two threads crossing"). Block
    // centers are `laneHeight` apart and each block is `blockSize` tall;
    // the gap between block edges of consecutive threads is therefore
    // `laneHeight - blockSize`. We give the bow at most ~70% of that gap.
    const laneHeight = blockSize + 3;
    const maxBow = Math.max(2, Math.floor((laneHeight - blockSize) * 0.7));
    const bow = Math.min(maxBow, Math.max(blockSize * 0.2, dx * 0.04));
    const bowY = fromy - bow;
    ctx.moveTo(startX, fromy);
    ctx.bezierCurveTo(
      startX + dx * 0.33,
      bowY,
      startX + dx * 0.67,
      bowY,
      endX,
      toy,
    );
    ctx.stroke();
    return;
  }

  // Case B: same period, parent on a lower thread — exit parent LEFT edge
  // and loop back under/over to the child's LEFT edge.
  if (parentPeriod === childPeriod && parentThread < childThread) {
    const startX = fromx - blockSize / 2; // parent LEFT edge
    const dx = endX - startX;
    const swing = Math.max(blockSize * 2, Math.abs(dx) * 0.8);
    // First control point: far left of the parent, pulled slightly up so
    // the curve arches away from the rails.
    const c1x = startX - swing;
    const c1y = fromy;
    // Second control point: just left of the child, at the child's rail
    // level so we enter horizontally.
    const c2x = endX - Math.max(blockSize * 2, dx * 0.3);
    const c2y = toy;
    ctx.moveTo(startX, fromy);
    ctx.bezierCurveTo(c1x, c1y, c2x, c2y, endX, toy);
    ctx.stroke();
    return;
  }

  // Case C: cross-period — uniform sigmoid. Parent RIGHT → child LEFT
  // with symmetric horizontal control points. Every cross-period parent
  // of the same child therefore renders with an identical bend.
  const startX = fromx + blockSize / 2;
  const dx = endX - startX;
  const mid = Math.max(blockSize * 2, Math.abs(dx) * 0.5);
  ctx.moveTo(startX, fromy);
  ctx.bezierCurveTo(startX + mid, fromy, endX - mid, toy, endX, toy);
  ctx.stroke();
}

function LegendDot({ color, label }: { color: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1 whitespace-nowrap">
      <span
        className="inline-block w-2.5 h-2.5 rounded-sm"
        style={{ background: color }}
      />
      {label}
    </span>
  );
}
