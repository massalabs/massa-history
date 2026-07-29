import { useCallback, useState } from "react";

/**
 * Shared pagination controls for every long list in the explorer.
 *
 * Design notes:
 *   - Pagination is strictly cursor-based. The backend enforces a hard
 *     page-size cap of 100 and returns an opaque `cursor_next` token,
 *     which we feed back as `?cursor=…` to request the next page. This
 *     avoids RocksDB having to iterate through every earlier row for
 *     deep pages — each "Next" click is a constant-cost seek.
 *   - Because cursors are opaque, "Previous" is implemented as a client
 *     side stack of cursors we've visited. This costs zero extra work
 *     on the server and lets users step backwards through a live list
 *     without re-keying against a moving tail.
 *   - The component is purely presentational — state management belongs
 *     to `usePaged` below so each page can own its own cursor stack.
 */
export function Paginator({
  page,
  pageSize,
  hasMore,
  loading,
  onPrev,
  onNext,
  count,
  rightSlot,
}: {
  page: number;
  pageSize: number;
  hasMore: boolean;
  loading?: boolean;
  onPrev: () => void;
  onNext: () => void;
  /** Number of rows on the current page — for the "n–m" hint. */
  count?: number;
  rightSlot?: React.ReactNode;
}) {
  if (page === 0 && !hasMore && !loading && (count ?? 0) <= pageSize) {
    return null;
  }
  const start = page * pageSize + 1;
  const end = start + Math.max(0, (count ?? 0) - 1);
  const disablePrev = loading || page === 0;
  const disableNext = loading || !hasMore;
  return (
    <div className="flex items-center justify-between gap-3 mt-3 text-xs text-muted flex-wrap">
      <div className="whitespace-nowrap">
        {count === 0 && page === 0
          ? "—"
          : `Showing ${start}${end > start ? `–${end}` : ""}`}
        {loading ? " · loading…" : ""}
      </div>
      <div className="flex items-center gap-2 flex-wrap">
        {rightSlot}
        <button
          type="button"
          className="btn disabled:opacity-40 disabled:cursor-not-allowed"
          disabled={disablePrev}
          onClick={onPrev}
          title="Previous page"
        >
          ← Prev
        </button>
        <span
          className="px-1 whitespace-nowrap"
          title={
            "Pagination is cursor-based: jumping to an arbitrary page would require " +
            "scanning every preceding row, so only sequential navigation is exposed."
          }
        >
          Page {page + 1}
        </span>
        <button
          type="button"
          className="btn disabled:opacity-40 disabled:cursor-not-allowed"
          disabled={disableNext}
          onClick={onNext}
          title="Next page"
        >
          Next →
        </button>
      </div>
    </div>
  );
}

/**
 * Cursor-based pagination state for a single list view.
 *
 * Usage pattern from a page component:
 *
 *     const paged = usePaged(25);
 *     const { data } = useQuery({
 *       queryKey: ["recent-blocks", paged.cursor, paged.limit],
 *       queryFn: () => api.recentBlocks(client, paged.limit, paged.cursor),
 *     });
 *     useEffect(() => paged.setLastResponse(data ?? null), [data]);
 *
 *     <Paginator
 *       page={paged.page}
 *       pageSize={paged.limit}
 *       hasMore={paged.hasMore}
 *       onPrev={paged.prev}
 *       onNext={paged.next}
 *     />
 *
 * The hook keeps a stack of cursors seen so far. `next` pushes the
 * current cursor and activates `cursor_next` from the last response;
 * `prev` pops one level. The caller MUST feed the last response back
 * via `setLastResponse` — that's what the hook uses to populate
 * `hasMore` and the forward cursor.
 *
 * The hook does not persist cursors across navigations. React Query's
 * cache already handles repeat visits to the same URL efficiently.
 */
export interface PagedResponseLike {
  data?: unknown;
  cursor_next?: string | null;
  has_more?: boolean;
}

/**
 * Pagination over an in-memory array. Used when we already have the
 * full list client-side (e.g. a block's `operation_ids` field) and
 * just want to show it page by page without round-tripping to the
 * backend.
 */
export function useLocalPaged(initialPageSize = 25) {
  const HARD_CAP = 100;
  const [pageSize] = useState(Math.min(initialPageSize, HARD_CAP));
  const [page, setPage] = useState(0);
  const next = useCallback(() => setPage((p) => p + 1), []);
  const prev = useCallback(() => setPage((p) => Math.max(0, p - 1)), []);
  const reset = useCallback(() => setPage(0), []);
  const offset = page * pageSize;
  return { page, pageSize, offset, next, prev, reset };
}

export function usePaged(initialPageSize = 25) {
  const HARD_CAP = 100;
  const [pageSize] = useState(Math.min(initialPageSize, HARD_CAP));
  // `stack` holds the cursors we sent for pages [0, current]. Index 0
  // is always `null` (first page). Pushing a new entry advances one
  // page forward; popping steps back.
  const [stack, setStack] = useState<(string | null)[]>([null]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);

  const cursor = stack[stack.length - 1] ?? null;
  const page = stack.length - 1;

  const next = useCallback(() => {
    if (!nextCursor) return;
    setStack((s) => [...s, nextCursor]);
    setNextCursor(null);
    setHasMore(false);
  }, [nextCursor]);

  const prev = useCallback(() => {
    setStack((s) => (s.length > 1 ? s.slice(0, -1) : s));
    setNextCursor(null);
    setHasMore(false);
  }, []);

  const reset = useCallback(() => {
    setStack([null]);
    setNextCursor(null);
    setHasMore(false);
  }, []);

  const setLastResponse = useCallback((resp: PagedResponseLike | null) => {
    if (!resp) {
      setNextCursor(null);
      setHasMore(false);
      return;
    }
    setNextCursor(resp.cursor_next ?? null);
    setHasMore(Boolean(resp.has_more || resp.cursor_next));
  }, []);

  return {
    page,
    pageSize,
    limit: pageSize,
    cursor,
    hasMore,
    next,
    prev,
    reset,
    setLastResponse,
  };
}
