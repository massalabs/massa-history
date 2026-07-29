import type { ReactNode } from "react";
import { Link } from "react-router-dom";
import {
  fmtSlot,
  formatRelative,
  formatTs,
  shortId,
  slotTimestampMs,
  type ChainParams,
} from "../lib/format";
import type { BlockStatus, Slot, SlotStatus } from "../lib/types";

export function StatusBadge({ status }: { status: SlotStatus }) {
  const cls =
    status === "final"
      ? "badge-final"
      : status === "candidate"
        ? "badge-candidate"
        : "badge-miss";
  return <span className={`badge ${cls}`}>{status}</span>;
}

export function BlockStatusBadge({ status }: { status: BlockStatus }) {
  const cls =
    status === "final"
      ? "badge-final"
      : status === "seen_candidate"
        ? "badge-candidate"
        : "badge-miss";
  const label = status === "seen_candidate" ? "candidate" : status;
  return <span className={`badge ${cls}`}>{label}</span>;
}

/**
 * Responsive id renderer: short label on narrow viewports, full string on
 * wider ones. Rendered as two spans toggled purely in CSS so there is no
 * re-render on resize. `xl` (1280px) is chosen for 52-char block/op ids
 * (too long for md/lg desktop table rows), `md` (768px) for 38-char
 * addresses which fit comfortably once the viewport isn't phone-width.
 *
 * When `short={false}` the full string is always rendered (with
 * `break-all` so narrow viewports wrap cleanly instead of breaking the
 * layout). This is the mode used by detail pages where the id is the
 * page's primary subject.
 */
function ResponsiveId({
  full,
  short,
  breakpoint,
}: {
  full: string;
  short: string;
  breakpoint: "md" | "xl";
}) {
  // `whitespace-nowrap` on the short form so the ellipsis character does
  // NOT become a line break opportunity — otherwise `AU12Ce…m3Hs9x` can
  // wrap to two lines in narrow mobile table cells, which looks awful.
  const shortCls =
    breakpoint === "xl" ? "xl:hidden whitespace-nowrap" : "md:hidden whitespace-nowrap";
  const fullCls =
    breakpoint === "xl" ? "hidden xl:inline break-all" : "hidden md:inline break-all";
  return (
    <>
      <span className={shortCls}>{short}</span>
      <span className={fullCls}>{full}</span>
    </>
  );
}

export function BlockLink({ id, short = true }: { id: string; short?: boolean }) {
  return (
    <Link to={`/block/${id}`} className="font-mono">
      {short ? (
        <ResponsiveId full={id} short={shortId(id)} breakpoint="xl" />
      ) : (
        <span className="break-all">{id}</span>
      )}
    </Link>
  );
}

export function OpLink({ id, short = true }: { id: string; short?: boolean }) {
  return (
    <Link to={`/op/${id}`} className="font-mono">
      {short ? (
        <ResponsiveId full={id} short={shortId(id)} breakpoint="xl" />
      ) : (
        <span className="break-all">{id}</span>
      )}
    </Link>
  );
}

export function AddrLink({ addr, short = true }: { addr: string; short?: boolean }) {
  return (
    <Link to={`/address/${addr}`} className="font-mono">
      {short ? (
        <ResponsiveId full={addr} short={shortId(addr, 6, 6)} breakpoint="md" />
      ) : (
        <span className="break-all">{addr}</span>
      )}
    </Link>
  );
}

export function SlotLink({ slot }: { slot: Slot }) {
  return (
    <Link
      to={`/slot/${slot.period}/${slot.thread}`}
      className="font-mono whitespace-nowrap"
      title={`slot (period ${slot.period}, thread ${slot.thread})`}
    >
      {fmtSlot(slot)}
    </Link>
  );
}

/**
 * Slot reference: link using (period, thread) + the deterministic derived
 * wall-clock timestamp of the slot. Use this instead of bare slot numbers.
 */
export function SlotRef({
  slot,
  params,
  timeMode = "relative",
}: {
  slot: Slot;
  params: ChainParams;
  /** "relative" = "5s ago", "absolute" = "2026-04-22 15:30:00", "both" = both */
  timeMode?: "relative" | "absolute" | "both" | "none";
}) {
  const ts = slotTimestampMs(slot, params);
  const rel = formatRelative(ts);
  const abs = formatTs(ts);
  return (
    <span className="inline-flex items-center gap-1 flex-wrap">
      <SlotLink slot={slot} />
      {timeMode !== "none" && (
        <span className="text-muted text-xs whitespace-nowrap" title={abs}>
          ·{" "}
          {timeMode === "absolute"
            ? abs
            : timeMode === "both"
              ? `${rel} (${abs})`
              : rel}
        </span>
      )}
    </span>
  );
}

/** Deterministic timestamp display for a slot (no link). */
export function SlotTimestamp({
  slot,
  params,
  mode = "relative",
}: {
  slot: Slot;
  params: ChainParams;
  mode?: "relative" | "absolute" | "both";
}) {
  const ts = slotTimestampMs(slot, params);
  if (mode === "absolute") return <span title={`${ts} ms`}>{formatTs(ts)}</span>;
  if (mode === "both")
    return (
      <span title={`${ts} ms`}>
        {formatTs(ts)} <span className="text-muted">({formatRelative(ts)})</span>
      </span>
    );
  return <span title={formatTs(ts)}>{formatRelative(ts)}</span>;
}

export function Panel({ title, children, action }: {
  title: string;
  children: ReactNode;
  action?: ReactNode;
}) {
  return (
    <section className="card">
      <header className="flex items-center justify-between mb-3">
        <h2 className="text-sm uppercase tracking-wide text-muted">{title}</h2>
        {action}
      </header>
      {children}
    </section>
  );
}

export function Loading({ msg = "Loading…" }: { msg?: string }) {
  return <div className="text-muted text-sm">{msg}</div>;
}

export function ErrorMsg({ err }: { err: unknown }) {
  const msg =
    err instanceof Error ? err.message : typeof err === "string" ? err : "Error";
  return <div className="text-bad text-sm">Error: {msg}</div>;
}

export function NotFound({ what }: { what: string }) {
  return <div className="text-muted">No {what} found.</div>;
}

/**
 * Pretty renderer for the SlotCompleteness bitmap. Each flag becomes a
 * small pill; present flags are coloured by topic, missing flags are muted.
 * Tooltips explain in plain terms what each ingestion stage means.
 */
export function CompletenessBadges({
  c,
}: {
  c: {
    block_body_stored: boolean;
    exec_output_final: boolean;
    exec_output_candidate: boolean;
    transfers_stored: boolean;
  };
}) {
  const items: { label: string; ok: boolean; className: string; hint: string }[] =
    [
      {
        label: "block body",
        ok: c.block_body_stored,
        className: "bg-emerald-500/15 text-emerald-300 border-emerald-500/30",
        hint: "The full block content (header, operations, endorsements) has been indexed.",
      },
      {
        label: "execution (final)",
        ok: c.exec_output_final,
        className: "bg-green-600/20 text-green-300 border-green-600/40",
        hint: "The slot's execution outputs were observed in the finalised ledger.",
      },
      {
        label: "execution (candidate)",
        ok: c.exec_output_candidate,
        className: "bg-sky-500/15 text-sky-300 border-sky-500/30",
        hint: "The slot's execution outputs were observed in the candidate (not yet finalised) ledger.",
      },
      {
        label: "transfers",
        ok: c.transfers_stored,
        className: "bg-indigo-500/15 text-indigo-300 border-indigo-500/30",
        hint: "Coin and roll transfers triggered in this slot have been decoded and stored.",
      },
    ];
  return (
    <div
      className="flex flex-wrap gap-1.5"
      title="Each pill shows whether a category of slot data has been fully ingested by the indexer. Greyed-out pills mean that part is still missing (e.g. peer or AWS history not yet backfilled)."
    >
      {items.map((it) => (
        <span
          key={it.label}
          className={`inline-flex items-center gap-1 px-2 py-0.5 text-[11px] rounded-full border ${
            it.ok
              ? it.className
              : "bg-panel text-muted border-border opacity-60"
          }`}
          title={it.hint}
        >
          <span
            className={`inline-block w-1.5 h-1.5 rounded-full ${
              it.ok ? "bg-current" : "bg-muted/40"
            }`}
          />
          {it.label}
        </span>
      ))}
    </div>
  );
}

/** Simple key/value row helper for detail pages. Wraps label and value cells. */
export function KV({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <>
      <dt>{label}</dt>
      <dd>{children}</dd>
    </>
  );
}

/** Small pill rendering an operation kind (`transaction`, `call_sc`, …). */
export function OpKindPill({ kind }: { kind: string }) {
  return (
    <span className="inline-block px-1.5 py-0.5 rounded-full bg-panel border border-border text-[10px] uppercase tracking-wide text-muted">
      {kind.replace("_", " ")}
    </span>
  );
}
