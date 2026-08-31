import { Link } from "react-router-dom";
import { fmtMas, fmtToken, shortId } from "../lib/format";
import type { CoinOrigin, StoredTransfer, TransferValue } from "../lib/types";
import { AddrLink, BlockLink, OpLink } from "./Bits";

/** Pretty label for a CoinOrigin — slightly shortened & title-cased. */
function formatOrigin(o: CoinOrigin): string {
  if (o.kind === "other") return `other(${o.code})`;
  // Replace abbreviations for readability.
  return o.kind
    .replace(/_/g, " ")
    .replace("op ", "op-")
    .replace("abi ", "abi-")
    .replace("async msg", "async-msg")
    .replace("callsc", "call-sc")
    .replace("executesc", "execute-sc")
    .replace("mrc20 ", "MRC-20 ");
}

/** Turn a TransferValue into a human-readable amount + unit chip.
 *  `fmtMas` already appends " MAS"; we only tack on an extra hint for
 *  non-default value kinds so the unit isn't duplicated. */
function ValueCell({ value }: { value: TransferValue }) {
  switch (value.kind) {
    case "coins":
      return <span className="font-mono">{fmtMas(value.nmas)}</span>;
    case "rolls":
      return (
        <span className="font-mono">
          {value.count.toLocaleString()}{" "}
          <span className="text-slate-500 text-xs">rolls</span>
        </span>
      );
    case "deferred_credits":
      return (
        <span className="font-mono">
          {fmtMas(value.nmas)}{" "}
          <span className="text-slate-500 text-xs">(deferred)</span>
        </span>
      );
    case "token":
      return (
        <span className="font-mono">
          {fmtToken(value.raw, value.decimals, value.symbol)}
        </span>
      );
    case "unknown":
      return <span className="text-slate-500">—</span>;
  }
}

/**
 * Parse a denunciation-index string emitted by the indexer, e.g.
 *   "endorsement@(4477200,15)#3"
 *   "block_header@(4477200,15)"
 * into its `(period, thread)` slot, so we can link to the offending slot.
 * Returns `null` if the shape doesn't match.
 */
function parseDenunciationIndex(s: string): { period: number; thread: number } | null {
  const m = s.match(/@\((\d+),(\d+)\)/);
  if (!m) return null;
  const period = Number(m[1]);
  const thread = Number(m[2]);
  if (!Number.isFinite(period) || !Number.isFinite(thread)) return null;
  return { period, thread };
}

/**
 * Every transfer has an upstream — either a user-driven cause (operation,
 * async message, deferred call, slash) or a slot-level implicit cause
 * (block reward, endorsement rewards, deferred credit). We render a
 * clickable link for every row so there is no dead-end "implicit" label:
 * if nothing more specific is known, we fall back to the enclosing block
 * (and, failing that, the slot). The origin tag underneath tells the user
 * WHY the transfer happened; the link above tells them WHERE to look for
 * context.
 */
function SourceCell({ t }: { t: StoredTransfer }) {
  const originLabel = formatOrigin(t.origin);
  let link: React.ReactNode;

  if (t.operation_id) {
    link = (
      <span className="text-xs">
        op <OpLink id={t.operation_id} />
      </span>
    );
  } else if (t.async_msg_id) {
    const label = shortId(t.async_msg_id, 6, 4);
    link = (
      <span className="text-xs" title={`async message ${t.async_msg_id}`}>
        async-msg{" "}
        {t.block_id ? (
          <Link to={`/block/${t.block_id}`} className="font-mono">
            {label}
          </Link>
        ) : (
          <span className="font-mono text-slate-500">{label}</span>
        )}
      </span>
    );
  } else if (t.deferred_call_id) {
    const label = shortId(t.deferred_call_id, 6, 4);
    link = (
      <span className="text-xs" title={`deferred call ${t.deferred_call_id}`}>
        deferred{" "}
        {t.block_id ? (
          <Link to={`/block/${t.block_id}`} className="font-mono">
            {label}
          </Link>
        ) : (
          <span className="font-mono text-slate-500">{label}</span>
        )}
      </span>
    );
  } else if (t.denunciation_index) {
    // The node's DenunciationIndex bundles the denounced slot plus an
    // endorsement index (or nothing, for block-header denunciations).
    // We don't have the canonical denunciation hash on this row so we
    // link to the slot — the /slot page lists every denunciation that
    // landed in that slot's block. Fallback: show the raw index string.
    const slot = parseDenunciationIndex(t.denunciation_index);
    link = (
      <span className="text-xs" title={t.denunciation_index}>
        slash{" "}
        {slot ? (
          <Link
            to={`/slot/${slot.period}/${slot.thread}`}
            className="font-mono"
          >
            ({slot.period}, {slot.thread})
          </Link>
        ) : (
          <span className="font-mono text-slate-500">
            {t.denunciation_index}
          </span>
        )}
      </span>
    );
  } else if (t.block_id) {
    // Block reward, endorsement reward, endorsed reward, deferred credit
    // release, etc. All of these are injected by the execution engine at
    // slot boundaries and therefore live inside exactly one block — the
    // one identified by `block_id`. Linking there lands the user on the
    // block page where the endorsements list is visible.
    const label = t.origin.kind === "block_reward" ? "block" : "in block";
    link = (
      <span className="text-xs">
        {label} <BlockLink id={t.block_id} />
      </span>
    );
  } else {
    // No block id either — last-resort link to the slot.
    link = (
      <span className="text-xs">
        slot{" "}
        <Link
          to={`/slot/${t.slot.period}/${t.slot.thread}`}
          className="font-mono"
        >
          ({t.slot.period}, {t.slot.thread})
        </Link>
      </span>
    );
  }

  return (
    <div className="leading-tight">
      <div>{link}</div>
      <div className="text-[10px] uppercase tracking-wide text-slate-500">
        {originLabel}
      </div>
    </div>
  );
}

function AssetCell({ value }: { value: TransferValue }) {
  if (value.kind === "token") {
    return (
      <div className="leading-tight max-w-[16rem]">
        <Link
          to={`/address/${value.contract}`}
          className="font-mono text-xs"
          title={value.contract}
        >
          {value.symbol || "token"}
        </Link>
        {value.name ? (
          <div className="text-[10px] text-slate-500 truncate" title={value.name}>
            {value.name}
          </div>
        ) : null}
      </div>
    );
  }
  if (value.kind === "rolls") {
    return <span className="text-xs text-slate-500">rolls</span>;
  }
  return <span className="text-xs text-slate-500">MAS</span>;
}

function AddrCell({ addr }: { addr: string | null }) {
  if (!addr) return <span className="text-slate-500">—</span>;
  return <AddrLink addr={addr} />;
}

interface Props {
  transfers: StoredTransfer[];
  /** If true, the `From` / `To` columns will bold the `highlight` address
   *  (useful on the address detail page). */
  highlight?: string;
  emptyLabel?: string;
  /** Whether to display the slot column (hide on per-slot or per-block views
   *  where every row shares the same slot). */
  showSlot?: boolean;
  /** Whether to display the block column (hide on per-block views). */
  showBlock?: boolean;
}

export function TransfersTable({
  transfers,
  highlight,
  emptyLabel,
  showSlot = true,
  showBlock = true,
}: Props) {
  if (transfers.length === 0) {
    return (
      <p className="text-sm text-slate-500">
        {emptyLabel ?? "No transfers recorded."}
      </p>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-xs uppercase text-slate-500">
            {showSlot && <th className="py-1.5 pr-3">Slot</th>}
            {showBlock && <th className="py-1.5 pr-3">Block</th>}
            <th className="py-1.5 pr-3">From</th>
            <th className="py-1.5 pr-3">To</th>
            <th className="py-1.5 pr-3">Asset</th>
            <th className="py-1.5 pr-3 text-right">Value</th>
            <th className="py-1.5 pr-3">Source</th>
          </tr>
        </thead>
        <tbody>
          {transfers.map((t, i) => {
            const fromHit =
              highlight && t.from && t.from === highlight ? "font-semibold" : "";
            const toHit =
              highlight && t.to && t.to === highlight ? "font-semibold" : "";
            return (
              <tr
                key={`${t.slot.period}-${t.slot.thread}-${t.index_in_slot}-${t.id || i}`}
                className="border-t border-border align-top"
                title={t.id ? `id ${t.id}` : undefined}
              >
                {showSlot && (
                  <td className="py-1.5 pr-3 font-mono text-xs">
                    <Link to={`/slot/${t.slot.period}/${t.slot.thread}`}>
                      ({t.slot.period}, {t.slot.thread})
                    </Link>
                  </td>
                )}
                {showBlock && (
                  <td className="py-1.5 pr-3 font-mono text-xs">
                    {t.block_id ? (
                      <Link to={`/block/${t.block_id}`} title={t.block_id}>
                        {t.block_id.slice(0, 8)}…
                      </Link>
                    ) : (
                      <span className="text-slate-500">—</span>
                    )}
                  </td>
                )}
                <td className={`py-1.5 pr-3 ${fromHit}`}>
                  <AddrCell addr={t.from} />
                </td>
                <td className={`py-1.5 pr-3 ${toHit}`}>
                  <AddrCell addr={t.to} />
                </td>
                <td className="py-1.5 pr-3">
                  <AssetCell value={t.value} />
                </td>
                <td className="py-1.5 pr-3 text-right">
                  <ValueCell value={t.value} />
                </td>
                <td className="py-1.5 pr-3">
                  <SourceCell t={t} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
