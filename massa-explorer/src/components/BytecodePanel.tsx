import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import {
  analyzeWasm,
  sha256Hex,
  type WasmAnalysis,
} from "../lib/wasm";
import { ErrorMsg, KV, Loading, Panel } from "./Bits";

/**
 * Inline bytecode panel for smart-contract addresses (AS…).
 *
 * Fetches the raw WASM via the indexer's `/v1/addresses/:addr/bytecode`
 * (which proxies the node's QueryState(AddressBytecodeFinal) — see
 * spec.md §5.2) and runs a full client-side analysis: section table,
 * imports / exports, memory + table types, globals, data segments,
 * and a heuristic dump of UTF-8 strings hidden in the data segments.
 *
 * The download button hands a blob URL to the browser; we never make
 * a second network call for the "save as" so the user can rely on
 * exactly the bytes they just analysed.
 */
export function BytecodePanel({ addr }: { addr: string }) {
  const { client, network } = useAppState();

  const q = useQuery({
    queryKey: ["addr-bytecode", network, addr],
    queryFn: () => api.addressBytecode(client, addr),
    enabled: !!addr,
    staleTime: 60_000,
  });

  const bytes = q.data?.bytes ?? null;
  // The synchronous analysis is cheap — it's allowed to re-run on every
  // render that follows a refetch; `useMemo` keyed on the buffer
  // identity keeps it cached across tab toggles.
  const analysis: WasmAnalysis | null = useMemo(
    () => (bytes ? analyzeWasm(bytes) : null),
    [bytes],
  );

  // SHA-256 fingerprint computed off the critical path so initial paint
  // shows the size & section table immediately. Stored in the analysis
  // object as a string we mutate-then-render (React just re-renders on
  // the state change).
  const [sha, setSha] = useState<string>("");
  useEffect(() => {
    setSha("");
    if (!bytes) return;
    let cancelled = false;
    sha256Hex(bytes).then((h) => {
      if (!cancelled) setSha(h);
    });
    return () => {
      cancelled = true;
    };
  }, [bytes]);

  // Build the download URL lazily. We revoke it on unmount so we don't
  // leak memory on a long-running tab.
  const blobUrl = useMemo(() => {
    if (!bytes) return null;
    return URL.createObjectURL(
      new Blob([bytes], { type: "application/wasm" }),
    );
  }, [bytes]);
  useEffect(
    () => () => {
      if (blobUrl) URL.revokeObjectURL(blobUrl);
    },
    [blobUrl],
  );

  if (q.isLoading) return <Panel title="Bytecode"><Loading /></Panel>;

  // 404 ⇒ data === null from `getBytes`. Surface as "no bytecode"
  // rather than an error.
  if (!bytes) {
    return (
      <Panel title="Bytecode">
        {q.isError ? (
          <ErrorMsg err={q.error} />
        ) : (
          <div className="text-muted text-sm">
            No bytecode found at this address. Either the contract was never
            deployed, the node has it but it's not yet final, or the node we
            queried is not exposing the bytecode entry.
          </div>
        )}
      </Panel>
    );
  }

  const filename = `${addr}.wasm`;

  return (
    <>
      <Panel
        title="Bytecode"
        action={
          <a
            className="btn text-sm"
            href={blobUrl ?? "#"}
            download={filename}
            title="Download the raw on-chain bytecode as a .wasm file."
          >
            Download .wasm
          </a>
        }
      >
        <dl className="kv">
          <KV label="Size">
            <span className="font-mono">
              {formatBytes(bytes.length)}{" "}
              <span className="text-muted text-xs">
                ({bytes.length.toLocaleString()} bytes)
              </span>
            </span>
          </KV>
          <KV label="SHA-256">
            <span className="font-mono text-xs break-all">
              {sha || <span className="text-muted">computing…</span>}
            </span>
          </KV>
          {analysis && (
            <>
              <KV label="WASM version">
                <span className="font-mono">
                  {analysis.validMagic ? analysis.version : "—"}
                </span>
                {!analysis.validMagic && (
                  <span className="text-muted text-xs ml-2">
                    (header is not a valid WebAssembly module)
                  </span>
                )}
              </KV>
              <KV label="Sections">
                <span className="text-sm">
                  {analysis.sections.length} sections,{" "}
                  {analysis.types.length} type signatures,{" "}
                  {analysis.funcCount.imported + analysis.funcCount.declared}{" "}
                  functions{" "}
                  <span className="text-muted">
                    ({analysis.funcCount.imported} imported,{" "}
                    {analysis.funcCount.declared} defined)
                  </span>
                  , {analysis.exports.length} exports
                </span>
              </KV>
              {analysis.startFunction !== null && (
                <KV label="Start function">
                  <span className="font-mono">
                    #{analysis.startFunction}
                    {analysis.names.functions[analysis.startFunction] && (
                      <span className="text-muted">
                        {" "}
                        ({analysis.names.functions[analysis.startFunction]})
                      </span>
                    )}
                  </span>
                </KV>
              )}
              {analysis.names.module && (
                <KV label="Module name">
                  <span className="font-mono">{analysis.names.module}</span>
                </KV>
              )}
            </>
          )}
        </dl>

        {analysis && analysis.warnings.length > 0 && (
          <div className="mt-3 text-xs text-amber-400 border border-amber-700/40 bg-amber-900/10 rounded px-2 py-1">
            <div className="font-semibold mb-1">Parse warnings:</div>
            <ul className="list-disc pl-4 space-y-0.5">
              {analysis.warnings.slice(0, 5).map((w, i) => (
                <li key={i}>{w}</li>
              ))}
              {analysis.warnings.length > 5 && (
                <li className="text-muted">
                  …and {analysis.warnings.length - 5} more
                </li>
              )}
            </ul>
          </div>
        )}
      </Panel>

      <div className="h-3" />

      {analysis && analysis.validMagic && (
        <>
          <SectionTable analysis={analysis} />
          <div className="h-3" />
          <ImportsPanel analysis={analysis} />
          <div className="h-3" />
          <ExportsPanel analysis={analysis} />
          <div className="h-3" />
          <MemoryGlobalsPanel analysis={analysis} />
          <div className="h-3" />
          <DataStringsPanel analysis={analysis} />
        </>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Sub-panels
// ---------------------------------------------------------------------------

function SectionTable({ analysis }: { analysis: WasmAnalysis }) {
  return (
    <Panel title="Section table">
      <div className="overflow-x-auto -mx-3 sm:mx-0">
        <table className="w-full text-sm min-w-[420px]">
          <thead className="text-muted text-xs uppercase">
            <tr>
              <th className="text-left py-1 px-2">#</th>
              <th className="text-left py-1 px-2">Id</th>
              <th className="text-left py-1 px-2">Section</th>
              <th className="text-right py-1 px-2">Size</th>
            </tr>
          </thead>
          <tbody>
            {analysis.sections.map((s, i) => (
              <tr key={i} className="border-t border-border">
                <td className="py-1 px-2 text-muted">{i}</td>
                <td className="px-2 font-mono">{s.id}</td>
                <td className="px-2">{s.name}</td>
                <td className="px-2 text-right font-mono whitespace-nowrap">
                  {formatBytes(s.size)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

function ImportsPanel({ analysis }: { analysis: WasmAnalysis }) {
  if (analysis.imports.length === 0) {
    return (
      <Panel title="Imports">
        <div className="text-muted text-sm">No imports.</div>
      </Panel>
    );
  }
  return (
    <Panel title={`Imports (${analysis.imports.length})`}>
      <div className="overflow-x-auto -mx-3 sm:mx-0">
        <table className="w-full text-sm min-w-[520px]">
          <thead className="text-muted text-xs uppercase">
            <tr>
              <th className="text-left py-1 px-2">Module</th>
              <th className="text-left py-1 px-2">Name</th>
              <th className="text-left py-1 px-2">Kind</th>
              <th className="text-left py-1 px-2">Signature / type</th>
            </tr>
          </thead>
          <tbody>
            {analysis.imports.map((imp, i) => (
              <tr key={i} className="border-t border-border align-top">
                <td className="py-1 px-2 font-mono break-all">{imp.module}</td>
                <td className="px-2 font-mono break-all">{imp.name}</td>
                <td className="px-2">{kindBadge(imp.kind)}</td>
                <td className="px-2 font-mono text-xs">
                  {imp.kind === "func" && imp.typeIndex !== null
                    ? formatSig(analysis.types[imp.typeIndex])
                    : imp.kind === "global" && imp.globalType
                      ? `${imp.globalType.mutable ? "mut " : ""}${imp.globalType.valtype}`
                      : imp.kind === "memory" && imp.memoryType
                        ? formatLimits(imp.memoryType, "pages")
                        : imp.kind === "table" && imp.tableType
                          ? `${imp.tableType.reftype} ${formatLimits(imp.tableType.limits, "entries")}`
                          : ""}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

function ExportsPanel({ analysis }: { analysis: WasmAnalysis }) {
  if (analysis.exports.length === 0) {
    return (
      <Panel title="Exports">
        <div className="text-muted text-sm">No exports.</div>
      </Panel>
    );
  }
  // Build a name lookup for func indices for nicer rendering.
  const funcNames = analysis.names.functions;
  return (
    <Panel title={`Exports (${analysis.exports.length})`}>
      <div className="overflow-x-auto -mx-3 sm:mx-0">
        <table className="w-full text-sm min-w-[480px]">
          <thead className="text-muted text-xs uppercase">
            <tr>
              <th className="text-left py-1 px-2">Name</th>
              <th className="text-left py-1 px-2">Kind</th>
              <th className="text-left py-1 px-2">Index</th>
              <th className="text-left py-1 px-2">Signature / type</th>
            </tr>
          </thead>
          <tbody>
            {analysis.exports.map((e, i) => {
              // For `func` exports, recover the type signature via the
              // `function` section's [imported funcs + declared funcs]
              // numbering, falling back to "?" when we can't resolve.
              let sig = "";
              if (e.kind === "func") {
                const importedFuncs = analysis.imports.filter(
                  (im) => im.kind === "func",
                );
                if (e.index < importedFuncs.length) {
                  const ti = importedFuncs[e.index].typeIndex;
                  if (ti !== null) sig = formatSig(analysis.types[ti]);
                }
                // (Declared funcs' type indices live in section 3 which we
                // don't fully parse — that lookup table requires walking
                // the function section, which we deliberately skipped to
                // keep parser size down. Documented limitation.)
              } else if (e.kind === "global" && analysis.globals[e.index]) {
                const g = analysis.globals[e.index];
                sig = `${g.mutable ? "mut " : ""}${g.valtype}`;
              } else if (e.kind === "memory" && analysis.memories[e.index]) {
                sig = formatLimits(analysis.memories[e.index], "pages");
              } else if (e.kind === "table" && analysis.tables[e.index]) {
                const t = analysis.tables[e.index];
                sig = `${t.reftype} ${formatLimits(t.limits, "entries")}`;
              }
              return (
                <tr key={i} className="border-t border-border align-top">
                  <td className="py-1 px-2 font-mono break-all">{e.name}</td>
                  <td className="px-2">{kindBadge(e.kind)}</td>
                  <td className="px-2 font-mono">
                    #{e.index}
                    {e.kind === "func" && funcNames[e.index] && (
                      <span className="text-muted"> ({funcNames[e.index]})</span>
                    )}
                  </td>
                  <td className="px-2 font-mono text-xs">{sig}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

function MemoryGlobalsPanel({ analysis }: { analysis: WasmAnalysis }) {
  return (
    <Panel title="Memory, tables & globals">
      <dl className="kv">
        <KV label="Memories">
          {analysis.memories.length === 0 ? (
            <span className="text-muted text-sm">
              none declared (may be imported)
            </span>
          ) : (
            <ul className="text-sm space-y-1">
              {analysis.memories.map((m, i) => (
                <li key={i} className="font-mono">
                  #{i}: {formatLimits(m, "pages")}{" "}
                  <span className="text-muted text-xs">
                    (1 page = 64 KiB; min ={" "}
                    {formatBytes(m.min * 65536)})
                  </span>
                </li>
              ))}
            </ul>
          )}
        </KV>
        <KV label="Tables">
          {analysis.tables.length === 0 ? (
            <span className="text-muted text-sm">none declared</span>
          ) : (
            <ul className="text-sm space-y-1">
              {analysis.tables.map((t, i) => (
                <li key={i} className="font-mono">
                  #{i}: {t.reftype} {formatLimits(t.limits, "entries")}
                </li>
              ))}
            </ul>
          )}
        </KV>
        <KV label="Globals">
          {analysis.globals.length === 0 ? (
            <span className="text-muted text-sm">none declared</span>
          ) : (
            <ul className="text-sm font-mono space-y-1">
              {analysis.globals.map((g, i) => (
                <li key={i}>
                  #{i}: {g.mutable ? "mut " : ""}
                  {g.valtype}
                  {analysis.names.globals[i] && (
                    <span className="text-muted"> ({analysis.names.globals[i]})</span>
                  )}
                </li>
              ))}
            </ul>
          )}
        </KV>
        <KV label="Data segments">
          <span className="text-sm">
            {analysis.data.segments} segment
            {analysis.data.segments === 1 ? "" : "s"} —{" "}
            <span className="font-mono">
              {formatBytes(analysis.data.totalBytes)}
            </span>{" "}
            <span className="text-muted text-xs">
              ({analysis.data.totalBytes.toLocaleString()} bytes)
            </span>
          </span>
        </KV>
      </dl>
    </Panel>
  );
}

function DataStringsPanel({ analysis }: { analysis: WasmAnalysis }) {
  const [showAll, setShowAll] = useState(false);
  if (analysis.data.strings.length === 0) {
    return (
      <Panel title="Data strings">
        <div className="text-muted text-sm">
          No printable strings of 4+ bytes found in any data segment.
        </div>
      </Panel>
    );
  }
  const shown = showAll
    ? analysis.data.strings
    : analysis.data.strings.slice(0, 40);
  return (
    <Panel
      title={`Data strings (${analysis.data.strings.length})`}
      action={
        analysis.data.strings.length > 40 ? (
          <button
            className="btn text-xs"
            onClick={() => setShowAll((s) => !s)}
          >
            {showAll ? "show less" : `show all (${analysis.data.strings.length})`}
          </button>
        ) : null
      }
    >
      <div className="text-xs text-muted mb-2">
        UTF-8 strings recovered from data segments via a printable-run
        heuristic (≥ 4 chars). Useful for spotting embedded ABI keys,
        log templates, datastore prefixes, etc.
      </div>
      <ul className="space-y-1 max-h-[28rem] overflow-y-auto -mx-3 sm:mx-0">
        {shown.map((s, i) => (
          <li
            key={i}
            className="font-mono text-xs px-3 sm:px-0 break-all"
          >
            <span className="text-muted">[{s.segment}]</span>{" "}
            <span>{s.value}</span>
          </li>
        ))}
      </ul>
    </Panel>
  );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function kindBadge(kind: string) {
  return (
    <span className="inline-block px-1.5 py-0.5 rounded-full bg-panel border border-border text-[10px] uppercase tracking-wide text-muted">
      {kind}
    </span>
  );
}

function formatSig(t: { params: string[]; results: string[] } | undefined) {
  if (!t) return "?";
  return `(${t.params.join(", ")}) -> (${t.results.join(", ")})`;
}

function formatLimits(
  l: { min: number; max: number | null },
  unit: string,
): string {
  return l.max !== null
    ? `min=${l.min} max=${l.max} ${unit}`
    : `min=${l.min} ${unit}`;
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / 1024 / 1024).toFixed(2)} MiB`;
}
