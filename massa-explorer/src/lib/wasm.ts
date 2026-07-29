// Self-contained Massa/SC WASM analyzer.
//
// The explorer fetches the raw bytecode of a smart-contract address via
// `/v1/addresses/:addr/bytecode` (a thin proxy over the node's
// `QueryState(AddressBytecodeFinal)`). To keep the page strictly
// client-side — no extra round-trips, no server-side disassembly — we
// parse the WebAssembly binary right here.
//
// We deliberately avoid pulling a heavyweight dependency (`wasmparser`
// is 200+ kB minified). The official WASM binary spec is small enough
// that a careful hand-rolled walker over the section table fits in a
// few hundred lines and returns everything we want to surface:
//
//   * magic + version
//   * counts and byte sizes of every section (well-known + custom)
//   * type table (function signatures)
//   * imports (module + name + kind + signature index)
//   * exports (name + kind + index)
//   * function count (declared / imported)
//   * memory and table type(s) with limits
//   * globals (value type, mutability, declared count)
//   * start function (if any)
//   * data-segment count and total size, plus extracted UTF-8 strings
//   * names from the custom "name" section (module, function, etc.)
//
// We never execute the bytecode; the analysis is read-only.
//
// The parser is tolerant: if any section is corrupt we record an error
// and keep walking from the next section's offset. Truncated files
// surface as `truncated: true` so the UI can warn instead of crashing.

export type WasmValType =
  | "i32"
  | "i64"
  | "f32"
  | "f64"
  | "v128"
  | "funcref"
  | "externref"
  | `??(0x${string})`;

export interface WasmFuncSignature {
  params: WasmValType[];
  results: WasmValType[];
}

export type WasmImportKind = "func" | "table" | "memory" | "global";
export interface WasmImport {
  module: string;
  name: string;
  kind: WasmImportKind;
  /** For `func` imports, an index into the type table; otherwise null. */
  typeIndex: number | null;
  /** For `global` imports, the declared value type. */
  globalType: { valtype: WasmValType; mutable: boolean } | null;
  /** For `table` imports, the reference type and limits. */
  tableType: { reftype: WasmValType; limits: WasmLimits } | null;
  /** For `memory` imports, the page limits. */
  memoryType: WasmLimits | null;
}

export type WasmExportKind = "func" | "table" | "memory" | "global";
export interface WasmExport {
  name: string;
  kind: WasmExportKind;
  index: number;
}

export interface WasmLimits {
  min: number;
  max: number | null;
}

export interface WasmGlobal {
  valtype: WasmValType;
  mutable: boolean;
}

export interface WasmSectionStat {
  /** Numeric section id (0=custom, 1=type, …). */
  id: number;
  /** Human name (or `custom:<name>` when known). */
  name: string;
  /** Section body size, in bytes (excluding the id + length header). */
  size: number;
}

export interface WasmDataString {
  /** UTF-8 string that survived the printable-run filter. */
  value: string;
  /** Index of the data segment the string came from. */
  segment: number;
}

export interface WasmAnalysis {
  ok: boolean;
  /** True if the binary was a valid `\0asm 0x01000000` module. */
  validMagic: boolean;
  /** WebAssembly module version (`1` for MVP, `2` for future revs). */
  version: number;
  /** Total file size in bytes. */
  byteSize: number;
  /** SHA-256 of the whole binary, hex-encoded. */
  sha256: string;
  /** Listed in walk order so the UI can show "section table" verbatim. */
  sections: WasmSectionStat[];
  types: WasmFuncSignature[];
  imports: WasmImport[];
  exports: WasmExport[];
  /** Total `func` count = imported funcs + functions declared in section 3. */
  funcCount: { imported: number; declared: number };
  memories: WasmLimits[];
  tables: { reftype: WasmValType; limits: WasmLimits }[];
  globals: WasmGlobal[];
  /** `start` section: the function index that runs at instantiation, if any. */
  startFunction: number | null;
  /** Aggregate over every data segment. */
  data: {
    segments: number;
    totalBytes: number;
    /** Up to N readable UTF-8 strings extracted from the data section. */
    strings: WasmDataString[];
  };
  /** Names from the optional "name" custom section. Keys are indices. */
  names: {
    module: string | null;
    functions: Record<number, string>;
    globals: Record<number, string>;
    types: Record<number, string>;
  };
  /** Non-fatal warnings collected while walking. */
  warnings: string[];
  /** True if we ran out of input before the section table was exhausted. */
  truncated: boolean;
}

/**
 * Decode a complete WASM module. Always returns an object — failures are
 * surfaced via `ok`, `warnings`, and `truncated`.
 */
export function analyzeWasm(bytes: Uint8Array): WasmAnalysis {
  const out: WasmAnalysis = {
    ok: false,
    validMagic: false,
    version: 0,
    byteSize: bytes.length,
    sha256: "",
    sections: [],
    types: [],
    imports: [],
    exports: [],
    funcCount: { imported: 0, declared: 0 },
    memories: [],
    tables: [],
    globals: [],
    startFunction: null,
    data: { segments: 0, totalBytes: 0, strings: [] },
    names: { module: null, functions: {}, globals: {}, types: {} },
    warnings: [],
    truncated: false,
  };

  const r = new Reader(bytes);
  // Magic + version. We tolerate truncated headers because some
  // contracts on testnets ship raw concatenations; the explorer's
  // "size & sha256" panel stays useful in that case.
  if (bytes.length < 8) {
    out.warnings.push("file too short to contain a WASM header");
    return out;
  }
  const magic = String.fromCharCode(bytes[0], bytes[1], bytes[2], bytes[3]);
  if (magic !== "\0asm") {
    out.warnings.push(`bad magic: 0x${hex4(bytes.slice(0, 4))}`);
    return out;
  }
  out.validMagic = true;
  out.version =
    bytes[4] | (bytes[5] << 8) | (bytes[6] << 16) | (bytes[7] << 24);
  r.pos = 8;

  // Walk sections.
  while (r.pos < r.len) {
    const sectionStart = r.pos;
    let id: number;
    try {
      id = r.u8();
    } catch {
      break; // EOF — graceful exit
    }
    let size: number;
    try {
      size = r.leb_u32();
    } catch {
      out.warnings.push(
        `section ${id} at offset ${sectionStart}: missing length`,
      );
      out.truncated = true;
      break;
    }
    const bodyStart = r.pos;
    const bodyEnd = bodyStart + size;
    if (bodyEnd > r.len) {
      out.warnings.push(
        `section ${id} (size ${size}) overflows file by ${bodyEnd - r.len} bytes`,
      );
      out.truncated = true;
      r.pos = r.len;
      break;
    }
    const name = sectionName(id);
    const stat: WasmSectionStat = { id, name, size };
    out.sections.push(stat);
    try {
      decodeSection(id, new Reader(bytes.subarray(bodyStart, bodyEnd)), out, stat);
    } catch (e) {
      out.warnings.push(
        `section ${name} (id=${id}): parse error — ${(e as Error).message}`,
      );
    }
    r.pos = bodyEnd;
  }

  out.ok = out.validMagic && out.warnings.length === 0;
  // Hash for fingerprinting / sharing. Crypto.subtle is fine in any
  // remotely modern browser. We do this asynchronously after the rest
  // so the synchronous report is available immediately.
  return out;
}

/** Async SHA-256 over a byte buffer; resolves to a lowercase hex string. */
export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const ab = bytes.buffer.slice(
    bytes.byteOffset,
    bytes.byteOffset + bytes.byteLength,
  );
  const dig = await crypto.subtle.digest("SHA-256", ab);
  return Array.from(new Uint8Array(dig))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// ---------------------------------------------------------------------------
// internals
// ---------------------------------------------------------------------------

function sectionName(id: number): string {
  switch (id) {
    case 0:  return "custom";
    case 1:  return "type";
    case 2:  return "import";
    case 3:  return "function";
    case 4:  return "table";
    case 5:  return "memory";
    case 6:  return "global";
    case 7:  return "export";
    case 8:  return "start";
    case 9:  return "element";
    case 10: return "code";
    case 11: return "data";
    case 12: return "data count";
    default: return `unknown(${id})`;
  }
}

function decodeSection(
  id: number,
  r: Reader,
  out: WasmAnalysis,
  stat: WasmSectionStat,
): void {
  switch (id) {
    case 0:
      decodeCustom(r, out, stat);
      break;
    case 1:
      decodeTypes(r, out);
      break;
    case 2:
      decodeImports(r, out);
      break;
    case 3:
      out.funcCount.declared = r.leb_u32();
      // skip the type indices themselves; we only need the count.
      break;
    case 4:
      decodeTables(r, out);
      break;
    case 5:
      decodeMemories(r, out);
      break;
    case 6:
      decodeGlobals(r, out);
      break;
    case 7:
      decodeExports(r, out);
      break;
    case 8:
      out.startFunction = r.leb_u32();
      break;
    case 11:
      decodeData(r, out);
      break;
    default:
      // sections we don't crack open contribute via their `size` stat
      // already recorded by the caller.
      break;
  }
}

function decodeTypes(r: Reader, out: WasmAnalysis): void {
  const n = r.leb_u32();
  for (let i = 0; i < n; i++) {
    const tag = r.u8();
    if (tag !== 0x60) {
      throw new Error(`type #${i}: expected 0x60 (func), got 0x${tag.toString(16)}`);
    }
    const params = readValTypeVec(r);
    const results = readValTypeVec(r);
    out.types.push({ params, results });
  }
}

function decodeImports(r: Reader, out: WasmAnalysis): void {
  const n = r.leb_u32();
  for (let i = 0; i < n; i++) {
    const module = r.utf8();
    const name = r.utf8();
    const kindByte = r.u8();
    let imp: WasmImport;
    switch (kindByte) {
      case 0x00:
        imp = {
          module,
          name,
          kind: "func",
          typeIndex: r.leb_u32(),
          globalType: null,
          tableType: null,
          memoryType: null,
        };
        out.funcCount.imported++;
        break;
      case 0x01: {
        const reftype = readValType(r);
        const limits = readLimits(r);
        imp = {
          module,
          name,
          kind: "table",
          typeIndex: null,
          globalType: null,
          tableType: { reftype, limits },
          memoryType: null,
        };
        break;
      }
      case 0x02:
        imp = {
          module,
          name,
          kind: "memory",
          typeIndex: null,
          globalType: null,
          tableType: null,
          memoryType: readLimits(r),
        };
        break;
      case 0x03: {
        const valtype = readValType(r);
        const mutable = r.u8() === 1;
        imp = {
          module,
          name,
          kind: "global",
          typeIndex: null,
          globalType: { valtype, mutable },
          tableType: null,
          memoryType: null,
        };
        break;
      }
      default:
        throw new Error(
          `import #${i}: unknown kind 0x${kindByte.toString(16)}`,
        );
    }
    out.imports.push(imp);
  }
}

function decodeTables(r: Reader, out: WasmAnalysis): void {
  const n = r.leb_u32();
  for (let i = 0; i < n; i++) {
    const reftype = readValType(r);
    const limits = readLimits(r);
    out.tables.push({ reftype, limits });
  }
}

function decodeMemories(r: Reader, out: WasmAnalysis): void {
  const n = r.leb_u32();
  for (let i = 0; i < n; i++) {
    out.memories.push(readLimits(r));
  }
}

function decodeGlobals(r: Reader, out: WasmAnalysis): void {
  const n = r.leb_u32();
  for (let i = 0; i < n; i++) {
    const valtype = readValType(r);
    const mutable = r.u8() === 1;
    skipInitExpr(r);
    out.globals.push({ valtype, mutable });
  }
}

function decodeExports(r: Reader, out: WasmAnalysis): void {
  const n = r.leb_u32();
  for (let i = 0; i < n; i++) {
    const name = r.utf8();
    const kindByte = r.u8();
    const index = r.leb_u32();
    let kind: WasmExportKind;
    switch (kindByte) {
      case 0x00: kind = "func";   break;
      case 0x01: kind = "table";  break;
      case 0x02: kind = "memory"; break;
      case 0x03: kind = "global"; break;
      default:
        throw new Error(
          `export #${i} '${name}': unknown kind 0x${kindByte.toString(16)}`,
        );
    }
    out.exports.push({ name, kind, index });
  }
}

const MAX_DATA_STRINGS = 256;

function decodeData(r: Reader, out: WasmAnalysis): void {
  const n = r.leb_u32();
  out.data.segments = n;
  for (let i = 0; i < n; i++) {
    const flags = r.leb_u32();
    // 0 = active mem=0, 1 = passive, 2 = active explicit memidx.
    if (flags === 0x02) r.leb_u32();
    if (flags !== 0x01) skipInitExpr(r);
    const len = r.leb_u32();
    const seg = r.bytes(len);
    out.data.totalBytes += len;
    if (out.data.strings.length < MAX_DATA_STRINGS) {
      extractStrings(seg, i, out.data.strings);
    }
  }
}

/**
 * Scan a data-segment payload for runs of 4+ printable ASCII characters
 * and append them to `acc`. Trims runs to a sensible max-length so a
 * single embedded asset can't blow up the report.
 */
function extractStrings(
  seg: Uint8Array,
  segIdx: number,
  acc: WasmDataString[],
): void {
  const MIN_RUN = 4;
  const MAX_LEN = 256;
  let runStart = -1;
  for (let i = 0; i <= seg.length; i++) {
    const b = i < seg.length ? seg[i] : 0;
    // Printable ASCII + common whitespace. Multi-byte UTF-8 is treated
    // as non-printable here on purpose; embedded English strings are
    // by far the most useful for SC analysis.
    const printable = (b >= 0x20 && b < 0x7f) || b === 0x09;
    if (printable) {
      if (runStart < 0) runStart = i;
    } else {
      if (runStart >= 0 && i - runStart >= MIN_RUN) {
        const slice = seg.subarray(runStart, Math.min(i, runStart + MAX_LEN));
        const s = new TextDecoder("utf-8", { fatal: false }).decode(slice);
        acc.push({ value: s, segment: segIdx });
        if (acc.length >= MAX_DATA_STRINGS) return;
      }
      runStart = -1;
    }
  }
}

function decodeCustom(
  r: Reader,
  out: WasmAnalysis,
  stat: WasmSectionStat,
): void {
  const name = r.utf8();
  stat.name = `custom:${name}`;
  if (name === "name") {
    decodeNameSection(r, out);
  }
  // Other customs (producers, sourceMappingURL, …) are reported in the
  // section table but not unpacked in detail.
}

function decodeNameSection(r: Reader, out: WasmAnalysis): void {
  // Subsections: id (u8) + size (u32) + payload.
  while (r.pos < r.len) {
    const sub = r.u8();
    const size = r.leb_u32();
    const subEnd = r.pos + size;
    if (subEnd > r.len) {
      out.warnings.push(`name subsection ${sub}: overflows section`);
      return;
    }
    const subReader = new Reader(r.bytes(size));
    try {
      switch (sub) {
        case 0:
          out.names.module = subReader.utf8();
          break;
        case 1:
          readNameMap(subReader, out.names.functions);
          break;
        case 4:
          readNameMap(subReader, out.names.types);
          break;
        case 7:
          readNameMap(subReader, out.names.globals);
          break;
        default:
          break;
      }
    } catch (e) {
      out.warnings.push(
        `name subsection ${sub}: parse error — ${(e as Error).message}`,
      );
    }
    r.pos = subEnd;
  }
}

function readNameMap(r: Reader, into: Record<number, string>): void {
  const n = r.leb_u32();
  for (let i = 0; i < n; i++) {
    const idx = r.leb_u32();
    into[idx] = r.utf8();
  }
}

function readValTypeVec(r: Reader): WasmValType[] {
  const n = r.leb_u32();
  const out: WasmValType[] = [];
  for (let i = 0; i < n; i++) out.push(readValType(r));
  return out;
}

function readValType(r: Reader): WasmValType {
  const b = r.u8();
  switch (b) {
    case 0x7f: return "i32";
    case 0x7e: return "i64";
    case 0x7d: return "f32";
    case 0x7c: return "f64";
    case 0x7b: return "v128";
    case 0x70: return "funcref";
    case 0x6f: return "externref";
    default:   return `??(0x${b.toString(16).padStart(2, "0")})`;
  }
}

function readLimits(r: Reader): WasmLimits {
  const flag = r.u8();
  const min = r.leb_u32();
  const max = flag === 1 ? r.leb_u32() : null;
  return { min, max };
}

/**
 * Skip a constant init-expr — sequence of bytes ending in 0x0B (`end`).
 * We don't reconstruct the value; the SC use case rarely needs it and
 * tracking opcode arities here would balloon this file.
 */
function skipInitExpr(r: Reader): void {
  while (r.pos < r.len) {
    const op = r.u8();
    if (op === 0x0b) return;
    // Opcodes with one immediate (the common cases for init exprs):
    //   * 0x41 i32.const, 0x42 i64.const → 1 signed LEB
    //   * 0x43 f32.const → 4 bytes raw
    //   * 0x44 f64.const → 8 bytes raw
    //   * 0x23 global.get → 1 unsigned LEB
    //   * 0x6b/0xd0/0xd2 ref.null / ref.func → 1 reftype byte or LEB
    // We handle the common ones; anything exotic gets skipped by
    // continuing the loop until we hit 0x0b. That's good enough for a
    // tolerance parser since malformed init-exprs would have already
    // been caught by the surrounding validators on chain.
    switch (op) {
      case 0x41: r.leb_s32(); break;
      case 0x42: r.leb_s64(); break;
      case 0x43: r.bytes(4);  break;
      case 0x44: r.bytes(8);  break;
      case 0x23: r.leb_u32(); break;
      case 0xd0: r.u8();      break;
      case 0xd2: r.leb_u32(); break;
      default:                break;
    }
  }
}

// ---------------------------------------------------------------------------
// LEB-aware byte reader
// ---------------------------------------------------------------------------

class Reader {
  pos = 0;
  constructor(public readonly buf: Uint8Array) {}
  get len(): number {
    return this.buf.length;
  }
  u8(): number {
    if (this.pos >= this.buf.length) throw new Error("unexpected EOF (u8)");
    return this.buf[this.pos++];
  }
  bytes(n: number): Uint8Array {
    if (this.pos + n > this.buf.length) {
      throw new Error(`unexpected EOF (need ${n}, have ${this.buf.length - this.pos})`);
    }
    const out = this.buf.subarray(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }
  utf8(): string {
    const len = this.leb_u32();
    const buf = this.bytes(len);
    return new TextDecoder("utf-8", { fatal: false }).decode(buf);
  }
  leb_u32(): number {
    let result = 0;
    let shift = 0;
    while (true) {
      const b = this.u8();
      result |= (b & 0x7f) << shift;
      if ((b & 0x80) === 0) return result >>> 0;
      shift += 7;
      if (shift > 35) throw new Error("LEB u32 too long");
    }
  }
  leb_s32(): number {
    let result = 0;
    let shift = 0;
    let b: number;
    do {
      b = this.u8();
      result |= (b & 0x7f) << shift;
      shift += 7;
    } while (b & 0x80);
    if (shift < 32 && b & 0x40) result |= -1 << shift;
    return result;
  }
  leb_s64(): bigint {
    let result = 0n;
    let shift = 0n;
    let b: number;
    do {
      b = this.u8();
      result |= BigInt(b & 0x7f) << shift;
      shift += 7n;
    } while (b & 0x80);
    if (shift < 64n && b & 0x40) result |= -1n << shift;
    return result;
  }
}

function hex4(bs: Uint8Array): string {
  return Array.from(bs.subarray(0, 4))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}
