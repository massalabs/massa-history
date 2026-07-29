import type { ChartPoint } from "../lib/types";

/**
 * Minimal inline-SVG line/area chart. Kept intentionally tiny so we don't
 * need to pull in recharts (~120kb gzipped) just to render 5 small time
 * series on the /charts page.
 *
 * Input is a `ChartPoint[]` as returned by `/v1/charts/*`. The chart auto-
 * scales vertically; horizontally it always spans the whole viewbox. We
 * draw a faint baseline + one colored line; no axes, no ticks — the idea
 * is to keep the visual weight below the numeric readout that sits above.
 */
export function MiniChart({
  data,
  height = 60,
  color = "var(--accent, #7c3aed)",
  label,
}: {
  data: ChartPoint[];
  height?: number;
  color?: string;
  label?: string;
}) {
  if (data.length === 0) {
    return <div className="text-muted text-xs">No data.</div>;
  }
  const ys = data.map((d) => d.value);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const span = maxY - minY || 1;
  const w = 600;
  const h = height;
  const pad = 6;
  const stepX = (w - 2 * pad) / Math.max(1, data.length - 1);
  const points = data
    .map(
      (d, i) =>
        `${pad + i * stepX},${pad + (h - 2 * pad) * (1 - (d.value - minY) / span)}`,
    )
    .join(" ");
  const last = data[data.length - 1].value;
  const first = data[0].value;
  return (
    <figure className="w-full">
      {label && (
        <figcaption className="text-xs text-muted flex justify-between">
          <span>{label}</span>
          <span className="font-mono">
            {first.toFixed(2)} → <strong className="text-fg">{last.toFixed(2)}</strong>
          </span>
        </figcaption>
      )}
      <svg
        viewBox={`0 0 ${w} ${h}`}
        preserveAspectRatio="none"
        className="w-full h-auto"
        role="img"
        aria-label={label ?? "chart"}
      >
        <polyline
          fill="none"
          stroke={color}
          strokeWidth={1.5}
          points={points}
        />
      </svg>
    </figure>
  );
}
