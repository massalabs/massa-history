import { useQuery } from "@tanstack/react-query";
import { Helmet } from "react-helmet-async";
import { useState } from "react";
import { useAppState } from "../AppState";
import { api } from "../lib/api";
import { ErrorMsg, Loading, Panel } from "../components/Bits";
import { MiniChart } from "../components/MiniChart";

const WINDOWS = [
  { label: "1 hour", secs: 3600, bucket: 60 },
  { label: "24 hours", secs: 86400, bucket: 600 },
  { label: "7 days", secs: 604800, bucket: 3600 },
];

const SERIES: { id: "throughput" | "blocks_per_slot" | "finality_lag" | "active_addresses"; title: string; unit: string; hint: string }[] = [
  {
    id: "throughput",
    title: "Throughput",
    unit: "ops / s",
    hint: "Final operations per second, bucketed.",
  },
  {
    id: "blocks_per_slot",
    title: "Blocks per slot",
    unit: "blocks",
    hint: "Average number of (candidate + final) blocks per slot.",
  },
  {
    id: "finality_lag",
    title: "Finality lag",
    unit: "seconds",
    hint: "Time between the slot's wall-clock ts and when we marked it final.",
  },
  {
    id: "active_addresses",
    title: "Active addresses",
    unit: "unique",
    hint: "Unique op creators observed in the bucket.",
  },
];

export function Charts() {
  const { network } = useAppState();
  const [windowIdx, setWindowIdx] = useState(0);
  const w = WINDOWS[windowIdx];

  return (
    <>
      <Helmet>
        <title>{`Charts — ${network}`}</title>
      </Helmet>
      <Panel
        title="Charts"
        action={
          <select
            aria-label="Time window"
            className="bg-bg border border-border rounded-md text-sm px-2 py-1.5"
            value={windowIdx}
            onChange={(e) => setWindowIdx(Number(e.target.value))}
          >
            {WINDOWS.map((x, i) => (
              <option value={i} key={x.label}>
                {x.label}
              </option>
            ))}
          </select>
        }
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {SERIES.map((s) => (
            <ChartTile
              key={s.id}
              id={s.id}
              title={s.title}
              unit={s.unit}
              hint={s.hint}
              windowSecs={w.secs}
              bucketSecs={w.bucket}
            />
          ))}
        </div>
      </Panel>
    </>
  );
}

function ChartTile({
  id,
  title,
  unit,
  hint,
  windowSecs,
  bucketSecs,
}: {
  id: "throughput" | "blocks_per_slot" | "finality_lag" | "active_addresses";
  title: string;
  unit: string;
  hint: string;
  windowSecs: number;
  bucketSecs: number;
}) {
  const { client, network } = useAppState();
  const q = useQuery({
    queryKey: ["chart", id, network, windowSecs, bucketSecs],
    queryFn: () => api.chart(client, id, { windowSecs, bucketSecs }),
    refetchInterval: 15_000,
  });
  return (
    <div className="rounded-md border border-border p-3 bg-panel">
      <div className="flex justify-between items-baseline mb-1">
        <h3 className="font-semibold text-sm">{title}</h3>
        <span className="text-muted text-xs">{unit}</span>
      </div>
      <p className="text-muted text-xs mb-2">{hint}</p>
      {q.isLoading ? (
        <Loading />
      ) : q.isError ? (
        <ErrorMsg err={q.error} />
      ) : (
        <MiniChart data={q.data?.data ?? []} label={title} />
      )}
    </div>
  );
}
