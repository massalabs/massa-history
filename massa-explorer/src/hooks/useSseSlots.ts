import { useEffect, useRef, useState } from "react";
import type { SlotState } from "../lib/types";
import type { ApiClient } from "../lib/api";

// Subscribes to /v1/stream/slots and keeps the N most recent updates.
// One connection per (client, path). Auto-reconnects via native EventSource.
// v1 TODO: Last-Event-ID replay wiring.

export interface SlotUpdate extends SlotState {
  type: "slot_updated";
}

export function useSseSlots(client: ApiClient, maxItems = 32) {
  const [events, setEvents] = useState<SlotUpdate[]>([]);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const esRef = useRef<EventSource | null>(null);

  useEffect(() => {
    const url = client.sseUrl("/v1/stream/slots");
    if (!url) return;
    const es = new EventSource(url);
    esRef.current = es;
    es.onopen = () => {
      setConnected(true);
      setError(null);
    };
    es.onmessage = (ev) => {
      try {
        const parsed = JSON.parse(ev.data) as SlotUpdate;
        if (parsed && parsed.type === "slot_updated") {
          setEvents((prev) => {
            const next = [parsed, ...prev];
            return next.length > maxItems ? next.slice(0, maxItems) : next;
          });
        }
      } catch {
        // ignore malformed payloads
      }
    };
    es.onerror = () => {
      setConnected(false);
      setError("SSE connection error");
    };
    return () => {
      es.close();
      esRef.current = null;
    };
  }, [client, maxItems]);

  return { events, connected, error };
}
