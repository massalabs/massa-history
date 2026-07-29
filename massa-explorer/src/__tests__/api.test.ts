import { describe, expect, it, vi } from "vitest";
import { ApiError, makeApiClient } from "../lib/api";

function mockFetch(handler: (url: string) => Partial<Response> | Promise<Partial<Response>>) {
  return vi.fn(async (input: RequestInfo | URL) => {
    const url = typeof input === "string" ? input : input.toString();
    const r = await handler(url);
    return {
      ok: r.status ? r.status >= 200 && r.status < 300 : true,
      status: r.status ?? 200,
      json: async () => (r as any).bodyJson ?? {},
      text: async () => (r as any).bodyText ?? "",
      ...r,
    } as Response;
  }) as unknown as typeof fetch;
}

describe("ApiClient endpoint failover", () => {
  it("uses first endpoint when it succeeds", async () => {
    const fetchImpl = mockFetch((url) => {
      expect(url).toBe("http://a/v1/health");
      return { status: 200, bodyJson: { status: "ok" } };
    });
    const c = makeApiClient("mainnet", { endpoints: ["http://a", "http://b"], fetchImpl });
    const body = await c.get<{ status: string }>("/v1/health");
    expect(body.status).toBe("ok");
    expect((fetchImpl as any).mock.calls.length).toBe(1);
  });

  it("falls over on 500", async () => {
    const fetchImpl = mockFetch((url) => {
      if (url.startsWith("http://a/")) return { status: 500, bodyText: "boom" };
      return { status: 200, bodyJson: { status: "ok" } };
    });
    const c = makeApiClient("mainnet", { endpoints: ["http://a", "http://b"], fetchImpl });
    const body = await c.get<{ status: string }>("/v1/health");
    expect(body.status).toBe("ok");
    expect((fetchImpl as any).mock.calls.length).toBe(2);
  });

  it("does NOT fall over on 404 (returns null via getMaybe)", async () => {
    const fetchImpl = mockFetch(() => ({ status: 404, bodyText: "nope" }));
    const c = makeApiClient("mainnet", { endpoints: ["http://a", "http://b"], fetchImpl });
    const body = await c.getMaybe("/v1/blocks/X");
    expect(body).toBeNull();
    expect((fetchImpl as any).mock.calls.length).toBe(1);
  });

  it("throws ApiError when all endpoints fail", async () => {
    const fetchImpl = mockFetch(() => ({ status: 502, bodyText: "x" }));
    const c = makeApiClient("mainnet", { endpoints: ["http://a", "http://b"], fetchImpl });
    await expect(c.get("/v1/status")).rejects.toBeInstanceOf(ApiError);
  });

  it("treats network errors like 5xx (falls over)", async () => {
    const calls: string[] = [];
    const fetchImpl = vi.fn(async (u: RequestInfo | URL) => {
      const url = typeof u === "string" ? u : u.toString();
      calls.push(url);
      if (url.startsWith("http://a/")) throw new Error("ECONNREFUSED");
      return {
        ok: true,
        status: 200,
        json: async () => ({ status: "ok" }),
        text: async () => "",
      } as Response;
    }) as unknown as typeof fetch;
    const c = makeApiClient("mainnet", { endpoints: ["http://a", "http://b"], fetchImpl });
    const body = await c.get<{ status: string }>("/v1/health");
    expect(body.status).toBe("ok");
    expect(calls.length).toBe(2);
  });
});
