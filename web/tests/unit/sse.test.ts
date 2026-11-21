import { describe, expect, it, vi } from "vitest";

import { parseSseChunk, subscribeStream, SSE_MAX_CONNECTIONS_PER_TAB } from "@/lib/sse";

describe("sse parser", () => {
  it("parses event, id and multi-line data", () => {
    const frame = parseSseChunk("event: execution\nid: 42\ndata: {\"status\":\"running\"}\ndata: more");
    expect(frame).toEqual({
      event: "execution",
      id: "42",
      data: "{\"status\":\"running\"}\nmore",
    });
  });
});

describe("subscribeStream cap", () => {
  it("refuses a second connection in the same tab", async () => {
    const body = new ReadableStream({
      start() {
        /* stay open */
      },
    });
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        body,
      }),
    );
    const errors: unknown[] = [];
    const first = subscribeStream({ onFrame: () => undefined, onError: (error) => errors.push(error) });
    const second = subscribeStream({ onFrame: () => undefined, onError: (error) => errors.push(error) });
    expect(SSE_MAX_CONNECTIONS_PER_TAB).toBe(1);
    expect(String(errors[0])).toMatch(/connection cap/);
    first();
    second();
    vi.unstubAllGlobals();
  });
});
