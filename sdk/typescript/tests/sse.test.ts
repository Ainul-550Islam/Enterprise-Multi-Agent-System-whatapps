import assert from "node:assert/strict";
import { test } from "node:test";

import { EventStreamParser, openEventStream } from "../src/sse.js";

test("parses event frames across chunk boundaries", () => {
  const parser = new EventStreamParser();
  const first = parser.feed("event: execution.update\nid: 1\ndata: {\"st");
  assert.equal(first.length, 0, "partial frame must not be emitted");
  const second = parser.feed("atus\":\"running\"}\n\n");
  assert.equal(second.length, 1);
  assert.equal(second[0].event, "execution.update");
  assert.equal(second[0].id, "1");
  assert.equal(second[0].data, '{"status":"running"}');
});

test("parses multi-line data and retry hints", () => {
  const parser = new EventStreamParser();
  const frames = parser.feed("retry: 2500\nevent: note\ndata: line one\ndata: line two\n\n");
  assert.equal(frames.length, 1);
  assert.equal(frames[0].retryMs, 2500);
  assert.equal(frames[0].data, "line one\nline two");
});

test("openEventStream delivers frames and stops cleanly", async () => {
  const encoder = new TextEncoder();
  const chunks = [encoder.encode("event: hello\ndata: one\n\n"), encoder.encode("data: two\n\n")];
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  });
  const fetchImpl = (async () =>
    new Response(stream, { status: 200, headers: { "content-type": "text/event-stream" } })) as unknown as typeof fetch;

  const frames: string[] = [];
  await new Promise<void>((resolve, reject) => {
    openEventStream({
      url: "http://api.test/v1/stream",
      token: "tok",
      fetchImpl,
      onFrame: (frame) => {
        frames.push(`${frame.event}:${frame.data}`);
        if (frames.length === 2) {
          resolve();
        }
      },
      onError: reject,
    });
  });
  assert.deepEqual(frames, ["hello:one", "message:two"]);
});

test("openEventStream surfaces stream refusals", async () => {
  const fetchImpl = (async () => new Response("nope", { status: 401 })) as unknown as typeof fetch;
  const error = await new Promise<unknown>((resolve) => {
    openEventStream({
      url: "http://api.test/v1/stream",
      token: "tok",
      fetchImpl,
      onFrame: () => undefined,
      onError: resolve,
    });
  });
  assert.match(String(error), /stream refused: HTTP 401/);
});

test("reconnects with Last-Event-ID after a drop", async () => {
  const encoder = new TextEncoder();
  let calls = 0;
  const seen: Array<string | undefined> = [];
  const fetchImpl = (async (_url: unknown, init: unknown) => {
    calls += 1;
    const headers = (init as { headers: Record<string, string> }).headers;
    seen.push(headers["last-event-id"]);
    const body =
      calls === 1
        ? encoder.encode("id: 42\nevent: tick\ndata: first\n\n")
        : encoder.encode("id: 43\nevent: tick\ndata: second\n\n");
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(body);
        controller.close();
      },
    });
    return new Response(stream, { status: 200, headers: { "content-type": "text/event-stream" } });
  }) as unknown as typeof fetch;

  const frames: string[] = [];
  const stop = await new Promise<() => void>((resolve, reject) => {
    const stopFn = openEventStream({
      url: "http://api.test/v1/stream",
      token: "tok",
      fetchImpl,
      reconnect: true,
      backoffMs: 5,
      onFrame: (frame) => {
        frames.push(`${frame.id}:${frame.data}`);
        if (frames.length >= 2) {
          resolve(stopFn);
        }
      },
      onError: reject,
    });
  });
  stop();
  assert.deepEqual(frames.slice(0, 2), ["42:first", "43:second"]);
  assert.equal(seen[0], undefined);
  assert.equal(seen[1], "42");
  assert.ok(calls >= 2);
});
