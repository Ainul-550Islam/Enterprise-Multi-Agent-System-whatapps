/**
 * Same-origin SSE client for `/api/mas/v1/stream`.
 *
 * Reconnects with `Last-Event-ID`. Caps concurrent connections per tab so a
 * leaked effect cannot open an unbounded number of streams.
 */

export interface StreamFrame {
  event: string;
  data: string;
  id?: string;
}

export interface SubscribeOptions {
  url?: string;
  onFrame: (frame: StreamFrame) => void;
  onError?: (error: unknown) => void;
  lastEventId?: string;
}

const MAX_CONNECTIONS_PER_TAB = 1;
let activeConnections = 0;

export function activeSseConnections(): number {
  return activeConnections;
}

export function parseSseChunk(raw: string): StreamFrame | undefined {
  let event = "message";
  let id: string | undefined;
  const data: string[] = [];
  for (const line of raw.split("\n")) {
    if (line.startsWith("event:")) {
      event = line.slice(6).trim();
    } else if (line.startsWith("data:")) {
      data.push(line.slice(5).replace(/^ /, ""));
    } else if (line.startsWith("id:")) {
      id = line.slice(3).trim();
    }
  }
  if (data.length === 0 && !id) {
    return undefined;
  }
  return { event, data: data.join("\n"), id };
}

/** Opens the platform event stream through the BFF. Returns unsubscribe. */
export function subscribeStream(options: SubscribeOptions): () => void {
  if (activeConnections >= MAX_CONNECTIONS_PER_TAB) {
    const error = new Error(`sse connection cap reached (${MAX_CONNECTIONS_PER_TAB} per tab)`);
    options.onError?.(error);
    return () => undefined;
  }
  const controller = new AbortController();
  activeConnections += 1;
  let lastEventId = options.lastEventId;
  const url = options.url ?? "/api/mas/v1/stream";

  const run = async (): Promise<void> => {
    const headers: Record<string, string> = { accept: "text/event-stream" };
    if (lastEventId) {
      headers["last-event-id"] = lastEventId;
    }
    const response = await fetch(url, {
      method: "GET",
      credentials: "same-origin",
      signal: controller.signal,
      headers,
    });
    if (!response.ok || !response.body) {
      throw new Error(`stream refused: HTTP ${response.status}`);
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      let boundary = buffer.indexOf("\n\n");
      while (boundary !== -1) {
        const frame = parseSseChunk(buffer.slice(0, boundary));
        buffer = buffer.slice(boundary + 2);
        if (frame) {
          if (frame.id) {
            lastEventId = frame.id;
          }
          options.onFrame(frame);
        }
        boundary = buffer.indexOf("\n\n");
      }
    }
  };

  const loop = async (): Promise<void> => {
    let delay = 500;
    while (!controller.signal.aborted) {
      try {
        await run();
        if (controller.signal.aborted) {
          return;
        }
        delay = 500;
      } catch (error) {
        if (controller.signal.aborted) {
          return;
        }
        options.onError?.(error);
        await new Promise((resolve) => setTimeout(resolve, delay));
        delay = Math.min(delay * 2, 8_000);
      }
    }
  };

  void loop();
  return () => {
    if (!controller.signal.aborted) {
      controller.abort();
      activeConnections = Math.max(0, activeConnections - 1);
    }
  };
}

export const SSE_MAX_CONNECTIONS_PER_TAB = MAX_CONNECTIONS_PER_TAB;
