/**
 * mas-sdk SSE — a dependency-free parser for the platform event stream.
 *
 * The API's `/v1/stream` speaks standard `text/event-stream`; this module
 * turns a byte stream into typed frames and enforces the two properties
 * callers rely on: frames are delivered whole (buffered until the blank-line
 * separator), and a `retry:` hint is surfaced rather than swallowed.
 *
 * `openEventStream` can reconnect. Reconnect is **off** unless
 * `reconnect: true` is passed, because a clean end of a finite mock stream
 * would otherwise loop. When enabled, the next request carries `Last-Event-ID`
 * from the last frame that had an `id:`, and the delay is the frame's
 * `retry:` hint (or `backoffMs`, doubling up to 30s).
 */

/** One decoded SSE frame. */
export interface EventFrame {
  /** `event:` field, defaulting to `message`. */
  event: string;
  /** Joined `data:` lines. */
  data: string;
  /** `id:` field, when present. */
  id?: string;
  /** `retry:` hint in milliseconds, when present. */
  retryMs?: number;
}

/** Incremental SSE decoder: feed chunks, receive whole frames. */
export class EventStreamParser {
  private buffer = "";

  /** Feeds one chunk and returns every frame it completed. */
  feed(chunk: string): EventFrame[] {
    this.buffer += chunk;
    const frames: EventFrame[] = [];
    let boundary = this.buffer.indexOf("\n\n");
    while (boundary !== -1) {
      const raw = this.buffer.slice(0, boundary);
      this.buffer = this.buffer.slice(boundary + 2);
      const frame = parseFrame(raw);
      if (frame) {
        frames.push(frame);
      }
      boundary = this.buffer.indexOf("\n\n");
    }
    return frames;
  }
}

function parseFrame(raw: string): EventFrame | undefined {
  let event = "message";
  let id: string | undefined;
  let retryMs: number | undefined;
  const data: string[] = [];
  for (const line of raw.split("\n")) {
    if (line.startsWith("event:")) {
      event = line.slice(6).trim();
    } else if (line.startsWith("data:")) {
      data.push(line.slice(5).replace(/^ /, ""));
    } else if (line.startsWith("id:")) {
      id = line.slice(3).trim();
    } else if (line.startsWith("retry:")) {
      const value = Number.parseInt(line.slice(6).trim(), 10);
      retryMs = Number.isFinite(value) ? value : undefined;
    }
  }
  if (!event && data.length === 0) {
    return undefined;
  }
  return { event, data: data.join("\n"), id, retryMs };
}

const MAX_BACKOFF_MS = 30_000;

/**
 * Opens the platform event stream over an injected fetch and invokes
 * `onFrame` per completed frame. Returns a stop function.
 */
export function openEventStream(options: {
  url: string;
  token: string;
  fetchImpl?: typeof fetch;
  onFrame: (frame: EventFrame) => void;
  onError?: (error: unknown) => void;
  lastEventId?: string;
  reconnect?: boolean;
  backoffMs?: number;
}): () => void {
  const fetchImpl = options.fetchImpl ?? fetch;
  const controller = new AbortController();
  let lastEventId = options.lastEventId;
  let delay = options.backoffMs ?? 500;
  const reconnect = options.reconnect === true;

  const pump = async (): Promise<void> => {
    while (!controller.signal.aborted) {
      try {
        const headers: Record<string, string> = {
          authorization: `Bearer ${options.token}`,
          accept: "text/event-stream",
        };
        if (lastEventId) {
          headers["last-event-id"] = lastEventId;
        }
        const response = await fetchImpl(options.url, {
          headers,
          signal: controller.signal,
        });
        if (!response.ok || !response.body) {
          throw new Error(`stream refused: HTTP ${response.status}`);
        }
        const parser = new EventStreamParser();
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }
          for (const frame of parser.feed(decoder.decode(value, { stream: true }))) {
            if (frame.id) {
              lastEventId = frame.id;
            }
            if (typeof frame.retryMs === "number" && frame.retryMs > 0) {
              delay = frame.retryMs;
            }
            options.onFrame(frame);
          }
        }
        if (!reconnect) {
          return;
        }
      } catch (error) {
        if (controller.signal.aborted) {
          return;
        }
        options.onError?.(error);
        if (!reconnect) {
          return;
        }
      }
      await sleep(Math.min(delay, MAX_BACKOFF_MS), controller.signal);
      delay = Math.min(delay * 2, MAX_BACKOFF_MS);
    }
  };

  void pump();
  return () => controller.abort();
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}
