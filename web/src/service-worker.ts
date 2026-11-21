/**
 * Offline shell for the operations dashboard.
 *
 * Precaches the app shell only (HTML/CSS/JS). API responses are never
 * cached: quota, executions and WhatsApp inbox must not be served stale.
 *
 * This file is a service-worker script (webworker globals). It is excluded
 * from the dashboard `tsconfig.json` `include` so `tsc --noEmit` does not
 * type-check it against DOM lib names.
 */

export const SHELL_CACHE = "mas-shell-v1";

const SHELL_URLS: readonly string[] = ["/", "/login", "/overview"];

declare const self: {
  skipWaiting(): Promise<void>;
  clients: { claim(): Promise<void> };
  addEventListener(type: string, listener: (event: SWEvent) => void): void;
};

interface SWEvent {
  waitUntil(promise: Promise<unknown>): void;
  request?: { method: string; url: string };
  respondWith?(promise: Promise<unknown>): void;
}

self.addEventListener("install", (event: SWEvent) => {
  event.waitUntil(
    caches.open(SHELL_CACHE).then((cache) => cache.addAll([...SHELL_URLS])),
  );
  void self.skipWaiting();
});

self.addEventListener("activate", (event: SWEvent) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter((key) => key !== SHELL_CACHE).map((key) => caches.delete(key)),
      ),
    ),
  );
  void self.clients.claim();
});

self.addEventListener("fetch", (event: SWEvent) => {
  const request = event.request;
  if (!request || request.method !== "GET" || !event.respondWith) {
    return;
  }
  const url = new URL(request.url);
  if (url.pathname.startsWith("/api/")) {
    return;
  }
  event.respondWith(
    caches.match(request as Request).then((cached) => cached ?? fetch(request as Request)),
  );
});
