import { http, HttpResponse } from "msw";

function envelope(data: unknown) {
  return {
    data,
    error: null,
    meta: {
      request_id: "msw",
      correlation_id: "msw",
      api_version: "v1",
      responded_at: "2026-11-15T00:00:00Z",
    },
  };
}

/** WhatsApp + billing mocks used until those planes are exercised live. */
export const handlers = [
  http.get("/api/mas/v1/billing/subscription", () =>
    HttpResponse.json(envelope({ plan_code: "pro", status: "active" })),
  ),
  http.get("/api/mas/v1/whatsapp/stats", () =>
    HttpResponse.json(envelope({ open_conversations: 3, unread: 1 })),
  ),
];
