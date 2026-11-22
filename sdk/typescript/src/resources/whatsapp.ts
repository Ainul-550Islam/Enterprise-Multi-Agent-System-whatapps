import type { MasClient, MasResult } from "../index.js";

/** WhatsApp channel, inbox and template operations. */
export class WhatsApp {
  constructor(private readonly client: MasClient) {}

  registerChannel(body: unknown): Promise<MasResult<unknown>> {
    return this.client.request("POST", "/v1/whatsapp/channels", body);
  }

  channels(): Promise<MasResult<unknown>> {
    return this.client.request("GET", "/v1/whatsapp/channels");
  }

  sendText(channelId: string, to: string, body: string): Promise<MasResult<unknown>> {
    return this.client.request("POST", `/v1/whatsapp/channels/${encodeURIComponent(channelId)}/messages`, {
      to,
      type: "text",
      body,
    });
  }

  templates(): Promise<MasResult<unknown>> {
    return this.client.request("GET", "/v1/whatsapp/templates");
  }

  submitTemplate(body: unknown): Promise<MasResult<unknown>> {
    return this.client.request("POST", "/v1/whatsapp/templates", body);
  }

  stats(): Promise<MasResult<unknown>> {
    return this.client.request("GET", "/v1/whatsapp/stats");
  }
}
