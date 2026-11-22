import type { MasClient, MasResult } from "../index.js";

/** Billing checkout, portal and evidence operations. */
export class Billing {
  constructor(private readonly client: MasClient) {}

  checkout(body: unknown): Promise<MasResult<unknown>> {
    return this.client.request("POST", "/v1/billing/checkout", body);
  }

  portalSession(body: unknown): Promise<MasResult<unknown>> {
    return this.client.request("POST", "/v1/billing/portal-session", body);
  }

  subscription(): Promise<MasResult<unknown>> {
    return this.client.request("GET", "/v1/billing/subscription");
  }

  entitlements(): Promise<MasResult<unknown>> {
    return this.client.request("GET", "/v1/billing/entitlements");
  }

  receipts(): Promise<MasResult<unknown>> {
    return this.client.request("GET", "/v1/billing/receipts");
  }
}
