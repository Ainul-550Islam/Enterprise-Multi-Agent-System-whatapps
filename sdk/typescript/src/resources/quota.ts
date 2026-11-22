import type { MasClient, MasResult } from "../index.js";

/** Quota and usage read operations. */
export class Quota {
  constructor(private readonly client: MasClient) {}

  quotas(): Promise<MasResult<unknown>> {
    return this.client.request("GET", "/v1/quotas");
  }

  usage(): Promise<MasResult<unknown>> {
    return this.client.request("GET", "/v1/usage");
  }

  summary(): Promise<MasResult<unknown>> {
    return this.client.request("GET", "/v1/usage/summary");
  }
}
