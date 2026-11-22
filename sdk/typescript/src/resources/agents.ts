import type { MasClient, MasResult } from "../index.js";

/** Agent catalogue operations (/v1/agents). */
export class Agents {
  constructor(private readonly client: MasClient) {}

  list(limit = 50, cursor?: string): Promise<MasResult<unknown>> {
    const query = `?limit=${limit}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`;
    return this.client.request("GET", `/v1/agents${query}`);
  }

  get(agentId: string): Promise<MasResult<unknown>> {
    return this.client.request("GET", `/v1/agents/${encodeURIComponent(agentId)}`);
  }

  create(body: unknown): Promise<MasResult<unknown>> {
    return this.client.request("POST", "/v1/agents", body);
  }

  versions(agentId: string): Promise<MasResult<unknown>> {
    return this.client.request("GET", `/v1/agents/${encodeURIComponent(agentId)}/versions`);
  }

  rollback(agentId: string, version: string): Promise<MasResult<unknown>> {
    return this.client.request(
      "POST",
      `/v1/agents/${encodeURIComponent(agentId)}/versions/${encodeURIComponent(version)}/rollback`,
    );
  }
}
