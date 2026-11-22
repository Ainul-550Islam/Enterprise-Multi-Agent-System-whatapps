import type { MasClient, MasResult } from "../index.js";

/** Workflow lifecycle operations (/v1/workflows). */
export class Workflows {
  constructor(private readonly client: MasClient) {}

  list(limit = 50, cursor?: string): Promise<MasResult<unknown>> {
    const query = `?limit=${limit}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`;
    return this.client.request("GET", `/v1/workflows${query}`);
  }

  get(workflowId: string): Promise<MasResult<unknown>> {
    return this.client.request("GET", `/v1/workflows/${encodeURIComponent(workflowId)}`);
  }

  create(body: unknown): Promise<MasResult<unknown>> {
    return this.client.request("POST", "/v1/workflows", body);
  }

  putGraph(workflowId: string, graph: unknown): Promise<MasResult<unknown>> {
    return this.client.request("PUT", `/v1/workflows/${encodeURIComponent(workflowId)}/graph`, graph);
  }

  publish(workflowId: string): Promise<MasResult<unknown>> {
    return this.client.request("POST", `/v1/workflows/${encodeURIComponent(workflowId)}/publish`);
  }

  disable(workflowId: string): Promise<MasResult<unknown>> {
    return this.client.request("POST", `/v1/workflows/${encodeURIComponent(workflowId)}/disable`);
  }
}
