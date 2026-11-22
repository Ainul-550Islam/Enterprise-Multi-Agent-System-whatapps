import type { MasClient, MasResult } from "../index.js";

/** Execution operations (/v1/executions). */
export class Executions {
  constructor(private readonly client: MasClient) {}

  submit(projectId: string, body: unknown, idempotencyKey?: string): Promise<MasResult<unknown>> {
    return this.client.request(
      "POST",
      `/v1/projects/${encodeURIComponent(projectId)}/executions`,
      body,
      idempotencyKey,
    );
  }

  list(limit = 50, cursor?: string): Promise<MasResult<unknown>> {
    const query = `?limit=${limit}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`;
    return this.client.request("GET", `/v1/executions${query}`);
  }

  get(executionId: string): Promise<MasResult<unknown>> {
    return this.client.request("GET", `/v1/executions/${encodeURIComponent(executionId)}`);
  }

  steps(executionId: string): Promise<MasResult<unknown>> {
    return this.client.request("GET", `/v1/executions/${encodeURIComponent(executionId)}/steps`);
  }

  transition(executionId: string, signal: string): Promise<MasResult<unknown>> {
    return this.client.request(
      "POST",
      `/v1/executions/${encodeURIComponent(executionId)}/${encodeURIComponent(signal)}`,
    );
  }
}
