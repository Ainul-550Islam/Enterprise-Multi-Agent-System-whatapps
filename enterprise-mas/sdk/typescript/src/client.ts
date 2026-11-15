"""mas SDK TypeScript client."""

export interface ClientConfig {
  baseUrl: string;
  token: string;
  tenant?: string;
  organization?: string;
  correlationId?: string;
}

export function createClient(config: ClientConfig) {
  // Returns a client object with get, post, etc. methods
  // In a full implementation, this would use axios with proper error handling
  return {
    baseUrl: config.baseUrl,
    token: config.token,
    getHeaders: () => ({
      Authorization: `Bearer ${config.token}`,
      ...(config.tenant && { 'X-Tenant-Id': config.tenant }),
      ...(config.organization && { 'X-Organization-Id': config.organization }),
      'Accept': 'application/json',
    }),
  };
}