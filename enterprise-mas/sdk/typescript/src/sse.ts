"""mas SDK TypeScript SSE."""

export function createSseClient(client: ReturnType<typeof createClient>, path: string) {
  // Returns an SSE event source setup
  // In full implementation, returns EventSource or async iterator
  return new EventSource(\`\${path}\`.replace('/api', ''));
}