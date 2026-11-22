"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { ErrorState } from "@/components/ui/error-state";
import { useMasMutation } from "@/lib/api/useMas";

interface AssistantExecution {
  execution_id: string;
}

/** Assistant messages are executions; the SSE stream follows execution_id. */
export default function AssistantPage() {
  const [prompt, setPrompt] = useState("");
  const [result, setResult] = useState<AssistantExecution | null>(null);
  const mutation = useMasMutation<AssistantExecution>("/v1/assistant/messages");

  async function send() {
    if (!prompt.trim()) return;
    const created = await mutation.mutate("POST", "/v1/assistant/messages", { message: prompt });
    if (created) {
      setResult(created);
      setPrompt("");
    }
  }

  return (
    <div>
      <h1>Platform assistant</h1>
      {mutation.error ? <ErrorState error={mutation.error} /> : null}
      <Card title="Ask the assistant" loading={false}>
        <textarea aria-label="Message" value={prompt} onChange={(event) => setPrompt(event.target.value)} />
        <Button disabled={!prompt.trim() || mutation.pending} onClick={() => void send()}>Send</Button>
        {result ? (
          <p>
            Execution <code>{result.execution_id}</code> accepted. Follow it in{" "}
            <a href={`/executions/${encodeURIComponent(result.execution_id)}`}>execution details</a>.
          </p>
        ) : null}
      </Card>
    </div>
  );
}
