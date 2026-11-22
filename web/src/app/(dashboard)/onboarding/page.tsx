"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { ErrorState } from "@/components/ui/error-state";
import { Select } from "@/components/ui/select";
import { api } from "@/lib/api/client";
import { StableApiError } from "@/lib/api/types";

const STEPS = ["Provider", "Channel", "Review", "Go live"] as const;

/** Guided readiness check: queries real config endpoints; never invents data. */
export default function OnboardingPage() {
  const [step, setStep] = useState(0);
  const [provider, setProvider] = useState("openai");
  const [channel, setChannel] = useState("whatsapp");
  const [ack, setAck] = useState(false);
  const [error, setError] = useState<StableApiError | null>(null);
  const [ready, setReady] = useState(false);

  async function check() {
    setError(null);
    try {
      if (channel === "whatsapp") {
        await api.get("/v1/whatsapp/channels");
      }
      await api.get("/v1/policies");
      setReady(true);
    } catch (caught: unknown) {
      setError(caught instanceof StableApiError ? caught : new StableApiError({ code: "CLIENT", message: String(caught) }));
    }
  }

  return (
    <div>
      <h1>Onboarding</h1>
      {error ? <ErrorState error={error} /> : null}
      <Card title={`Step ${step + 1} of ${STEPS.length}: ${STEPS[step]}`} loading={false}>
        {step === 0 ? <Select label="Provider" value={provider} options={["openai", "anthropic", "gemini", "azure"].map((value) => ({ value, label: value }))} onChange={(event) => setProvider(event.target.value)} /> : null}
        {step === 1 ? <Select label="Channel" value={channel} options={["whatsapp", "web"].map((value) => ({ value, label: value }))} onChange={(event) => setChannel(event.target.value)} /> : null}
        {step === 2 ? (
          <div>
            <p>Provider: {provider}. Channel: {channel}.</p>
            <Checkbox label="I understand credentials are supplied by operators and never pasted into this wizard" checked={ack} onCheckedChange={setAck} />
          </div>
        ) : null}
        {step === 3 ? (
          <div>
            <p>{ready ? "Configuration endpoints responded. Finish setup in Settings and the provider guides." : "Run the readiness check to confirm the selected channel and policies are reachable."}</p>
            <Button onClick={() => void check()}>Run readiness check</Button>
          </div>
        ) : null}
        <div className="flex gap-2">
          {step > 0 ? <Button tone="ghost" onClick={() => setStep(step - 1)}>Back</Button> : null}
          {step < STEPS.length - 1 ? (
            <Button disabled={step === 2 && !ack} onClick={() => setStep(step + 1)}>Next</Button>
          ) : null}
        </div>
      </Card>
    </div>
  );
}
