"use client";

import { Card } from "@/components/ui/card";
import { ErrorState } from "@/components/ui/error-state";
import { JsonViewer } from "@/components/ui/json-viewer";
import { KpiChart } from "@/components/charts/kpi-chart";
import { useMas } from "@/lib/api/useMas";

interface Usage {
  points?: Array<{ bucket?: string; quantity?: number }>;
}

interface Summary {
  totals?: Record<string, number>;
}

export default function UsagePage() {
  const usage = useMas<Usage>("/v1/usage");
  const summary = useMas<Summary>("/v1/usage/summary");
  const points = (usage.data?.points ?? []).map((point) => ({
    label: String(point.bucket ?? "").slice(0, 10),
    value: Number(point.quantity ?? 0),
  }));

  return (
    <div>
      <h1>Usage</h1>
      {usage.error ? <ErrorState error={usage.error} /> : null}
      <Card title="Summary" loading={summary.loading}>
        {summary.data ? <JsonViewer value={summary.data} /> : <p className="mas-muted">No summary.</p>}
      </Card>
      <Card title="Series" loading={usage.loading}>
        <KpiChart points={points} />
      </Card>
    </div>
  );
}
