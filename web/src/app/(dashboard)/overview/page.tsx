"use client";

import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading";
import { KpiChart } from "@/components/charts/kpi-chart";
import { useMas } from "@/lib/api/useMas";

interface KpiCard {
  value: number | null;
  delta_percent: number | null;
  sparkline: number[];
  unit: string;
}

interface Overview {
  from: string;
  to: string;
  total_agents: KpiCard;
  tasks_completed: KpiCard;
  success_rate: KpiCard;
  time_saved_minutes: KpiCard;
}

interface Performance {
  granularity: string;
  points: Array<{ bucket: string; tasks_completed: number; success_rate: number | null }>;
}

interface TasksByAgent {
  slices: Array<{ agent_id: string | null; label: string; tasks_completed: number }>;
  total_completed: number;
}

interface UsageCost {
  total_cost: number | null;
  currency: string | null;
  tokens_input: number;
  tokens_output: number;
  tool_invocations: number;
}

function formatKpi(card: KpiCard | undefined): string {
  if (!card || card.value === null || card.value === undefined) {
    return "—";
  }
  if (card.unit === "percent") {
    return `${card.value.toFixed(1)}%`;
  }
  return card.value.toLocaleString();
}

function Kpi({ title, card }: { title: string; card?: KpiCard }) {
  const delta = card?.delta_percent;
  return (
    <Card title={title}>
      <p className="m-0 text-3xl font-semibold">{formatKpi(card)}</p>
      <p className="mas-muted m-0 mt-1 text-sm">
        {delta === null || delta === undefined ? "no prior window" : `${delta > 0 ? "+" : ""}${delta.toFixed(1)}% vs prior window`}
        {card?.unit ? ` · ${card.unit}` : ""}
      </p>
    </Card>
  );
}

export default function OverviewPage() {
  const overview = useMas<Overview>("/v1/analytics/overview", { granularity: "day" });
  const performance = useMas<Performance>("/v1/analytics/performance", { granularity: "day" });
  const byAgent = useMas<TasksByAgent>("/v1/analytics/tasks-by-agent", { granularity: "day" });
  const cost = useMas<UsageCost>("/v1/analytics/usage-cost", { granularity: "day" });

  return (
    <div>
      <h1 className="mt-0">Overview</h1>
      {overview.error ? <ErrorState error={overview.error} /> : null}
      {overview.loading ? <LoadingState label="Loading analytics…" /> : null}
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <Kpi title="Agents" card={overview.data?.total_agents} />
        <Kpi title="Tasks completed" card={overview.data?.tasks_completed} />
        <Kpi title="Success rate" card={overview.data?.success_rate} />
        <Kpi title="Time saved" card={overview.data?.time_saved_minutes} />
      </div>
      <Card title="Performance" loading={performance.loading}>
        {performance.error ? <ErrorState error={performance.error} /> : null}
        <KpiChart
          points={(performance.data?.points ?? []).map((point) => ({
            label: point.bucket.slice(0, 10),
            value: point.tasks_completed,
          }))}
        />
      </Card>
      <div className="grid gap-4 lg:grid-cols-2">
        <Card title="Tasks by agent" loading={byAgent.loading}>
          {byAgent.error ? <ErrorState error={byAgent.error} /> : null}
          {(byAgent.data?.slices ?? []).length === 0 ? (
            <p className="mas-muted">No completed tasks in this window.</p>
          ) : (
            <ul className="m-0 list-none p-0">
              {(byAgent.data?.slices ?? []).map((slice) => (
                <li key={`${slice.agent_id ?? "other"}-${slice.label}`} className="flex justify-between border-b border-line py-2">
                  <span>{slice.label}</span>
                  <Badge>{String(slice.tasks_completed)}</Badge>
                </li>
              ))}
            </ul>
          )}
        </Card>
        <Card title="Usage and cost" loading={cost.loading}>
          {cost.error ? <ErrorState error={cost.error} /> : null}
          {cost.data ? (
            <dl className="grid grid-cols-2 gap-3">
              <div>
                <dt className="mas-muted">Cost</dt>
                <dd className="m-0 text-xl">
                  {cost.data.total_cost === null
                    ? "unknown (mixed or missing pricing)"
                    : `${cost.data.total_cost.toFixed(4)} ${cost.data.currency ?? ""}`}
                </dd>
              </div>
              <div>
                <dt className="mas-muted">Tool invocations</dt>
                <dd className="m-0 text-xl">{cost.data.tool_invocations}</dd>
              </div>
              <div>
                <dt className="mas-muted">Tokens in</dt>
                <dd className="m-0 text-xl">{cost.data.tokens_input}</dd>
              </div>
              <div>
                <dt className="mas-muted">Tokens out</dt>
                <dd className="m-0 text-xl">{cost.data.tokens_output}</dd>
              </div>
            </dl>
          ) : null}
        </Card>
      </div>
    </div>
  );
}
