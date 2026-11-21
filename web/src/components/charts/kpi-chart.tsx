"use client";

import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export interface KpiPoint {
  label: string;
  value: number;
}

export function KpiChart({ points, color = "var(--accent)" }: { points: readonly KpiPoint[]; color?: string }) {
  if (points.length === 0) {
    return (
      <div className="mas-muted py-8 text-center" role="status">
        No series for this window.
      </div>
    );
  }
  return (
    <div className="h-56 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={[...points]}>
          <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
          <XAxis dataKey="label" stroke="var(--muted)" fontSize={11} />
          <YAxis stroke="var(--muted)" fontSize={11} />
          <Tooltip
            contentStyle={{ background: "var(--panel)", border: "1px solid var(--border)" }}
          />
          <Area type="monotone" dataKey="value" stroke={color} fill={color} fillOpacity={0.2} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
