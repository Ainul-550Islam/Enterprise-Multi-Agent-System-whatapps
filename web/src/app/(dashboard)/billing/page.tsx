"use client";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/table";
import { ErrorState } from "@/components/ui/error-state";
import { JsonViewer } from "@/components/ui/json-viewer";
import { api } from "@/lib/api/client";
import { useMas } from "@/lib/api/useMas";

interface Subscription {
  plan_code?: string;
  status?: string;
}

interface Entitlement extends Record<string, unknown> {
  feature: string;
  enabled?: boolean;
}

interface Receipt extends Record<string, unknown> {
  id: string;
  provider?: string;
  amount?: number;
}

interface UsageTotals {
  totals?: Record<string, number>;
}

export default function BillingPage() {
  const subscription = useMas<Subscription>("/v1/billing/subscription");
  const entitlements = useMas<Entitlement[]>("/v1/billing/entitlements");
  const receipts = useMas<Receipt[]>("/v1/billing/receipts");
  const usage = useMas<UsageTotals>("/v1/billing/usage-totals");

  async function checkout() {
    const session = await api.post<{ url?: string }>("/v1/billing/checkout", {});
    if (session.url) {
      window.location.href = session.url;
    }
  }

  async function portal() {
    const session = await api.post<{ url?: string }>("/v1/billing/portal-session", {});
    if (session.url) {
      window.location.href = session.url;
    }
  }

  return (
    <div>
      <h1>Billing</h1>
      {subscription.error ? <ErrorState error={subscription.error} /> : null}
      <Card title="Plan" loading={subscription.loading}>
        <p>
          {subscription.data?.plan_code ?? "no plan"} · {subscription.data?.status ?? "unknown"}
        </p>
        <div className="flex gap-2">
          <Button onClick={() => void checkout()}>Upgrade / checkout</Button>
          <Button tone="ghost" onClick={() => void portal()}>
            Customer portal
          </Button>
        </div>
      </Card>
      <Card title="Entitlements" loading={entitlements.loading}>
        <DataTable
          columns={[
            { key: "feature", header: "Feature" },
            { key: "enabled", header: "Enabled" },
          ]}
          rows={entitlements.data ?? []}
          emptyLabel="No entitlements returned."
          rowKey={(row) => String(row.feature)}
        />
      </Card>
      <Card title="Usage totals" loading={usage.loading}>
        {usage.data ? <JsonViewer value={usage.data} /> : <p className="mas-muted">No usage totals.</p>}
      </Card>
      <Card title="Receipts" loading={receipts.loading}>
        <DataTable
          columns={[
            { key: "id", header: "Receipt" },
            { key: "provider", header: "Provider" },
            { key: "amount", header: "Amount" },
          ]}
          rows={receipts.data ?? []}
          emptyLabel="No receipts."
          rowKey={(row) => String(row.id)}
        />
      </Card>
    </div>
  );
}
