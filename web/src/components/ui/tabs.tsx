"use client";

import { useState, type ReactNode } from "react";

export interface TabItem {
  id: string;
  label: string;
  content: ReactNode;
}

/** Minimal accessible tab strip with one active panel. */
export function Tabs({ items, initial }: { items: TabItem[]; initial?: string }) {
  const [active, setActive] = useState(initial ?? items[0]?.id ?? "");
  return (
    <div>
      <div role="tablist" className="flex gap-2 border-b border-line">
        {items.map((item) => (
          <button
            key={item.id}
            type="button"
            role="tab"
            aria-selected={active === item.id}
            className="px-3 py-2 text-sm"
            onClick={() => setActive(item.id)}
          >
            {item.label}
          </button>
        ))}
      </div>
      {items.find((item) => item.id === active)?.content ?? null}
    </div>
  );
}
