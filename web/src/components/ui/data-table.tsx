"use client";

import { useMemo, useState, type ReactNode } from "react";

import { Button } from "@/components/ui/button";

export interface SortableColumn<T> {
  key: string;
  header: string;
  value?: (row: T) => string | number;
  render?: (row: T) => ReactNode;
}

/**
 * Sortable table with keyset-cursor pagination. The server supplies opaque
 * cursors; this component never computes offsets or guesses page numbers.
 */
export function DataTableWithCursor<T extends Record<string, unknown>>({
  columns,
  rows,
  rowKey,
  nextCursor,
  onNext,
  loading,
  emptyLabel = "No records.",
}: {
  columns: SortableColumn<T>[];
  rows: T[];
  rowKey: (row: T) => string;
  nextCursor?: string | null;
  onNext?: (cursor: string) => void;
  loading?: boolean;
  emptyLabel?: string;
}) {
  const [sort, setSort] = useState<{ key: string; asc: boolean } | null>(null);
  const sorted = useMemo(() => {
    if (!sort) return rows;
    const column = columns.find((item) => item.key === sort.key);
    if (!column) return rows;
    const value = column.value ?? ((row: T) => String(row[column.key] ?? ""));
    return [...rows].sort((a, b) => {
      const left = value(a);
      const right = value(b);
      const result = left < right ? -1 : left > right ? 1 : 0;
      return sort.asc ? result : -result;
    });
  }, [rows, columns, sort]);

  return (
    <div>
      <table className="mas-table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column.key}>
                <button
                  type="button"
                  onClick={() => setSort((current) => ({ key: column.key, asc: current?.key === column.key ? !current.asc : true }))}
                >
                  {column.header}
                </button>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.length === 0 ? (
            <tr><td colSpan={columns.length}>{emptyLabel}</td></tr>
          ) : (
            sorted.map((row) => (
              <tr key={rowKey(row)}>
                {columns.map((column) => (
                  <td key={column.key}>
                    {column.render
                      ? column.render(row)
                      : column.value
                        ? String(column.value(row))
                        : String((row as Record<string, unknown>)[column.key] ?? "")}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
      {nextCursor && onNext ? (
        <Button tone="ghost" onClick={() => onNext(nextCursor)} disabled={loading}>Next page</Button>
      ) : null}
    </div>
  );
}
