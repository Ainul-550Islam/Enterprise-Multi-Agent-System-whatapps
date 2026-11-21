"use client";

import type { ReactNode } from "react";

export interface Column<Row> {
  /** Unique column id; also the default cell accessor. */
  key: string;
  header: string;
  /** Optional custom cell renderer; defaults to `row[key]` as a string. */
  render?: (row: Row) => ReactNode;
}

export interface DataTableProps<Row> {
  columns: readonly Column<Row>[];
  rows: readonly Row[];
  /** Rendered inside the table body when there are no rows. */
  emptyLabel?: string;
  rowKey?: (row: Row, index: number) => string;
}

/** Generic data table with stable column keys and an explicit empty state. */
export function DataTable<Row extends Record<string, unknown>>({
  columns,
  rows,
  emptyLabel = "No rows yet.",
  rowKey,
}: DataTableProps<Row>) {
  return (
    <table className="mas-table">
      <thead>
        <tr>
          {columns.map((column) => (
            <th key={column.key} scope="col">
              {column.header}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.length === 0 ? (
          <tr>
            <td colSpan={columns.length} className="mas-muted">
              {emptyLabel}
            </td>
          </tr>
        ) : (
          rows.map((row, index) => (
            <tr key={rowKey?.(row, index) ?? String(index)}>
              {columns.map((column) => (
                <td key={column.key}>
                  {column.render ? column.render(row) : String(row[column.key] ?? "")}
                </td>
              ))}
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}
