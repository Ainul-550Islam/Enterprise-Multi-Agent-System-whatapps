// @vitest-environment jsdom
import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { DataTable } from "@/components/ui/table";

afterEach(cleanup);

describe("DataTable", () => {
  it("renders rows", () => {
    render(
      <DataTable
        columns={[
          { key: "name", header: "Name" },
          { key: "status", header: "Status" },
        ]}
        rows={[
          { name: "alpha", status: "running" },
          { name: "beta", status: "succeeded" },
        ]}
        rowKey={(row) => String(row.name)}
      />,
    );
    const table = screen.getByRole("table");
    expect(within(table).getByText("alpha")).toBeTruthy();
    expect(within(table).getByText("beta")).toBeTruthy();
    expect(within(table).getByText("succeeded")).toBeTruthy();
  });

  it("renders the empty state instead of a fake row", () => {
    render(
      <DataTable columns={[{ key: "name", header: "Name" }]} rows={[]} emptyLabel="Nothing here." />,
    );
    expect(screen.getByText("Nothing here.")).toBeTruthy();
  });

  it("uses custom renderers when provided", () => {
    render(
      <DataTable
        columns={[
          {
            key: "value",
            header: "Value",
            render: (row) => <strong>{`#${row.value}`}</strong>,
          },
        ]}
        rows={[{ value: 7 }]}
      />,
    );
    expect(screen.getByText("#7")).toBeTruthy();
  });
});
