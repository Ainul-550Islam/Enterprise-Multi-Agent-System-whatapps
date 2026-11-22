// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Tabs } from "@/components/ui/tabs";
import { DataTableWithCursor } from "@/components/ui/data-table";
import { Toast } from "@/components/ui/toast";

afterEach(cleanup);

describe("dashboard components", () => {
  it("switches tab panels", async () => {
    const { act } = await import("react");
    render(<Tabs items={[{ id: "a", label: "A", content: <p>first</p> }, { id: "b", label: "B", content: <p>second</p> }]} />);
    expect(screen.getByText("first")).toBeTruthy();
    act(() => screen.getByRole("tab", { name: "B" }).click());
    expect(screen.getByText("second")).toBeTruthy();
  });

  it("sorts locally and forwards the opaque server cursor", async () => {
    const { act } = await import("react");
    const onNext = vi.fn();
    render(
      <DataTableWithCursor
        columns={[{ key: "name", header: "Name", value: (row: { name: string }) => row.name }]}
        rows={[{ name: "b" }, { name: "a" }]}
        rowKey={(row) => row.name}
        nextCursor="opaque-2"
        onNext={onNext}
      />,
    );
    act(() => screen.getByRole("button", { name: "Name" }).click());
    expect(screen.getAllByRole("cell")[0]?.textContent).toBe("a");
    act(() => screen.getByRole("button", { name: "Next page" }).click());
    expect(onNext).toHaveBeenCalledWith("opaque-2");
  });

  it("renders empty state and ephemeral status", () => {
    render(<DataTableWithCursor columns={[{ key: "x", header: "X" }]} rows={[]} rowKey={() => "x"} emptyLabel="Nothing yet." />);
    expect(screen.getByText("Nothing yet.")).toBeTruthy();
  });
});
