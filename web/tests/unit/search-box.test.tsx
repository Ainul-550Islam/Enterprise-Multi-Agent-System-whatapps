// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ masFetch: vi.fn(), push: vi.fn() }));
vi.mock("@/lib/api/client", () => ({ masFetch: mocks.masFetch }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: mocks.push }) }));

import { SearchBox } from "@/components/search-box";
import { StableApiError } from "@/lib/api/types";

afterEach(() => cleanup());
beforeEach(() => {
  mocks.masFetch.mockReset();
  mocks.push.mockReset();
});

describe("SearchBox", () => {
  it("debounces a query and shows the loading state", async () => {
    let resolve: ((value: { items: []; truncated: boolean }) => void) | undefined;
    mocks.masFetch.mockImplementation(() => new Promise((done) => { resolve = done; }));
    render(<SearchBox />);
    fireEvent.change(screen.getByRole("combobox"), { target: { value: "agent" } });
    expect(await screen.findByText("Searching…")).toBeInTheDocument();
    await waitFor(() => expect(mocks.masFetch).toHaveBeenCalledTimes(1));
    expect(mocks.masFetch).toHaveBeenCalledWith("/v1/search", {
      query: { q: "agent", kinds: "agents,workflows", limit: 8 },
      signal: expect.any(AbortSignal),
    });
    await act(async () => resolve?.({ items: [], truncated: false }));
  });

  it("renders an empty result state", async () => {
    mocks.masFetch.mockResolvedValue({ items: [], truncated: false });
    render(<SearchBox />);
    fireEvent.change(screen.getByRole("combobox"), { target: { value: "nothing" } });
    expect(await screen.findByText("No results")).toBeInTheDocument();
  });

  it("shows stable API errors including a not-provisioned 503", async () => {
    mocks.masFetch.mockRejectedValue(new StableApiError({ code: "SERVICE_NOT_PROVISIONED", message: "search is not provisioned" }));
    render(<SearchBox />);
    fireEvent.change(screen.getByRole("combobox"), { target: { value: "agents" } });
    expect(await screen.findByRole("alert")).toHaveTextContent("SERVICE_NOT_PROVISIONED");
    expect(screen.getByRole("alert")).toHaveTextContent("search is not provisioned");
  });

  it("groups results and navigates to an agent on Enter", async () => {
    mocks.masFetch.mockResolvedValue({
      items: [
        { kind: "agents", id: "agent-1", title: "Support agent", subtitle: "support" },
        { kind: "workflows", id: "workflow-1", title: "Triage flow", subtitle: "v2" },
      ],
      truncated: false,
    });
    render(<SearchBox />);
    const input = screen.getByRole("combobox");
    fireEvent.change(input, { target: { value: "support" } });
    expect(await screen.findByRole("group", { name: "agents" })).toBeInTheDocument();
    expect(screen.getByRole("group", { name: "workflows" })).toBeInTheDocument();
    fireEvent.keyDown(input, { key: "Enter" });
    expect(mocks.push).toHaveBeenCalledWith("/agents/agent-1");
    expect(input).toHaveAttribute("aria-expanded", "false");
  });

  it("closes the list on Escape", async () => {
    mocks.masFetch.mockResolvedValue({ items: [{ kind: "workflows", id: "wf-2", title: "Review" }], truncated: false });
    render(<SearchBox />);
    const input = screen.getByRole("combobox");
    fireEvent.change(input, { target: { value: "review" } });
    expect(await screen.findByText("Review")).toBeInTheDocument();
    fireEvent.keyDown(input, { key: "Escape" });
    expect(screen.queryByRole("listbox")).not.toBeInTheDocument();
  });
});
