// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ reload: vi.fn(), post: vi.fn(), useMas: vi.fn() }));
vi.mock("@/lib/api/useMas", () => ({ useMas: mocks.useMas }));
vi.mock("@/lib/api/client", () => ({ api: { post: mocks.post } }));

import SchedulesPage from "@/app/(dashboard)/schedules/page";
import { StableApiError } from "@/lib/api/types";

afterEach(cleanup);
beforeEach(() => {
  mocks.reload.mockReset();
  mocks.post.mockReset().mockResolvedValue({ id: "schedule-2", name: "nightly", status: "active" });
  mocks.useMas.mockReturnValue({ data: [], error: null, loading: false, reload: mocks.reload });
});

describe("SchedulesPage", () => {
  it("renders the loading state", () => {
    mocks.useMas.mockReturnValue({ data: null, error: null, loading: true, reload: mocks.reload });
    render(<SchedulesPage />);
    expect(screen.getByText("Loading…")).toBeInTheDocument();
  });

  it("renders the empty state", () => {
    render(<SchedulesPage />);
    expect(screen.getByText("No schedules yet.")).toBeInTheDocument();
  });

  it("explains an unprovisioned schedule plane", () => {
    mocks.useMas.mockReturnValue({
      data: null,
      error: new StableApiError({ code: "SERVICE_NOT_PROVISIONED", message: "schedule plane unavailable" }),
      loading: false,
      reload: mocks.reload,
    });
    render(<SchedulesPage />);
    expect(screen.getByText("Schedules are not provisioned")).toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent("SERVICE_NOT_PROVISIONED");
  });

  it("renders rows and submits an interval without run_at", async () => {
    mocks.useMas.mockReturnValue({
      data: [{ id: "schedule-1", name: "hourly", status: "active", next_run_at: "2030-01-01T00:00:00Z" }],
      error: null,
      loading: false,
      reload: mocks.reload,
    });
    render(<SchedulesPage />);
    expect(screen.getByText("hourly")).toBeInTheDocument();
    expect(screen.getByText("active")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "New schedule" }));
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "nightly" } });
    fireEvent.change(screen.getByLabelText("Every (seconds)"), { target: { value: "600" } });
    fireEvent.change(screen.getByLabelText("Task template (JSON object)"), { target: { value: "{\"operation\":\"report\"}" } });
    fireEvent.click(screen.getByRole("button", { name: "Create schedule" }));

    await waitFor(() => expect(mocks.post).toHaveBeenCalledWith("/v1/schedules", {
      name: "nightly",
      kind: "interval",
      task_template: { operation: "report" },
      every_seconds: 600,
    }));
    expect(mocks.post.mock.calls[0]?.[1]).not.toHaveProperty("run_at");
    await waitFor(() => expect(mocks.reload).toHaveBeenCalled());
    expect(await screen.findByText("Schedule created.")).toBeInTheDocument();
  });

  it("submits a one-time schedule without every_seconds", async () => {
    render(<SchedulesPage />);
    fireEvent.click(screen.getByRole("button", { name: "New schedule" }));
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "one-off" } });
    fireEvent.change(screen.getByLabelText("Kind"), { target: { value: "one_time" } });
    fireEvent.change(screen.getByLabelText("Run at (RFC 3339)"), { target: { value: "2030-06-01T12:00:00Z" } });
    fireEvent.click(screen.getByRole("button", { name: "Create schedule" }));
    await waitFor(() => expect(mocks.post).toHaveBeenCalledWith("/v1/schedules", {
      name: "one-off",
      kind: "one_time",
      task_template: {},
      run_at: "2030-06-01T12:00:00Z",
    }));
    expect(mocks.post.mock.calls[0]?.[1]).not.toHaveProperty("every_seconds");
  });
});
