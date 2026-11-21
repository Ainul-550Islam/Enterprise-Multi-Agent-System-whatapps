// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { Badge, toneForStatus } from "@/components/ui/badge";

afterEach(cleanup);

describe("Badge", () => {
  it("renders tone", () => {
    render(<Badge tone="ok">healthy</Badge>);
    const badge = screen.getByText("healthy");
    expect(badge.getAttribute("data-tone")).toBe("ok");
    expect(badge.className).toContain("mas-badge-ok");
  });

  it("defaults to neutral", () => {
    render(<Badge>plain</Badge>);
    expect(screen.getByText("plain").getAttribute("data-tone")).toBe("neutral");
  });

  it("maps resource statuses to tones", () => {
    expect(toneForStatus("succeeded")).toBe("ok");
    expect(toneForStatus("running")).toBe("warn");
    expect(toneForStatus("failed")).toBe("danger");
    expect(toneForStatus("mystery")).toBe("neutral");
  });
});
