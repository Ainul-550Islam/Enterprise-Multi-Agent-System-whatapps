import { describe, expect, it } from "vitest";

import { readCsrfCookie } from "@/lib/csrf";

describe("schedule page environment", () => {
  it("reads the CSRF token from the browser cookie", () => {
    const original = document.cookie;
    document.cookie = "mas_csrf=test-token; path=/";
    expect(readCsrfCookie()).toBe("test-token");
    document.cookie = original;
  });
});
