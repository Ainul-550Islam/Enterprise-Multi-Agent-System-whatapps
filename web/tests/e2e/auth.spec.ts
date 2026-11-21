import { expect, test } from "@playwright/test";

test.describe("API-key login", () => {
  test("signs in through the BFF and never writes the key to localStorage", async ({ page }) => {
    await page.goto("/login");
    await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
    await expect(page.getByLabel("API key")).toBeEditable();
    await page.getByLabel("API key").fill("mas_live_e2e_key");
    await page.getByRole("button", { name: "Continue with API key" }).click();
    await page.waitForURL(/\/overview/);
    await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
    const stored = await page.evaluate(() => window.localStorage.getItem("mas.apiKey"));
    expect(stored).toBeNull();
  });
});
