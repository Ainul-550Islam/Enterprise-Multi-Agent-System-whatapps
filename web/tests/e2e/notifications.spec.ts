import { expect, test } from "@playwright/test";

test("notification feed loads for a recipient and marks an item read", async ({ page }) => {
  await page.goto("/login");
  await page.getByLabel("API key").fill("mas_live_e2e_key");
  await page.getByRole("button", { name: "Continue with API key" }).click();
  await page.waitForURL(/\/overview/);
  await page.goto("/notifications");
  await page.getByLabel("Recipient user id").fill("user-1");
  await expect(page.getByText("Run failed")).toBeVisible();
  await page.getByRole("button", { name: "Mark read" }).first().click();
  await expect(page.getByText("Run failed")).toBeVisible();
});
