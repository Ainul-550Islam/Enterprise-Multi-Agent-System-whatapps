import { expect, test } from "@playwright/test";

async function login(page: import("@playwright/test").Page) {
  await page.goto("/login");
  await page.getByLabel("API key").fill("mas_live_e2e_key");
  await page.getByRole("button", { name: "Continue with API key" }).click();
  await page.waitForURL(/\/overview/);
}

test("operator can create a schedule from the API contract", async ({ page }) => {
  await login(page);
  await page.goto("/schedules");
  await expect(page.getByRole("heading", { name: "Schedules", level: 1 })).toBeVisible();
  await page.getByRole("button", { name: "New schedule" }).click();
  await page.getByLabel("Name").fill("nightly-report");
  await page.getByLabel("Task template (JSON object)").fill('{"prompt":"daily report"}');
  await page.getByRole("button", { name: "Create schedule" }).click();
  await expect(page.getByText("nightly-report")).toBeVisible();
});

test("team screen renders real API rows", async ({ page }) => {
  await login(page);
  await page.goto("/team");
  await expect(page.getByRole("heading", { name: "Team" })).toBeVisible();
  await expect(page.getByText("operator@example.test")).toBeVisible({ timeout: 15_000 });
});

test("knowledge hub loads the selected project's data sources", async ({ page }) => {
  await login(page);
  await page.goto("/knowledge");
  await expect(page.getByRole("heading", { name: "Knowledge / Data Hub" })).toBeVisible();
  await page.getByLabel("Project id").fill("33333333-3333-3333-3333-333333333333");
  await expect(page.getByText("Product docs")).toBeVisible();
});
