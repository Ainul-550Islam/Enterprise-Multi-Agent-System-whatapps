import { expect, test } from "@playwright/test";

test("loads the dashboard", async ({ page }) => {
  await page.goto("/login");
  await expect(page.getByLabel("API key")).toBeEditable();
  await page.getByLabel("API key").fill("mas_live_e2e_key");
  await page.getByRole("button", { name: "Continue with API key" }).click();
  await page.waitForURL(/\/overview/);
  await expect(page).toHaveURL(/\/overview/);
});

test("API-key login, create agent, start run, inspect steps", async ({ page }) => {
  await page.goto("/login");
  await expect(page.getByLabel("API key")).toBeEditable();
  await page.getByLabel("API key").fill("mas_live_e2e_key");
  await page.getByRole("button", { name: "Continue with API key" }).click();
  await page.waitForURL(/\/overview/);

  await page.goto("/agents");
  await page.getByRole("button", { name: "Register agent" }).click();
  await page.getByLabel("Name").fill("support-triage");
  await page.getByLabel("Project id").fill("proj-e2e");
  await page.getByRole("button", { name: "Create" }).click();
  await expect(page.getByText("support-triage")).toBeVisible();

  await page.goto("/executions");
  await page.getByRole("button", { name: "Start execution" }).click();
  await page.getByLabel("Project id").fill("proj-e2e");
  await page.getByLabel("Agent id").fill("agent-1");
  await page.getByRole("button", { name: "Start", exact: true }).click();
  await expect(page.getByText("exec-1")).toBeVisible();

  await page.getByRole("link", { name: "Details" }).click();
  await expect(page.getByRole("heading", { name: /Execution exec-1/ })).toBeVisible();
  await expect(page.getByText("agent.run")).toBeVisible();
});
