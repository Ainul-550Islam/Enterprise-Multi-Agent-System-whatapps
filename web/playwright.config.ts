import { defineConfig, devices } from "@playwright/test";

const mockPort = process.env.MOCK_MAS_PORT ?? "18080";

export default defineConfig({
  testDir: "tests/e2e",
  timeout: 60_000,
  fullyParallel: false,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  use: {
    baseURL: process.env.MAS_WEB_BASE_URL ?? "http://127.0.0.1:3100",
    trace: "on-first-retry",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: process.env.MAS_WEB_BASE_URL
    ? undefined
    : [
        {
          command: `node tests/e2e/mock-mas.mjs`,
          url: `http://127.0.0.1:${mockPort}/v1/health/live`,
          reuseExistingServer: false,
          env: { ...process.env, MOCK_MAS_PORT: mockPort },
        },
        {
          command: "npx next dev --hostname 127.0.0.1 --port 3100",
          url: "http://127.0.0.1:3100/login",
          reuseExistingServer: false,
          timeout: 120_000,
          env: {
            ...process.env,
            MAS_API_URL: `http://127.0.0.1:${mockPort}`,
            SESSION_SECRET: "playwright-session-secret-must-be-32-chars",
            NEXT_TELEMETRY_DISABLED: "1",
          },
        },
      ],
});
