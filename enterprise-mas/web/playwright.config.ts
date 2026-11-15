import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: './src/e2e',
  fullyParallel: true,
  globalSetup: './global-setup.ts',
  timeout: 120_000,
  expect: {
    timeout: 5000,
  },
  projects: [
    {
      name: 'Chromium',
      use: {
        ...devices['Desktop Chrome'],
        baseURL: process.env.PLAYWRIGHT_BASE_URL,
      },
    },
    {
      name: 'Firefox',
      use: {
        ...devices['Desktop Firefox'],
        baseURL: process.env.PLAYWRIGHT_BASE_URL,
      },
    },
    {
      name: 'WebKit',
      use: {
        ...devices['Desktop Safari'],
        baseURL: process.env.PLAYWRIGHT_BASE_URL,
      },
    },
  ],
})
