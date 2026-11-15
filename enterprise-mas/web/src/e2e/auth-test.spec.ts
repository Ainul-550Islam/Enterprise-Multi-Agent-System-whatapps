import { test, expect, beforeEach } from "vitest/globals"
import { chromium } from "playwright"

const BASE_URL = process.env.PLAYWRIGHT_BASE_URL || "http://localhost:3000"

test.describe("Authentication Flow", () => {
  test("should show login page initially", async ({ page }) => {
    await page.goto(BASE_URL)
    await expect(page).toHaveTitle(/Sign In/)
  })

  test("should login with API key", async ({ page }) => {
    await page.goto(`${BASE_URL}/(auth)/login`)
    
    // Select API Key method
    await page.click('label:has-text("API Key")')
    
    // Enter API key
    await page.fill('input[type="password"]', "sk-test-key-12345")
    
    // Submit
    await page.click("button:has-text('Sign In')")
    
    // Should redirect to dashboard
    await expect(page).toHaveURL(`${BASE_URL}/(dashboard)/overview`)
  })

  test("should login with OIDC", async ({ page }) => {
    await page.goto(`${BASE_URL}/(auth)/login`)
    
    // Select OIDC method
    await page.click('label:has-text("OIDC")')
    
    // Click continue with OIDC
    await page.click("button:has-text('Continue with OIDC')")
    
    // Should redirect to identity provider
    await expect(page).toHaveURL(/.*provider.*/)
  })
})

test.describe("Dashboard Flow", () => {
  beforeEach(async ({ page }) => {
    // Login before each test
    await page.goto(`${BASE_URL}/(auth)/login`)
    await page.click('label:has-text("API Key")')
    await page.fill('input[type="password"]', "sk-test-key-12345")
    await page.click("button:has-text('Sign In')")
    await expect(page).toHaveURL(`${BASE_URL}/(dashboard)/overview`)
  })

  test("should show Lite tier by default", async ({ page }) => {
    await expect(page.locator("h2")).toHaveText("Lite Tier Overview")
  })

  test("should switch to FULL tier", async ({ page }) => {
    await page.click("button:has-text('FULL')")
    await expect(page.locator("h2")).toHaveText("Full Tier Overview")
  })

  test("should switch back to LITE tier", async ({ page }) => {
    await page.click("button:has-text('LITE')")
    await expect(page.locator("h2")).toHaveText("Lite Tier Overview")
  })
})
