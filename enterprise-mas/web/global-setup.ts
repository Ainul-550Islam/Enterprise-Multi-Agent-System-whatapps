import { test, expect } from '@playwright/test'

export default async function globalSetup() {
  console.log('Playwright test suite starting')
}

export async function start() {
  console.log('Playwright test suite started')
}
