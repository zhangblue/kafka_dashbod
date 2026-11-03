import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './e2e',
  workers: 1,
  retries: 0,
  timeout: 120_000,
  expect: { timeout: 20_000 },
  reporter: 'list',
  use: { baseURL: process.env.E2E_BASE_URL, browserName: 'chromium', locale: 'zh-CN', trace: 'off', screenshot: 'off', video: 'off' },
})
