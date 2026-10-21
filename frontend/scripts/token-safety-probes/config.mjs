import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: '.', testMatch: 'probe.spec.mjs', workers: 1, retries: 0, timeout: 10_000,
  outputDir: process.env.TOKEN_SAFETY_OUTPUT,
  reporter: [['list'], ['./reporter.mjs'], ['blob', { outputDir: process.env.TOKEN_SAFETY_BLOB }]],
  use: { browserName: 'chromium', trace: 'off', screenshot: 'off', video: 'off' },
})
