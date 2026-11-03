import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: '.', testMatch: 'probe.spec.mjs', workers: 1, retries: 0, timeout: 10_000,
  outputDir: process.env.TOKEN_SAFETY_OUTPUT,
  reporter: [['list'], ['./reporter.mjs'], ['blob', { outputDir: process.env.TOKEN_SAFETY_BLOB }]],
  // 探针故意触发含合成秘密的失败；关闭 trace、截图和视频，避免浏览器画面形成额外泄露通道。
  use: { browserName: 'chromium', trace: 'off', screenshot: 'off', video: 'off' },
})
