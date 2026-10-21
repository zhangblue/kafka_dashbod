import { expect } from '@playwright/test'

async function tokenCheck(action, operation) {
  try {
    await operation()
  } catch {
    // Do not retain the original error, matcherResult or cause: Playwright's
    // call log can contain fill arguments and an input's current value.
    throw new Error(`Token ${action}检查失败`)
  }
}

export async function fillToken(input, value, options = {}) {
  await tokenCheck('填写', () => input.fill(value, { timeout: 20_000, ...options }))
}

export async function expectTokenEmpty(input, { timeout = 20_000 } = {}) {
  await tokenCheck('清空', async () => {
    // Only a boolean crosses the browser boundary; eventual clearing still polls.
    await expect.poll(() => input.evaluate(element => element.value === '', undefined, { timeout }), { timeout }).toBe(true)
  })
}

export async function expectTokenVisibility(input, visible, { timeout = 20_000 } = {}) {
  await tokenCheck('可见性', async () => {
    await expect.poll(() => input.evaluate((element, shown) => element.type === (shown ? 'text' : 'password')
      && (!shown || element.value.length > 0), visible, { timeout }), { timeout }).toBe(true)
  })
}
