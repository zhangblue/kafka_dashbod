import { test, expect } from '@playwright/test'
import { fillToken, expectTokenEmpty, expectTokenVisibility } from '../../e2e/token-safety.mjs'

const synthetic = process.env.TOKEN_SAFETY_PROBE

test.beforeEach(async ({ page }) => {
  await page.setContent('<input aria-label="Token" type="password">')
})

test.afterEach(async ({ page }, info) => {
  const scrubbed = await page.evaluate(() => [...document.querySelectorAll('input')]
    .every(element => element.type === 'password' && element.value === ''))
  info.annotations.push({ type: 'token-scrubbed', description: String(scrubbed) })
})

test('setting missing input fails safely', async ({ page }) => {
  await fillToken(page.getByLabel('missing', { exact: true }), synthetic, { timeout: 100 })
})

test('setting with a throwing DOM setter fails safely', async ({ page }) => {
  await page.evaluate(value => {
    Object.defineProperty(HTMLInputElement.prototype, 'value', { configurable: true,
      get() { return '' }, set() { throw new Error(value) } })
  }, synthetic)
  await fillToken(page.getByLabel('Token', { exact: true }), synthetic, { timeout: 100 })
})

test('uncleared visible input fails safely', async ({ page }) => {
  const input = page.getByLabel('Token', { exact: true })
  await fillToken(input, synthetic)
  await input.evaluate(element => { element.type = 'text' })
  await expectTokenEmpty(input, { timeout: 100 })
})

test('unexpectedly visible input fails safely', async ({ page }) => {
  const input = page.getByLabel('Token', { exact: true })
  await fillToken(input, synthetic)
  await input.evaluate(element => { element.type = 'text' })
  await expectTokenVisibility(input, false, { timeout: 100 })
})

test('ambiguous input locators fail without DOM previews', async ({ page }) => {
  await page.evaluate(value => {
    document.body.innerHTML = '<input aria-label="Token" type="text"><input aria-label="Token" type="text">'
    for (const element of document.querySelectorAll('input')) element.setAttribute('value', value)
  }, synthetic)
  await expectTokenEmpty(page.getByLabel('Token', { exact: true }), { timeout: 100 })
})

test('successful setting records no sensitive steps', async ({ page }) => {
  const input = page.getByLabel('Token', { exact: true })
  await fillToken(input, synthetic)
  expect(await input.evaluate((element, value) => element.value === value, synthetic)).toBe(true)
})
