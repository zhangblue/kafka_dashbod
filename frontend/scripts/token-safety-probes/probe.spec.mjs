import { test, expect } from '@playwright/test'
import { fillToken, expectTokenEmpty, expectTokenVisibility } from '../../e2e/token-safety.mjs'

const synthetic = process.env.TOKEN_SAFETY_PROBE

test.beforeEach(/* 在每个安全探针前建立独立密码输入页，避免复用上个失败场景的 DOM。 */ async ({ page }) => {
  await page.setContent('<input aria-label="Token" type="password">')
})

test.afterEach(/* 在探针结束时只记录密码框是否已清空的布尔标记，供外层验证错误附件生成前的清理。 */ async ({ page }, info) => {
  const scrubbed = await page.evaluate(() => [...document.querySelectorAll('input')]
    .every(element => element.type === 'password' && element.value === ''))
  info.annotations.push({ type: 'token-scrubbed', description: String(scrubbed) })
})

test('setting missing input fails safely', /* 在本用例执行时验证输入不存在时失败诊断仍不泄露 Token。 */ async ({ page }) => {
  await fillToken(page.getByLabel('missing', { exact: true }), synthetic, { timeout: 100 })
})

test('setting with a throwing DOM setter fails safely', /* 在本用例执行时验证DOM setter 抛出含合成值异常后仍安全清理。 */ async ({ page }) => {
  await page.evaluate(value => {
    Object.defineProperty(HTMLInputElement.prototype, 'value', { configurable: true,
      // 在 setter 异常探针中提供空值读取，避免合成秘密进入普通诊断。
      get() { return '' }, /* 在探针填写阶段故意抛出含合成值异常，验证错误边界会抹除原诊断。 */ set() { throw new Error(value) } })
  }, synthetic)
  await fillToken(page.getByLabel('Token', { exact: true }), synthetic, { timeout: 100 })
})

test('uncleared visible input fails safely', /* 在本用例执行时验证可见且未清空输入触发固定错误并先清理页面。 */ async ({ page }) => {
  const input = page.getByLabel('Token', { exact: true })
  await fillToken(input, synthetic)
  await input.evaluate(element => { element.type = 'text' })
  await expectTokenEmpty(input, { timeout: 100 })
})

test('unexpectedly visible input fails safely', /* 在本用例执行时验证意外可见的输入触发安全失败。 */ async ({ page }) => {
  const input = page.getByLabel('Token', { exact: true })
  await fillToken(input, synthetic)
  await input.evaluate(element => { element.type = 'text' })
  await expectTokenVisibility(input, false, { timeout: 100 })
})

test('ambiguous input locators fail without DOM previews', /* 在本用例执行时验证模糊定位失败不附带含凭据的 DOM 预览。 */ async ({ page }) => {
  await page.evaluate(value => {
    document.body.innerHTML = '<input aria-label="Token" type="text"><input aria-label="Token" type="text">'
    for (const element of document.querySelectorAll('input')) element.setAttribute('value', value)
  }, synthetic)
  await expectTokenEmpty(page.getByLabel('Token', { exact: true }), { timeout: 100 })
})

test('successful setting records no sensitive steps', /* 在本用例执行时验证成功填写步骤也不能记录敏感值。 */ async ({ page }) => {
  const input = page.getByLabel('Token', { exact: true })
  await fillToken(input, synthetic)
  expect(await input.evaluate((element, value) => element.value === value, synthetic)).toBe(true)
})
