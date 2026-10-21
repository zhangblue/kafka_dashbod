import { after, before, test } from 'node:test'
import assert from 'node:assert/strict'
import { chromium, expect } from '@playwright/test'
import { fillToken, expectTokenEmpty, expectTokenVisibility } from '../e2e/token-safety.mjs'

const synthetic = ['synthetic', 'token', 'diagnostic', 'fixture'].join('-')
let browser
before(async () => { browser = await chromium.launch() })
after(async () => { await browser?.close() })

async function inputFixture(t, { disabled = false } = {}) {
  const page = await browser.newPage()
  t.after(() => page.close())
  await page.setContent(`<input aria-label="Token" type="password" ${disabled ? 'disabled' : ''}>`)
  const input = page.getByLabel('Token', { exact: true })
  await input.evaluate((element, value) => { element.value = value }, synthetic)
  return input
}

async function captureFailure(operation, expectedMessage) {
  try {
    await operation()
    return { failed: false, leaks: false, fixedMessage: false }
  } catch (error) {
    // Never return raw diagnostics to assertions or reporters.
    const diagnostic = `${String(error)}\n${error?.stack ?? ''}\n${JSON.stringify(error)}`
    return { failed: true, leaks: diagnostic.includes(synthetic), fixedMessage: error?.message === expectedMessage }
  }
}

test('controlled legacy empty-value and fill failures expose synthetic input', async t => {
  const input = await inputFixture(t, { disabled: true })
  const empty = await captureFailure(() => expect(input).toHaveValue('', { timeout: 50 }))
  const fill = await captureFailure(() => input.fill(synthetic, { timeout: 50 }))
  assert.equal(empty.failed && empty.leaks && fill.failed && fill.leaks, true, '旧诊断风险必须能够稳定复现')
})

for (const [name, operation] of [
  ['填写', input => fillToken(input, synthetic, { timeout: 50 })],
  ['清空', input => expectTokenEmpty(input, { timeout: 50 })],
  ['可见性', input => expectTokenVisibility(input, true, { timeout: 50 })],
]) {
  test(`Token ${name}失败仅暴露固定诊断`, async t => {
    const input = await inputFixture(t, { disabled: true })
    const failure = await captureFailure(() => operation(input), `Token ${name}检查失败`)
    assert.equal(failure.failed, true, '受控场景必须触发失败')
    assert.equal(failure.leaks, false, '安全诊断不得包含合成敏感值')
    assert.equal(failure.fixedMessage, true, '诊断必须是固定消息')
  })
}

test('Token helpers preserve successful fills, visibility checks and eventual clearing', async t => {
  const input = await inputFixture(t)
  await fillToken(input, synthetic)
  assert.equal(await input.evaluate((element, value) => element.value === value, synthetic), true)
  await expectTokenVisibility(input, false)
  await input.evaluate(element => { element.type = 'text' })
  await expectTokenVisibility(input, true)
  await input.evaluate(element => { setTimeout(() => { element.value = '' }, 50) })
  await expectTokenEmpty(input)
})
