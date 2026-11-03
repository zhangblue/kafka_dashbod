import { after, before, test } from 'node:test'
import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { mkdtemp, readdir, readFile, rm } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { chromium } from '@playwright/test'
import { fillToken, expectTokenEmpty, expectTokenVisibility } from '../e2e/token-safety.mjs'

const synthetic = ['synthetic', 'token', 'diagnostic', 'fixture'].join('-')
let browser
before(/* 在安全回归开始前启动独立浏览器，供受控输入场景复用。 */ async () => { browser = await chromium.launch() })
after(/* 在安全回归结束后关闭浏览器并释放原生进程。 */ async () => { await browser?.close() })

// 在 Token 安全回归开始时创建独立输入页并登记关闭清理，只使用合成敏感值。
async function inputFixture(t, { disabled = false } = {}) {
  const page = await browser.newPage()
  t.after(() => page.close())
  await page.setContent(`<input aria-label="Token" type="password" ${disabled ? 'disabled' : ''}>`)
  const input = page.getByLabel('Token', { exact: true })
  await input.evaluate((element, value) => { element.value = value }, synthetic)
  return input
}

// 在预期失败场景中捕获诊断并只返回泄露与固定消息布尔值，避免断言回显原错误。
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

for (const [name, operation] of [
  ['填写', input => fillToken(input, synthetic, { timeout: 50 })],
  ['清空', input => expectTokenEmpty(input, { timeout: 50 })],
  ['可见性', input => expectTokenVisibility(input, true, { timeout: 50 })],
]) {
  test(`Token ${name}失败仅暴露固定诊断`, /* 在本用例执行时验证Token 操作失败只保留固定诊断且没有合成值。 */ async t => {
    const input = await inputFixture(t, { disabled: true })
    const failure = await captureFailure(() => operation(input), `Token ${name}检查失败`)
    assert.equal(failure.failed, true, '受控场景必须触发失败')
    assert.equal(failure.leaks, false, '安全诊断不得包含合成敏感值')
    assert.equal(failure.fixedMessage, true, '诊断必须是固定消息')
  })
}

test('Token helpers preserve successful fills, visibility checks and eventual clearing', /* 在本用例执行时验证成功填写、显隐检查与延迟清空均保持正常语义。 */ async t => {
  const input = await inputFixture(t)
  await fillToken(input, synthetic)
  assert.equal(await input.evaluate((element, value) => element.value === value, synthetic), true)
  await expectTokenVisibility(input, false)
  await input.evaluate(element => { element.type = 'text' })
  await expectTokenVisibility(input, true)
  await input.evaluate(element => { setTimeout(() => { element.value = '' }, 50) })
  await expectTokenEmpty(input)
})

// 在安全验收扫描阶段递归枚举临时附件目录，覆盖错误上下文和官方归档。
async function allFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true })
  return (await Promise.all(entries.map(entry => entry.isDirectory()
    ? allFiles(path.join(directory, entry.name)) : path.join(directory, entry.name)))).flat()
}

test('real Playwright runner keeps steps, stdout, failure context and official blob secret-free', /* 在本用例执行时验证真实 runner 步骤、输出、上下文附件和官方 blob 全部无合成值。 */ async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'token-safety-runner-'))
  try {
    const output = path.join(directory, 'output')
    const blob = path.join(directory, 'blob')
    const steps = path.join(directory, 'steps.json')
    const frontend = fileURLToPath(new URL('..', import.meta.url))
    // 运行真实受控失败 runner 并保留临时产物，才能覆盖步骤序列、最终错误及官方附件边界。
    const child = spawnSync(process.execPath, ['node_modules/@playwright/test/cli.js', 'test',
      '--config', 'scripts/token-safety-probes/config.mjs'], {
      cwd: frontend, encoding: 'utf8', timeout: 60_000, maxBuffer: 4 * 1024 * 1024,
      env: { ...process.env, TOKEN_SAFETY_PROBE: synthetic, TOKEN_SAFETY_OUTPUT: output,
        TOKEN_SAFETY_BLOB: blob, TOKEN_SAFETY_STEPS: steps },
    })
    assert.equal(!child.error && child.status === 1, true, '受控失败 runner 必须正常返回失败状态')
    const results = JSON.parse(await readFile(steps, 'utf8'))
    assert.equal((await readFile(steps)).includes(Buffer.from(synthetic)), false, 'runner 步骤不得包含敏感值')
    assert.equal(results.filter(result => result.status === 'failed').length, 5, '必须实际执行五个失败场景')
    assert.equal(results.filter(result => result.status === 'passed').length, 1, '必须实际执行一个成功场景')
    assert.equal(results.filter(result => result.status === 'failed').every(result => result.errors.length === 1
      && /^(Error: )?Token (填写|清空|可见性)检查失败$/.test(result.errors[0].message)), true,
    'runner 最终错误必须仅为固定消息')
    assert.equal(results.filter(result => result.status === 'failed').every(result => result.annotations
      .some(annotation => annotation.type === 'token-scrubbed' && annotation.description === 'true')), true,
    '失败时必须先隐藏并清空输入，或移除无法安全清空的控件')
    const contexts = (await allFiles(output)).filter(file => file.endsWith('error-context.md'))
    assert.equal(contexts.length === 5, true, '五个失败场景必须生成真实 error-context 附件')
    const blobs = (await allFiles(blob)).filter(file => file.endsWith('.zip'))
    assert.equal(blobs.length === 1, true, '必须生成官方 blob 产物')
    // 官方 blob 需解压后逐字节扫描，不能只检查文件名或未展开的压缩内容。
    const expanded = spawnSync('unzip', ['-p', blobs[0]], { maxBuffer: 8 * 1024 * 1024 })
    assert.equal(!expanded.error && expanded.status === 0, true, '必须解压并扫描官方 blob')
    // 扫描输出与附件时只向断言返回布尔结果，避免安全测试自身把原始诊断写入失败报告。
    // No captured child output or artifact bytes reach assertion diagnostics.
    assert.equal(`${child.stdout}${child.stderr}`.includes(synthetic), false, 'runner 标准输出不得包含敏感值')
    assert.equal((await Promise.all(contexts.map(file => readFile(file)))).some(data => data.includes(Buffer.from(synthetic))), false,
      'error-context 附件不得包含敏感值')
    assert.equal(expanded.stdout.includes(Buffer.from(synthetic)), false, '官方 blob 解压内容不得包含敏感值')
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})
