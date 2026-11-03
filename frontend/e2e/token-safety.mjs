import { expect } from '@playwright/test'

// 在 Token 检查失败后先隐藏并清空输入，无法清理时移除控件或关闭页面以保护错误附件。
async function discardTokenInputs(input) {
  try {
    await input.page().evaluate(() => {
      for (const element of document.querySelectorAll('input')) {
        if (!['text', 'password'].includes(element.type)) continue
        try {
          element.type = 'password'
          element.removeAttribute('value')
          const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set
          if (!setter) { element.remove(); continue }
          setter.call(element, '')
          element.dispatchEvent(new Event('input', { bubbles: true }))
          element.dispatchEvent(new Event('change', { bubbles: true }))
          element.type = 'password'
        } catch {
          // An unusable setter must not leave a credential for error-context.
          element.remove()
        }
      }
    })
  } catch {
    // If the page cannot be scrubbed, prevent the runner from snapshotting it.
    try { await input.page().close() } catch { /* Already inaccessible. */ }
  }
}

// 在每次 Token 浏览器操作外统一捕获异常，先清理 DOM 再抛固定错误，防止原诊断泄露输入。
async function tokenCheck(input, action, operation) {
  try {
    await operation()
  } catch {
    // 必须先清理页面再抛固定错误，让 runner 随后生成的上下文不携带原凭据或原异常。
    await discardTokenInputs(input)
    // Retain no original error/cause/matcherResult in runner steps or attachments.
    throw new Error(`Token ${action}检查失败`)
  }
}

// 在浏览器验收输入合成 Token 时使用不记录参数的 DOM 操作，避免步骤标题和定位错误暴露凭据。
export async function fillToken(input, value, { timeout = 20_000 } = {}) {
  await tokenCheck(input, '填写', async () => {
    // Evaluate arguments are omitted from Playwright's reported step parameters.
    // Never pass credentials to fill/type/press, whose titles can record values.
    // evaluateAll avoids strict-locator errors that include matching DOM previews.
    // 只让布尔结果跨浏览器边界；避免 fill/type 的步骤参数或严格定位失败预览记录秘密。
    await expect.poll(() => input.evaluateAll((elements, nextValue) => {
      try {
        if (elements.length !== 1) return false
        const element = elements[0]
        if (!(element instanceof HTMLInputElement) || element.disabled || element.readOnly) return false
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set
        if (!setter) return false
        setter.call(element, nextValue)
        element.dispatchEvent(new Event('input', { bubbles: true }))
        element.dispatchEvent(new Event('change', { bubbles: true }))
        return true
      } catch {
        // A DOM exception can contain its input: only a boolean may leave here.
        return false
      }
    }, value), { timeout }).toBe(true)
  })
}

// 在保存、清除或切换配置后轮询输入是否为空，仅让布尔结果进入测试报告。
export async function expectTokenEmpty(input, { timeout = 20_000 } = {}) {
  await tokenCheck(input, '清空', async () => {
    // Only a boolean crosses the browser boundary; eventual clearing still polls.
    await expect.poll(() => input.evaluateAll(elements => {
      try { return elements.length === 1 && elements[0].value === '' } catch { return false }
    }), { timeout }).toBe(true)
  })
}

// 在显隐按钮交互后检查输入类型与非空状态，仅返回布尔值而不读取凭据到断言。
export async function expectTokenVisibility(input, visible, { timeout = 20_000 } = {}) {
  await tokenCheck(input, '可见性', async () => {
    await expect.poll(() => input.evaluateAll((elements, shown) => {
      try {
        if (elements.length !== 1) return false
        const element = elements[0]
        return element.type === (shown ? 'text' : 'password') && (!shown || element.value.length > 0)
      }
      catch { return false }
    }, visible), { timeout }).toBe(true)
  })
}
