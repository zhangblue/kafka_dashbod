import { expect } from '@playwright/test'

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

async function tokenCheck(input, action, operation) {
  try {
    await operation()
  } catch {
    await discardTokenInputs(input)
    // Retain no original error/cause/matcherResult in runner steps or attachments.
    throw new Error(`Token ${action}检查失败`)
  }
}

export async function fillToken(input, value, { timeout = 20_000 } = {}) {
  await tokenCheck(input, '填写', async () => {
    // Evaluate arguments are omitted from Playwright's reported step parameters.
    // Never pass credentials to fill/type/press, whose titles can record values.
    // evaluateAll avoids strict-locator errors that include matching DOM previews.
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

export async function expectTokenEmpty(input, { timeout = 20_000 } = {}) {
  await tokenCheck(input, '清空', async () => {
    // Only a boolean crosses the browser boundary; eventual clearing still polls.
    await expect.poll(() => input.evaluateAll(elements => {
      try { return elements.length === 1 && elements[0].value === '' } catch { return false }
    }), { timeout }).toBe(true)
  })
}

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
