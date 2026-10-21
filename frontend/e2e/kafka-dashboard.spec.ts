import { test, expect, type Page } from '@playwright/test'
import { randomUUID } from 'node:crypto'

const topic = 'dashboard-e2e-orders'
const group = 'dashboard-e2e-reader'
const secret = process.env.E2E_FIXTURE_SECRET ?? 'missing-fixture-secret'

test.beforeEach(() => {
  expect(process.env.E2E_BASE_URL, 'Run through scripts/test-e2e.sh to start isolated services').toBeTruthy()
  expect(process.env.E2E_KAFKA_BROKERS).toMatch(/^127\.0\.0\.1:\d+$/)
  const port = Number(process.env.E2E_KAFKA_BROKERS?.split(':')[1])
  expect(port).toBeGreaterThan(0)
  expect(port).toBeLessThanOrEqual(65535)
  expect(process.env.E2E_KAFKA_FIXTURE_PERMIT).toBe('disposable-compose')
})

async function createCluster(page: Page, name: string) {
  await page.getByRole('button', { name: '新增集群', exact: true }).click()
  await expect(page.getByRole('dialog', { name: 'Kafka 集群配置' })).toBeVisible()
  await page.getByLabel('集群名称', { exact: true }).fill(name)
  await page.getByLabel('Bootstrap Servers').fill(process.env.E2E_KAFKA_BROKERS!)
  const tested = page.waitForResponse(r => r.url().endsWith('/api/clusters/test') && r.request().method() === 'POST')
  await page.getByRole('button', { name: '测试当前配置', exact: true }).click()
  expect((await tested).status()).toBe(200)
  await expect(page.getByText(/当前配置连接成功/)).toBeVisible()
  await expect(page.getByRole('group', { name, exact: true })).toHaveCount(0)
  const saved = page.waitForResponse(r => r.url().endsWith('/api/clusters') && r.request().method() === 'POST')
  await page.getByRole('button', { name: '保存集群' }).click()
  const response = await saved
  expect(response.status()).toBe(201)
  await expect(page.getByRole('group', { name, exact: true })).toBeVisible()
  return (await response.json()).id as string
}

// Breaks if UI saves, real Kafka collection, rules, or lease release stop working.
test('cluster CRUD, connection, partitions, lag rules and session lifecycle', async ({ page, request }) => {
  await page.goto('/settings')
  const first = await createCluster(page, 'E2E primary')
  const second = await createCluster(page, 'E2E secondary')
  const card = page.getByRole('group', { name: 'E2E primary', exact: true })
  await card.getByRole('button', { name: '编辑', exact: true }).click()
  await page.getByLabel('集群名称', { exact: true }).fill('E2E edited')
  const draftTest = page.waitForResponse(r => r.url().endsWith(`/api/clusters/${first}/test`) && r.request().method() === 'POST')
  await page.getByRole('button', { name: '测试当前配置', exact: true }).click()
  expect((await draftTest).status()).toBe(200)
  await expect(page.getByText(/当前配置连接成功/)).toBeVisible()
  expect((await (await request.get(`/api/clusters/${first}`)).json()).name).toBe('E2E primary')
  await page.getByRole('button', { name: '保存集群' }).click()
  const edited = page.getByRole('group', { name: 'E2E edited', exact: true })
  await edited.getByRole('button', { name: '测试连接' }).click()
  await expect(edited.getByText(/连接成功/)).toBeVisible()
  await edited.getByRole('button', { name: '配置 Lag 规则' }).click()
  const defaults = page.getByRole('group', { name: '集群默认规则', exact: true })
  await expect(defaults.getByLabel('正常上限', { exact: true })).toBeEnabled()
  await defaults.getByLabel('正常上限', { exact: true }).fill('2')
  await defaults.getByLabel('高 Lag 起始值', { exact: true }).fill('8')
  await defaults.getByLabel('持续超限时间（秒）').fill('0')
  await page.getByRole('button', { name: '新增规则' }).click()
  const rule = page.getByRole('group', { name: '规则 1', exact: true })
  await rule.getByLabel('Consumer Group', { exact: true }).selectOption(group)
  await rule.getByLabel('Topic', { exact: true }).selectOption(topic)
  await rule.getByLabel('正常上限', { exact: true }).fill('1')
  await rule.getByLabel('高 Lag 起始值', { exact: true }).fill('4')
  await page.getByRole('button', { name: '保存规则' }).click()
  await expect(page.getByText('规则已保存', { exact: true })).toBeVisible()
  const savedRules = await (await request.get(`/api/clusters/${first}/lag-rules`)).json()
  expect(savedRules.defaults).toEqual({ normalMax: 2, highMin: 8, overLimitSeconds: 0 })
  expect(savedRules.overrides[0]).toMatchObject({ groupId: group, topic, normalMax: 1, highMin: 4 })
  await page.getByRole('button', { name: '关闭规则设置' }).click()
  await page.getByRole('link', { name: 'Topics', exact: true }).click()
  await page.getByLabel('集群', { exact: true }).selectOption(first)
  await page.getByRole('button', { name: `展开 ${topic}`, exact: true }).click()
  const partitions = page.getByRole('table', { name: `${topic} Partitions`, exact: true })
  await expect(partitions.locator('tbody tr')).toHaveCount(2)
  await expect(partitions.locator('tbody tr').first().getByRole('cell')).toHaveText(['0', '0', '10', '9', '10'])
  await expect(partitions.locator('tbody tr').nth(1).getByRole('cell')).toHaveText(['1', '0', '0', '—', '0'])
  await page.getByRole('link', { name: 'Consumer Groups', exact: true }).click()
  await page.getByRole('button', { name: `展开 ${group} / ${topic}`, exact: true }).click()
  await expect(page.getByRole('table', { name: `${group} / ${topic} Partitions`, exact: true }).locator('tbody tr').first().getByRole('cell')).toHaveText(['0', '10', '9', '4', '6'])
  await expect(page.getByRole('table', { name: `${group} / ${topic} Partitions`, exact: true }).locator('tbody tr').nth(1).getByRole('cell')).toHaveText(['1', '0', '—', '0', '0'])
  await page.getByLabel('趋势 Consumer Group').selectOption(group)
  await page.getByRole('checkbox', { name: `${topic} / Partition 0`, exact: true }).check()
  await expect(page.getByRole('img', { name: `${group} Lag 历史折线图` })).toBeVisible()
  await page.getByText('查看图表数据', { exact: true }).click()
  const groupHistory = page.getByRole('table', { name: `${group} Lag 历史数据` })
  await expect(groupHistory.getByRole('columnheader')).toHaveText(['采集时间', `${topic} / 总 Lag`, `${topic} / Partition 0`])
  await expect(groupHistory.locator('tbody tr').last().getByRole('cell').nth(2)).toHaveText('6')
  await expect(page.getByRole('button', { name: `自定义规则 ${group} / ${topic}` })).toBeVisible()
  let sessionId = ''
  for (const seconds of ['3', '5', '10']) {
    await page.getByLabel('刷新频率').selectOption(seconds)
    const snapshot = await page.waitForResponse(r => /\/api\/sessions\/[^/]+\/snapshot$/.test(r.url()) && r.status() === 200, { timeout: 30_000 })
    sessionId = new URL(snapshot.url()).pathname.split('/')[3]
    await expect(page.getByLabel('刷新频率')).toHaveValue(seconds)
  }
  const closed = page.waitForResponse(r => r.url().endsWith(`/api/sessions/${sessionId}`) && r.request().method() === 'DELETE')
  const opened = page.waitForResponse(r => r.url().endsWith(`/api/clusters/${second}/sessions`) && r.status() === 201)
  await page.getByLabel('集群', { exact: true }).selectOption(second)
  expect((await closed).status()).toBe(204)
  const nextSession = (await (await opened).json()).sessionId
  expect((await request.get(`/api/sessions/${sessionId}/snapshot`)).status()).toBe(410)
  await expect(page.getByText('已连接', { exact: true })).toBeVisible()
  const left = page.waitForResponse(r => r.url().endsWith(`/api/sessions/${nextSession}`) && r.request().method() === 'DELETE')
  await page.getByRole('link', { name: '集群设置', exact: true }).click()
  expect((await left).status()).toBe(204)
  // Deleting a configured cluster also releases a real backend session.
  const active = await (await request.post(`/api/clusters/${first}/sessions`)).json()
  await page.getByRole('group', { name: 'E2E edited', exact: true }).getByRole('button', { name: '删除', exact: true }).click()
  await expect(page.getByRole('group', { name: 'E2E edited', exact: true })).toHaveCount(0)
  expect((await request.get(`/api/sessions/${active.sessionId}/snapshot`)).status()).toBe(410)
  await page.getByRole('group', { name: 'E2E secondary', exact: true }).getByRole('button', { name: '删除', exact: true }).click()
  await expect(page.getByRole('group', { name: 'E2E secondary', exact: true })).toHaveCount(0)
})

// Breaks on missing real Admin writes, stale config reads, incorrect source mapping
// or DELETE instead of removing the override. Never manage the seed Topic.
test('creates a unique Topic, rereads retention, restores default and deletes it', async ({ page, request }) => {
  const managed = `dashboard-e2e-managed-browser-${randomUUID()}`
  let created = false
  let deleted = false
  await page.goto('/settings')
  const clusterId = await createCluster(page, 'E2E Topic management')
  try {
    await page.getByRole('link', { name: 'Topics', exact: true }).click()
    await page.getByLabel('集群', { exact: true }).selectOption(clusterId)
    await expect(page.getByRole('complementary', { name: 'Kafka Lens' })).not.toHaveText(/只读模式/)
    await page.getByRole('button', { name: '新建 Topic', exact: true }).click()
    const create = page.getByRole('dialog', { name: '新建 Topic', exact: true })
    await create.getByLabel('Topic 名称', { exact: true }).fill(managed)
    await create.getByLabel('副本因子', { exact: true }).fill('1')
    const creation = page.waitForResponse(r => /\/api\/sessions\/[^/]+\/topics$/.test(r.url()) && r.request().method() === 'POST')
    await create.getByRole('button', { name: '创建 Topic', exact: true }).click()
    const response = await creation
    created = response.status() === 201
    expect(response.status()).toBe(201)
    await expect(create).not.toBeVisible()
    const row = page.getByRole('row').filter({ has: page.getByRole('button', { name: `展开 ${managed}`, exact: true }) })
    await expect(row).toBeVisible()

    const config = page.getByRole('dialog', { name: 'Topic 参数配置', exact: true })
    const retention = config.getByRole('article', { name: '消息保留时间 配置项', exact: true })
    await row.getByRole('button', { name: `配置 ${managed}`, exact: true }).click()
    await expect(retention.getByText('Broker 默认值', { exact: true })).toBeVisible()
    const original = await retention.getByLabel('消息保留时间', { exact: true }).inputValue()
    expect(original).toMatch(/^-?\d+$/)
    await retention.getByLabel('消息保留时间', { exact: true }).fill('3600000')
    await config.getByRole('button', { name: '保存配置', exact: true }).click()
    await expect(config.getByText('配置已保存，当前值已从 Kafka 重新读取。', { exact: true })).toBeVisible()
    await expect(retention.getByText('Topic 覆盖值', { exact: true })).toBeVisible()
    await config.getByRole('button', { name: '取消', exact: true }).click()
    await row.getByRole('button', { name: `配置 ${managed}`, exact: true }).click()
    await expect(retention.getByLabel('消息保留时间', { exact: true })).toHaveValue('3600000')
    await expect(retention.getByText('Topic 覆盖值', { exact: true })).toBeVisible()
    await retention.getByRole('button', { name: '恢复 消息保留时间 为 Broker 默认值', exact: true }).click()
    await config.getByRole('button', { name: '保存配置', exact: true }).click()
    await expect(config.getByText('配置已保存，当前值已从 Kafka 重新读取。', { exact: true })).toBeVisible()
    await expect(retention.getByLabel('消息保留时间', { exact: true })).toHaveValue(original)
    await expect(retention.getByText('Broker 默认值', { exact: true })).toBeVisible()
    await config.getByRole('button', { name: '取消', exact: true }).click()
    await row.getByRole('button', { name: `配置 ${managed}`, exact: true }).click()
    await expect(retention.getByLabel('消息保留时间', { exact: true })).toHaveValue(original)
    await expect(retention.getByText('Broker 默认值', { exact: true })).toBeVisible()
    await config.getByRole('button', { name: '取消', exact: true }).click()

    await row.getByRole('button', { name: `删除 ${managed}`, exact: true }).click()
    const confirmation = page.getByRole('dialog', { name: '删除 Topic', exact: true })
    await expect(confirmation.getByRole('button', { name: '确认删除', exact: true })).toBeDisabled()
    await confirmation.getByLabel('确认 Topic 名称', { exact: true }).fill(managed)
    const deletion = page.waitForResponse(r => r.url().endsWith(`/topics/${managed}`) && r.request().method() === 'DELETE')
    await confirmation.getByRole('button', { name: '确认删除', exact: true }).click()
    const removal = await deletion
    deleted = removal.status() === 204
    expect(removal.status()).toBe(204)
    await expect(confirmation).not.toBeVisible()
    await expect(row).toHaveCount(0)
    await expect(page.getByRole('button', { name: `展开 ${topic}`, exact: true })).toBeVisible()
  } finally {
    // Only this test's acknowledged creation can be deleted by cleanup. A new
    // short-lived session avoids reusing an expired browser lease after failure.
    if (created && !deleted) {
      const opened = await request.post(`/api/clusters/${clusterId}/sessions`)
      expect(opened.status()).toBe(201)
      const { sessionId } = await opened.json()
      try {
        const removed = await request.delete(`/api/sessions/${sessionId}/topics/${managed}`, { data: { confirmationName: managed } })
        expect([204, 404]).toContain(removed.status())
      } finally {
        expect((await request.delete(`/api/sessions/${sessionId}`)).status()).toBe(204)
      }
    }
    expect((await request.delete(`/api/clusters/${clusterId}`)).status()).toBe(204)
  }
})

// Uses real HTTP validation/storage paths; never sends these credentials to Kafka.
test('API hides fixture secrets and rejects oversized bodies', async ({ request }) => {
  const response = await request.post('/api/clusters', { data: { name: 'E2E secret', bootstrapServers: process.env.E2E_KAFKA_BROKERS,
    securityProtocol: 'SASL_PLAINTEXT', saslMechanism: 'PLAIN', username: 'fixture', password: secret } })
  expect(response.status()).toBe(201)
  expect((await response.text()).includes(secret)).toBe(false)
  const { id } = await response.json()
  expect((await (await request.get(`/api/clusters/${id}`)).text()).includes(secret)).toBe(false)
  expect((await (await request.get('/api/clusters')).text()).includes(secret)).toBe(false)
  const invalid = await request.post('/api/clusters', { data: { password: secret, securityProtocol: secret } })
  expect(invalid.status()).toBe(422)
  expect((await invalid.text()).includes(secret)).toBe(false)
  const tooLarge = await request.post('/api/clusters', { data: { password: secret, name: 'x'.repeat(300_000) } })
  expect(tooLarge.status()).toBe(413)
  expect((await tooLarge.text()).includes(secret)).toBe(false)
  expect((await request.delete(`/api/clusters/${id}`)).status()).toBe(204)
})

// Only collection timestamps are accelerated; all sampled metrics come from real Kafka.
test('browser drops samples older than one hour and pauses hidden-page requests', async ({ page, request }) => {
  await page.goto('/settings')
  const id = await createCluster(page, 'E2E history')
  let sample = 0
  const start = Date.parse('2026-01-01T00:00:00Z')
  await page.route('**/api/sessions/*/snapshot', async route => {
    const response = await route.fetch()
    if (!response.ok()) { await route.fulfill({ response }); return }
    const json = await response.json()
    json.collectedAt = new Date(start + sample++ * 1_801_000).toISOString()
    await route.fulfill({ response, json })
  })
  const opened = page.waitForResponse(r => r.url().endsWith(`/api/clusters/${id}/sessions`) && r.status() === 201)
  await page.getByRole('link', { name: '概览', exact: true }).click()
  const { sessionId } = await (await opened).json()
  await expect(page.getByRole('button', { name: '手动刷新' })).toBeEnabled()
  // Assert actual loaded layout, not just stylesheet text: cards and chart panels stay side-by-side.
  const cards = page.locator('.overview-metrics > section')
  await expect(cards).toHaveCount(4)
  const firstCard = await cards.first().boundingBox()
  const lastCard = await cards.last().boundingBox()
  expect(firstCard).not.toBeNull()
  expect(lastCard!.y).toBeCloseTo(firstCard!.y, 0)
  expect(lastCard!.x).toBeGreaterThan(firstCard!.x + firstCard!.width)
  const chart = await page.getByRole('img', { name: '总 Lag 折线图' }).boundingBox()
  const distribution = await page.getByRole('region', { name: 'Lag 分布' }).boundingBox()
  expect(distribution!.x).toBeGreaterThan(chart!.x + chart!.width)
  await expect(page.locator('.topbar').getByRole('button', { name: '手动刷新' })).toBeVisible()
  await page.getByText('查看图表数据', { exact: true }).click()
  const rows = page.getByRole('table', { name: '最近 1 小时总 Lag 数据' }).locator('tbody tr')
  await expect(rows).toHaveCount(1)
  for (let i = 0; i < 2; i++) {
    const collected = page.waitForResponse(r => r.url().endsWith('/snapshot') && r.status() === 200)
    await page.getByRole('button', { name: '手动刷新' }).click()
    await collected
    await expect(page.getByRole('button', { name: '手动刷新' })).toBeEnabled()
  }
  await expect(rows).toHaveCount(2)
  await expect(rows.locator('time').first()).toHaveAttribute('datetime', '2026-01-01T00:30:01.000Z')
  let polls = 0
  page.on('request', req => { if (req.url().endsWith('/snapshot')) polls++ })
  // Browser visibility events are synthesized because headless tabs stay visible.
  await page.evaluate(() => {
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'hidden' })
    document.dispatchEvent(new Event('visibilitychange'))
  })
  await expect(page.getByText('已暂停（页面不可见）')).toBeVisible()
  await page.waitForTimeout(11_000)
  expect(polls).toBe(0)
  await page.goto('/settings')
  await expect.poll(async () => (await request.get(`/api/sessions/${sessionId}/snapshot`)).status(), { timeout: 5_000 }).toBe(410)
  expect((await request.delete(`/api/clusters/${id}`)).status()).toBe(204)
})

// The runner sends SIGTERM immediately after Playwright; leave real Kafka clients live.
test('graceful shutdown fixture has an active Kafka session', async ({ request }) => {
  const created = await request.post('/api/clusters', { data: {
    name: 'E2E shutdown', bootstrapServers: process.env.E2E_KAFKA_BROKERS, securityProtocol: 'PLAINTEXT',
  } })
  expect(created.status()).toBe(201)
  const { id } = await created.json()
  const session = await request.post(`/api/clusters/${id}/sessions`)
  expect(session.status()).toBe(201)
  const { sessionId } = await session.json()
  const response = await request.get(`/api/sessions/${sessionId}/snapshot`)
  expect(response.status()).toBe(200)
  expect((await response.json()).topics.find((row: { topic: string }) => row.topic === topic).partitions[0].high).toBe(10)
})
