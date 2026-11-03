import assert from 'node:assert/strict'
import { after, before, test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { chromium, expect } from '@playwright/test'
import { createServer } from 'vite'

let server
let browser
let baseUrl

// 布局回归开始时启动隔离前端和浏览器，使用动态端口避免复用已有服务。
before(async () => {
  server = await createServer({ root: fileURLToPath(new URL('..', import.meta.url)), server: { host: '127.0.0.1', port: 0 } })
  await server.listen()
  baseUrl = server.resolvedUrls.local[0]
  browser = await chromium.launch()
})

// 回归结束时关闭浏览器和临时服务，避免失败场景留下后台进程。
after(async () => { await browser?.close(); await server?.close() })

const cluster = { id: 'layout-cluster', name: '布局测试 Kafka', bootstrapServers: 'fixture:9092', securityProtocol: 'PLAINTEXT',
  connectionTimeoutMs: 10000, defaultNormalLagMax: 999, defaultHighLagMin: 5000, defaultOverLimitSeconds: 30,
  createdAt: '2026-11-03T00:00:00Z', updatedAt: '2026-11-03T00:00:00Z' }

for (const [language, labels] of [
  ['zh-CN', { cluster: '集群', interval: '刷新频率', refresh: '手动刷新', reconnect: '重新连接' }],
  ['en', { cluster: 'Cluster', interval: 'Refresh interval', refresh: 'Manual refresh', reconnect: 'Reconnect' }],
]) {
  for (const status of ['connected', 'error']) {
    // 真实浏览器检查两种语言的连接与断连状态，捕获固定操作宽度挤压集群或控件越界的回归。
    test(`360px ${language} ${status} header controls fit without overlapping`, { timeout: 15000 }, async t => {
      const page = await browser.newPage({ viewport: { width: 360, height: 800 }, locale: language })
      t.after(() => page.close())
      let disconnected = false
      // 所有后端请求只返回内存夹具；手动刷新后按场景返回安全错误，避免接触真实 Kafka。
      await page.route('**/api/**', async route => {
        const path = new URL(route.request().url()).pathname
        if (!path.startsWith('/api/')) return route.continue()
        if (path === '/api/clusters') return route.fulfill({ json: [cluster] })
        if (path.endsWith('/lag-rules')) return route.fulfill({ json: { defaults: { normalMax: 999, highMin: 5000, overLimitSeconds: 30 }, overrides: [] } })
        if (path.endsWith('/sessions')) return route.fulfill({ json: { sessionId: 'layout-session', leaseSeconds: 30 }, status: 201 })
        if (path.endsWith('/snapshot')) return route.fulfill(disconnected
          ? { status: 502, json: { code: 'KAFKA_TIMEOUT', message: 'Fixture unavailable', requestId: 'layout-test' } }
          : { json: { clusterId: cluster.id, collectedAt: new Date().toISOString(), topics: [], consumerGroupIds: [], consumerGroups: [], partialFailures: [] } })
        if (route.request().method() === 'DELETE') return route.fulfill({ status: 204 })
        throw new Error(`Unexpected fixture request: ${path}`)
      })
      await page.goto(new URL('topics', baseUrl).href)
      await expect(page.locator('[data-connection]')).toHaveAttribute('data-connection', 'connected')
      if (status === 'error') {
        disconnected = true
        await page.getByRole('button', { name: labels.refresh, exact: true }).click()
        await expect(page.locator('[data-connection]')).toHaveAttribute('data-connection', 'error')
        await expect(page.getByRole('button', { name: labels.reconnect, exact: true })).toBeEnabled()
      }
      const controls = [
        page.getByRole('combobox', { name: labels.cluster, exact: true }),
        // 刷新区域包含文字和选择器，整体参与边界检查，避免漏掉标签与集群控件的重叠。
        page.getByRole('combobox', { name: labels.interval, exact: true }).locator('..'),
        page.getByRole('button', { name: labels.refresh, exact: true }),
        page.getByRole('button', { name: '中文', exact: true }),
        page.getByRole('button', { name: 'English', exact: true }),
        ...(status === 'error' ? [page.getByRole('button', { name: labels.reconnect, exact: true })] : []),
      ]
      const header = await page.locator('header.topbar').boundingBox()
      const boxes = await Promise.all(controls.map(control => control.boundingBox()))
      const failures = []
      for (const [index, box] of boxes.entries()) {
        if (!box || box.x < 0 || box.x + box.width > 360.5 || box.y < header.y || box.y + box.height > header.y + header.height + 0.5) failures.push(`control ${index} outside header or viewport`)
        if (index === 0 && box?.width < 72) failures.push(`cluster selector is only ${box.width}px wide`)
        for (const previous of boxes.slice(0, index)) {
          if (box && previous && Math.min(box.x + box.width, previous.x + previous.width) - Math.max(box.x, previous.x) > 0.5
            && Math.min(box.y + box.height, previous.y + previous.height) - Math.max(box.y, previous.y) > 0.5) failures.push(`control ${index} overlaps an earlier control`)
        }
      }
      t.diagnostic(JSON.stringify({ language, status, header, boxes }))
      assert.deepEqual(failures, [])
    })
  }
}
