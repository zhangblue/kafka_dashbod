import { fireEvent, render, screen } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import App from './App'
import { clustersApi, lagRulesApi, sessionsApi } from '../api/client'
import { clusterFixture, rulesFixture, snapshotFixture } from '../test/handlers'

afterEach(() => { vi.restoreAllMocks(); window.history.replaceState({}, '', '/') })

it('renders the dashboard navigation', () => {
  vi.spyOn(clustersApi, 'list').mockResolvedValue([])
  render(<App />)
  expect(screen.getByText('Kafka Lens')).toBeInTheDocument()
  expect(screen.getByRole('navigation')).toBeInTheDocument()
  expect(screen.getByText('概览')).toBeInTheDocument()
  expect(screen.getByText('Topics')).toBeInTheDocument()
  expect(screen.getByText('Consumer Groups')).toBeInTheDocument()
  expect(screen.getByText('集群设置')).toBeInTheDocument()
})

it('opens settings without retaining a dashboard session and supports direct settings entry', async () => {
  const closed: string[] = []
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'monitor', leaseSeconds: 30 })
  vi.spyOn(sessionsApi, 'snapshot').mockResolvedValue(snapshotFixture)
  vi.spyOn(sessionsApi, 'close').mockImplementation(async id => { closed.push(id) })
  const first = render(<App />)
  await screen.findByText('已连接')
  fireEvent.click(screen.getByRole('link', { name: '集群设置' }))
  expect(await screen.findByRole('button', { name: '新增集群' })).toBeInTheDocument()
  expect(closed).toEqual(['monitor'])
  first.unmount()
  vi.mocked(sessionsApi.create).mockClear()
  render(<App />)
  expect(await screen.findByRole('button', { name: '新增集群' })).toBeInTheDocument()
  expect(sessionsApi.create).not.toHaveBeenCalled()
})

it('opens the Consumer Groups route with real snapshot rows', async () => {
  window.history.replaceState({}, '', '/consumer-groups')
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(lagRulesApi, 'get').mockResolvedValue(rulesFixture)
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'groups', leaseSeconds: 30 })
  vi.spyOn(sessionsApi, 'snapshot').mockResolvedValue(snapshotFixture)
  vi.spyOn(sessionsApi, 'close').mockResolvedValue(undefined)
  render(<App />)
  expect(await screen.findByRole('table', { name: 'Consumer Groups 汇总' })).toBeInTheDocument()
  expect(screen.getByText('audit-worker')).toBeInTheDocument()
})
