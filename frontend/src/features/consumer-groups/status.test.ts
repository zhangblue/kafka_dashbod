import { expect, it } from 'vitest'
import type { ClusterSnapshot } from '../../api/types'
import { snapshotFixture } from '../../test/handlers'
import { advanceLagStates, evaluateLag, initialLagState, resetAllLagStates, resolveLagRule, rowHealth } from './status'

const rule = { normalMax: 999, highMin: 5000, overLimitSeconds: 0 }

// Catches off-by-one range boundaries and applying the high threshold before the duration gate.
it.each([[0, 'normal'], [999, 'normal'], [1000, 'backlog'], [4999, 'backlog'], [5000, 'high']])('classifies %i as %s', (lag, status) => {
  expect(evaluateLag({ lag: Number(lag), elapsedVisibleMs: 0, rule }, initialLagState()).state.status).toBe(status)
})

it('keeps checkpoint lag normal until duration elapses, preserves time across ranges and resets on recovery', () => {
  const custom = { normalMax: 12000, highMin: 30000, overLimitSeconds: 120 }
  let state = evaluateLag({ lag: 13000, elapsedVisibleMs: 60000, rule: custom }, initialLagState()).state
  expect(state.status).toBe('normal')
  state = evaluateLag({ lag: 35000, elapsedVisibleMs: 60000, rule: custom }, state).state
  expect(state.status).toBe('high')
  state = evaluateLag({ lag: 13500, elapsedVisibleMs: 5000, rule: custom }, state).state
  expect(state.status).toBe('backlog')
  state = evaluateLag({ lag: 200, elapsedVisibleMs: 5000, rule: custom }, state).state
  expect(state).toMatchObject({ status: 'normal', overLimitMs: 0 })
})

// Catches unknown data accidentally counted as continuous lag or shown as healthy.
it('prioritizes collection errors then uncommitted offsets and clears the timer immediately', () => {
  const previous = evaluateLag({ lag: 9000, elapsedVisibleMs: 60000, rule }, initialLagState()).state
  expect(evaluateLag({ lag: 9000, elapsedVisibleMs: 60000, rule, collectionError: true, uncommitted: true }, previous).state)
    .toMatchObject({ status: 'error', overLimitMs: 0 })
  expect(evaluateLag({ lag: 9000, elapsedVisibleMs: 60000, rule, uncommitted: true }, previous).state)
    .toMatchObject({ status: 'uncommitted', overLimitMs: 0 })
})

it('uses exact group and topic overrides, otherwise cluster defaults', () => {
  const override = { groupId: 'g', topic: 't', normalMax: 12000, highMin: 30000, overLimitSeconds: 120 }
  const rules = { defaults: rule, overrides: [override] }
  expect(resolveLagRule(rules, 'g', 't')).toEqual({ rule: override, custom: true })
  expect(resolveLagRule(rules, 'g', 't-other')).toEqual({ rule, custom: false })
  expect(resolveLagRule(rules, 'g-other', 't')).toEqual({ rule, custom: false })
})

const sample = (seconds: number, lag = 6000): ClusterSnapshot => ({ ...snapshotFixture,
  collectedAt: new Date(seconds * 1000).toISOString(), consumerGroups: [{ groupId: 'g', topic: 't', lag,
    incomplete: false, unknownPartitionCount: 0, partitions: [{ partition: 0, high: 10000, committedOffset: 10000 - lag, lag }] }] })
const rules = { defaults: { ...rule, overLimitSeconds: 30 }, overrides: [] }

it('only advances on distinct connected samples; resets continuity on pause, rebuild and cluster switch', () => {
  let book = resetAllLagStates()
  const step = (seconds: number, status = 'connected', continuityKey = 'a', snapshot = sample(seconds)) => {
    book = advanceLagStates({ snapshot, status, continuityKey, rules }, book)
    return Object.values(book.rows)[0]
  }
  expect(step(0).state.overLimitMs).toBe(0)
  expect(step(20).state.status).toBe('normal')
  expect(step(20).state.overLimitMs).toBe(20000)
  expect(step(30).state.status).toBe('high')
  step(30, 'paused')
  expect(step(100).state.overLimitMs).toBe(0)
  expect(step(120).state.status).toBe('normal')
  expect(step(130, 'connected', 'rebuild').state.overLimitMs).toBe(0)
  expect(step(150, 'connected', 'rebuild').state.overLimitMs).toBe(20000)
  expect(step(160, 'connected', 'rebuild', { ...sample(160), clusterId: 'b' }).state.overLimitMs).toBe(0)
})

it('does not count the interval before the first over-limit sample, after errors, or after a missing row', () => {
  let book = resetAllLagStates()
  const step = (snapshot: ClusterSnapshot) => {
    book = advanceLagStates({ snapshot, status: 'connected', continuityKey: 'a', rules }, book)
    return Object.values(book.rows)[0]
  }
  step(sample(0, 0))
  expect(step(sample(60)).state.overLimitMs).toBe(0)
  const broken = sample(90)
  broken.consumerGroups[0].incomplete = true
  expect(step(broken).state.status).toBe('error')
  expect(step(sample(120)).state.overLimitMs).toBe(0)
  step({ ...sample(150), consumerGroups: [] })
  expect(step(sample(180)).state.overLimitMs).toBe(0)
})

it('distinguishes genuine uncommitted offsets from failed collection and missing metadata', () => {
  const snapshot = sample(0)
  const row = { ...snapshot.consumerGroups[0], incomplete: true, unknownPartitionCount: 1,
    partitions: [{ partition: 0, high: 10, committedOffset: null, lag: null }] }
  expect(rowHealth(row, snapshot)).toMatchObject({ collectionError: false, uncommitted: true })
  expect(rowHealth(row, { ...snapshot, partialFailures: [{ scope: 'committed', resource: 'g/t:0', code: 'TIMEOUT', message: '' }] }).collectionError).toBe(true)
  expect(rowHealth(row, { ...snapshot, partialFailures: [{ scope: 'committed', resource: 'other', code: 'TIMEOUT', message: '' }] }).collectionError).toBe(false)
  expect(rowHealth({ ...row, unknownPartitionCount: 2 }, snapshot).collectionError).toBe(true)
  expect(rowHealth({ ...row, partitions: [{ partition: 0, high: null, committedOffset: null, lag: null }] }, snapshot).collectionError).toBe(true)
})

// Catches reusing the retained snapshot as the baseline on visibility resume or session rebuild.
it.each(['pause', 'rebuild'])('does not count hidden time when %s exposes the retained snapshot before a fresh collection', mode => {
  let book = advanceLagStates({ snapshot: sample(0), status: 'connected', continuityKey: 'a', rules }, resetAllLagStates())
  if (mode === 'pause') book = advanceLagStates({ snapshot: sample(0), status: 'paused', continuityKey: 'a', rules }, book)
  const continuityKey = mode === 'pause' ? 'a' : 'b'
  book = advanceLagStates({ snapshot: sample(0), status: 'connected', continuityKey, rules }, book)
  book = advanceLagStates({ snapshot: sample(600), status: 'connected', continuityKey, rules }, book)
  expect(Object.values(book.rows)[0].state).toMatchObject({ status: 'normal', overLimitMs: 0 })
})
