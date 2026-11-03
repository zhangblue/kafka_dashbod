import { expect, it } from 'vitest'
import { appendSample } from './history'

// 验证一小时窗口以最新服务端时间为准，精确保留边界样本。
it('keeps samples in the latest server hour including its boundary', () => {
  const result = appendSample([{ at: 0, value: 1 }, { at: 1, value: 2 }], { at: 3_600_001, value: 3 })
  expect(result).toEqual([{ at: 1, value: 2 }, { at: 3_600_001, value: 3 }])
})

// 验证乱序样本不能倒退保留窗口，相同时间戳只保留最新值。
it('does not move retention backwards for out of order samples or duplicate timestamps', () => {
  expect(appendSample([{ at: 4_000_000, value: 1 }], { at: 0, value: 2 })).toEqual([{ at: 4_000_000, value: 1 }])
  expect(appendSample([{ at: 4_000_000, value: 1 }], { at: 4_000_000, value: 2 })).toEqual([{ at: 4_000_000, value: 2 }])
})
