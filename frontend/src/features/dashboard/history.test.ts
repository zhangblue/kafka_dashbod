import { expect, it } from 'vitest'
import { appendSample } from './history'

it('keeps samples in the latest server hour including its boundary', () => {
  const result = appendSample([{ at: 0, value: 1 }, { at: 1, value: 2 }], { at: 3_600_001, value: 3 })
  expect(result).toEqual([{ at: 1, value: 2 }, { at: 3_600_001, value: 3 }])
})

it('does not move retention backwards for out of order samples or duplicate timestamps', () => {
  expect(appendSample([{ at: 4_000_000, value: 1 }], { at: 0, value: 2 })).toEqual([{ at: 4_000_000, value: 1 }])
  expect(appendSample([{ at: 4_000_000, value: 1 }], { at: 4_000_000, value: 2 })).toEqual([{ at: 4_000_000, value: 2 }])
})
