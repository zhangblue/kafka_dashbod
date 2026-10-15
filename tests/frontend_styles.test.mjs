import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import path from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')

test('dashboard stylesheet covers the core production controls', async () => {
  const stylesheet = await readFile(path.join(root, 'frontend/src/index.css'), 'utf8')
  assert.match(stylesheet, /--bg:\s*#070b14/)
  assert.match(stylesheet, /\.app-shell\s*\{/)
  assert.match(stylesheet, /\.sidebar\s*\{/)
  assert.match(stylesheet, /\.topbar\s*\{/)
  assert.match(stylesheet, /\.panel,/)
  assert.match(stylesheet, /nav a\.active/)
  assert.match(stylesheet, /button:not\(\[aria-label\$="字段说明"\]\)/)
  assert.match(stylesheet, /input,\s*select,\s*textarea/)
  assert.match(stylesheet, /table\s*\{/)
  assert.match(stylesheet, /\[role="alert"\]/)
  assert.match(stylesheet, /@media \(max-width:/)
})
