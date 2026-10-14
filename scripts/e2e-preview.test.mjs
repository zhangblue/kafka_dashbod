import { test } from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import { verifyPreview } from './e2e-preview.mjs'

// The wrong process can return a healthy API response on the expected port.
test('rejects a healthy unrelated service before any mutating request', async t => {
  const requests = []
  const server = http.createServer((req, res) => {
    requests.push(req.method)
    res.writeHead(200, { 'content-type': 'application/json' }).end('{"status":"ok"}')
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise(resolve => server.close(resolve)))
  const ready = { pid: process.pid, url: `http://127.0.0.1:${server.address().port}`, marker: 'this-run' }
  await assert.rejects(verifyPreview(ready, process.pid, 'this-run'), /ownership/)
  assert.deepEqual(requests, ['GET'])
})

test('accepts only the matching live process and instance marker', async t => {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'x-e2e-instance': 'this-run' }).end('{"status":"ok"}')
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise(resolve => server.close(resolve)))
  const ready = { pid: process.pid, url: `http://127.0.0.1:${server.address().port}`, marker: 'this-run' }
  assert.equal(await verifyPreview(ready, process.pid, 'this-run'), ready.url)
  await assert.rejects(verifyPreview({ ...ready, pid: process.pid + 1 }, process.pid, 'this-run'), /ownership/)
  await assert.rejects(verifyPreview(ready, process.pid, 'other-run'), /ownership/)
  await assert.rejects(verifyPreview({ ...ready, pid: 2147483647 }, 2147483647, 'this-run'), /not running/)
})
