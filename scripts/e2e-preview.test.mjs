import { test } from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import { spawnSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { copyFile, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
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

// Removing the temporary dotenv or launching from the repository must break this boundary test.
for (const existingDotenv of [false, true]) {
  test(`E2E backend uses a private runtime dotenv with repository dotenv ${existingDotenv ? 'present' : 'absent'}`, async t => {
    const directory = await realpath(await mkdtemp(path.join(os.tmpdir(), 'e2e-startup-test-')))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const repository = path.join(directory, 'repository')
    const runtimeParent = path.join(directory, 'runs')
    const commands = path.join(directory, 'commands')
    const observation = path.join(directory, 'backend-startup.json')
    for (const location of ['scripts', 'target/debug']) await mkdir(path.join(repository, location), { recursive: true })
    await mkdir(runtimeParent)
    await mkdir(commands)
    await copyFile(new URL('./test-e2e.sh', import.meta.url), path.join(repository, 'scripts/test-e2e.sh'))
    if (existingDotenv) await writeFile(path.join(repository, '.env'), 'USER_SETTING=keep-this-file\n')
    // These external tools would create a broker and compile code; this test only reaches startup.
    for (const command of ['docker', 'cargo']) {
      await writeFile(path.join(commands, command), '#!/bin/sh\nexit 0\n', { mode: 0o755 })
    }
    await writeFile(path.join(repository, 'target/debug/kafka-dashboard'), `#!${process.execPath}
const fs = require('node:fs')
const path = require('node:path')
const dotenv = path.join(process.cwd(), '.env')
fs.writeFileSync(process.env.E2E_STARTUP_OBSERVATION, JSON.stringify({
  cwd: process.cwd(),
  dotenv: fs.existsSync(dotenv) ? fs.readFileSync(dotenv, 'utf8') : null,
  databaseUrl: process.env.DATABASE_URL,
  bind: process.env.KAFKA_DASHBOARD_BIND,
}))
// Stop before frontend/browser work; the real shell lifecycle must clean up this failed child.
process.exit(66)
`, { mode: 0o755 })
    const result = spawnSync('/bin/bash', [path.join(repository, 'scripts/test-e2e.sh')], {
      cwd: directory,
      encoding: 'utf8',
      timeout: 10000,
      env: {
        ...process.env,
        PATH: `${commands}:${process.env.PATH}`,
        TMPDIR: runtimeParent,
        E2E_STARTUP_OBSERVATION: observation,
      },
    })
    assert.equal(result.error, undefined)
    assert.equal(result.status, 1, result.stderr)
    assert.match(result.stdout, /Backend failed to start/)
    const observed = JSON.parse(await readFile(observation, 'utf8'))
    assert.equal(path.dirname(observed.cwd), runtimeParent, 'backend must run from its temporary directory')
    assert.equal(observed.dotenv, '', 'backend needs a valid dotenv without test secrets')
    assert.equal(observed.databaseUrl, `sqlite://${observed.cwd}/dashboard.db`)
    assert.equal(observed.bind, '127.0.0.1:0')
    assert.equal(existsSync(observed.cwd), false, 'script cleanup must remove its runtime directory')
    if (existingDotenv) assert.equal(await readFile(path.join(repository, '.env'), 'utf8'), 'USER_SETTING=keep-this-file\n')
    else assert.equal(existsSync(path.join(repository, '.env')), false)
  })
}
