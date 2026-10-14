import fs from 'node:fs/promises'
import { fileURLToPath, pathToFileURL } from 'node:url'

function requireAlive(pid) {
  try { process.kill(pid, 0) } catch { throw new Error('Preview process is not running') }
}

export async function verifyPreview(ready, expectedPid, expectedMarker) {
  if (!Number.isSafeInteger(expectedPid) || expectedPid <= 0 || !expectedMarker
    || ready.pid !== expectedPid || ready.marker !== expectedMarker
    || !/^http:\/\/127\.0\.0\.1:[1-9]\d*$/.test(ready.url)) {
    throw new Error('Preview ownership record mismatch')
  }
  requireAlive(expectedPid)
  const response = await fetch(`${ready.url}/api/health`, { redirect: 'error', signal: AbortSignal.timeout(2_000) })
  if (!response.ok || response.headers.get('x-e2e-instance') !== expectedMarker) {
    throw new Error('Preview endpoint ownership check failed')
  }
  await response.arrayBuffer()
  requireAlive(expectedPid)
  return ready.url
}

async function startPreview(readyPath) {
  const marker = process.env.E2E_RUN_ID
  if (!marker || !readyPath) throw new Error('Missing preview ownership configuration')
  const { preview } = await import('../frontend/node_modules/vite/dist/node/index.js')
  const server = await preview({
    root: fileURLToPath(new URL('../frontend', import.meta.url)),
    preview: { host: '127.0.0.1', port: 0, strictPort: true },
    plugins: [{
      name: 'e2e-instance-ownership',
      configurePreviewServer(server) {
        server.middlewares.use((_request, response, next) => {
          response.setHeader('x-e2e-instance', marker)
          next()
        })
      },
    }],
  })
  const address = server.httpServer.address()
  if (!address || typeof address === 'string') throw new Error('Preview has no bound TCP address')
  const ready = { pid: process.pid, url: `http://127.0.0.1:${address.port}`, marker }
  // Publish atomically, only after this process actually owns the listening socket.
  await fs.writeFile(`${readyPath}.tmp`, JSON.stringify(ready), { mode: 0o600 })
  await fs.rename(`${readyPath}.tmp`, readyPath)
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    if (process.argv[2] === 'start') await startPreview(process.argv[3])
    else if (process.argv[2] === 'verify') {
      const ready = JSON.parse(await fs.readFile(process.argv[3], 'utf8'))
      console.log(await verifyPreview(ready, Number(process.argv[4]), process.env.E2E_RUN_ID))
    } else throw new Error('Unknown preview helper command')
  } catch (error) {
    console.error(error.message)
    process.exitCode = 1
  }
}
