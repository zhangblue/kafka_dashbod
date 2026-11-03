import fs from 'node:fs/promises'
import { fileURLToPath, pathToFileURL } from 'node:url'

// 所有权检查前后探测预期 PID 是否存活，避免已退出 preview 的记录被继续使用。
function requireAlive(pid) {
  try { process.kill(pid, 0) } catch { throw new Error('Preview process is not running') }
}

// 浏览器验收入口调用，联合核验记录、活进程及 HTTP 实例标识后才返回可操作地址。
export async function verifyPreview(ready, expectedPid, expectedMarker) {
  if (!Number.isSafeInteger(expectedPid) || expectedPid <= 0 || !expectedMarker
    || ready.pid !== expectedPid || ready.marker !== expectedMarker
    || !/^http:\/\/127\.0\.0\.1:[1-9]\d*$/.test(ready.url)) {
    throw new Error('Preview ownership record mismatch')
  }
  // 先确认活进程再探测端点，读取响应后再次确认，收窄检查期间进程退出的窗口。
  requireAlive(expectedPid)
  const response = await fetch(`${ready.url}/api/health`, { redirect: 'error', signal: AbortSignal.timeout(2_000) })
  if (!response.ok || response.headers.get('x-e2e-instance') !== expectedMarker) {
    throw new Error('Preview endpoint ownership check failed')
  }
  await response.arrayBuffer()
  requireAlive(expectedPid)
  return ready.url
}

// start 子命令创建回环临时端口服务，实际监听后才向 E2E 脚本发布带所有权的就绪记录。
async function startPreview(readyPath) {
  const marker = process.env.E2E_RUN_ID
  if (!marker || !readyPath) throw new Error('Missing preview ownership configuration')
  const { preview } = await import('../frontend/node_modules/vite/dist/node/index.js')
  const server = await preview({
    root: fileURLToPath(new URL('../frontend', import.meta.url)),
    preview: { host: '127.0.0.1', port: 0, strictPort: true },
    plugins: [{
      name: 'e2e-instance-ownership',
      // Vite 装配 preview 时注入本次实例标识，供随后健康探测确认端点所有权。
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
  // 先写私有临时记录再原子重命名，避免 Shell 读到尚未绑定或只写入一半的记录。
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
