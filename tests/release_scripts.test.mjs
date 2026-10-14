import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync } from 'node:fs';
import { mkdtemp, mkdir, copyFile, writeFile, readFile, readdir, rm, rename, stat, realpath } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const key = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=';
async function executable(file, content) {
  await writeFile(file, content, { mode: 0o755 });
}
async function fixture(t, osName = 'Darwin', arch = 'arm64') {
  const temp = await realpath(await mkdtemp(path.join(os.tmpdir(), 'release-test-')));
  t.after(() => rm(temp, { recursive: true, force: true }));
  const release = path.join(temp, 'movable release');
  const commands = path.join(temp, 'commands');
  await mkdir(path.join(release, 'bin'), { recursive: true });
  await mkdir(path.join(release, 'web'));
  await mkdir(commands);
  await copyFile(path.join(root, 'packaging/start.sh'), path.join(release, 'start.sh'));
  await executable(path.join(commands, 'uname'), `#!/bin/sh\ncase "$1" in -s) echo '${osName}';; -m) echo '${arch}';; esac\n`);
  for (const platform of ['darwin-arm64', 'linux-amd64']) {
    await executable(path.join(release, 'bin', `kafka-dashboard-${platform}`), `#!/bin/sh\nprintf '%s\\n' '${platform}' "$DATABASE_URL" "$KAFKA_DASHBOARD_WEB_DIR" "$@"\n` + 'exit "${FAKE_EXIT:-0}"\n');
  }
  return { release, temp, run(args = [], env = {}, location = release) {
    const cleanEnv = { ...process.env };
    delete cleanEnv.DATABASE_URL;
    delete cleanEnv.KAFKA_DASHBOARD_WEB_DIR;
    return spawnSync('/bin/sh', [path.join(location, 'start.sh'), ...args], {
      cwd: temp, encoding: 'utf8', env: { ...cleanEnv, PATH: `${commands}:${process.env.PATH}`, KAFKA_DASHBOARD_MASTER_KEY: key, ...env },
    });
  } };
}

// Detect selecting the wrong binary or losing argument boundaries.
for (const [osName, arch, platform] of [['Darwin', 'arm64', 'darwin-arm64'], ['Linux', 'x86_64', 'linux-amd64']]) {
  test(`${osName}/${arch} selects its binary and preserves argv`, async t => {
    const f = await fixture(t, osName, arch);
    const result = f.run(['--bind', '127.0.0.1:9090', 'argument with spaces', '']);
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual(result.stdout.split('\n'), [platform, 'sqlite://data/kafka-dashboard.db?mode=rwc', `${f.release}/web`, '--bind', '127.0.0.1:9090', 'argument with spaces', '', '']);
    assert.equal((await stat(path.join(f.release, 'data'))).isDirectory(), true);
  });
}
test('missing master key refuses startup without loading .env', async t => {
  const f = await fixture(t);
  await writeFile(path.join(f.release, '.env'), `KAFKA_DASHBOARD_MASTER_KEY=${key}\n`);
  const result = f.run([], { KAFKA_DASHBOARD_MASTER_KEY: '' });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /KAFKA_DASHBOARD_MASTER_KEY/);
  assert.equal(result.stdout, '');
});
test('unsupported platform reports both supported platforms', async t => {
  const f = await fixture(t, 'Linux', 'aarch64');
  const result = f.run();
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Darwin\/arm64/);
  assert.match(result.stderr, /Linux\/x86_64/);
});
test('moved package resolves paths relative to start.sh and propagates exit status', async t => {
  const f = await fixture(t);
  const moved = path.join(f.temp, 'new location');
  await rename(f.release, moved);
  const result = f.run([], { FAKE_EXIT: '17' }, moved);
  assert.equal(result.status, 17);
  assert.match(result.stdout, new RegExp(`${moved}/web`));
});
test('explicit database and web paths remain configurable', async t => {
  const f = await fixture(t);
  const result = f.run([], { DATABASE_URL: 'sqlite:///custom.db', KAFKA_DASHBOARD_WEB_DIR: '/custom/web' });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(result.stdout.split('\n').slice(1, 3), ['sqlite:///custom.db', '/custom/web']);
});

// Exercise SQLx's real URI parser: literal '?' and '%20' must remain directory names.
const nativeBinary = path.join(root, 'dist/kafka-dashboard/bin/kafka-dashboard-darwin-arm64');
for (const directory of ['release?query', 'release%20encoded', 'release with spaces', '中文发行目录']) {
  test(`real binary starts after relocation to ${directory}`, {
    skip: process.platform !== 'darwin' || process.arch !== 'arm64' || !existsSync(nativeBinary)
      ? 'Requires the built Darwin/arm64 release binary' : false,
  }, async t => {
    const f = await fixture(t);
    await copyFile(nativeBinary, path.join(f.release, 'bin/kafka-dashboard-darwin-arm64'));
    await writeFile(path.join(f.release, 'web/index.html'), '<html>relocated release</html>');
    const moved = path.join(f.temp, directory);
    await rename(f.release, moved);
    const env = { ...process.env, KAFKA_DASHBOARD_MASTER_KEY: key, KAFKA_DASHBOARD_BIND: '127.0.0.1:0', RUST_LOG: 'info', NO_COLOR: '1' };
    delete env.DATABASE_URL;
    delete env.KAFKA_DASHBOARD_WEB_DIR;
    const child = spawn('/bin/sh', [path.join(moved, 'start.sh')], { cwd: f.temp, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    const exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
    try {
      let port;
      for (let attempt = 0; attempt < 100; attempt++) {
        if (child.exitCode !== null || child.signalCode !== null) break;
        port = stdout.match(/address=127\.0\.0\.1:(\d+)/)?.[1];
        if (port) break;
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      assert.ok(port, `server failed to start: ${stderr}`);
      const response = await fetch(`http://127.0.0.1:${port}/api/health`);
      assert.equal(response.status, 200);
      assert.deepEqual(await response.json(), { status: 'ok' });
      const db = await readFile(path.join(moved, 'data/kafka-dashboard.db'));
      assert.equal(db.subarray(0, 16).toString(), 'SQLite format 3\0');
      assert.equal(existsSync(path.join(f.temp, 'data/kafka-dashboard.db')), false);
      child.kill('SIGTERM');
      const result = await Promise.race([exited, new Promise(resolve => setTimeout(() => resolve('timeout'), 5000).unref())]);
      assert.deepEqual(result, { code: 0, signal: null });
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      await exited;
    }
  });
}

async function buildFixture(t, fail = '') {
  const temp = await mkdtemp(path.join(os.tmpdir(), 'release-build-test-'));
  t.after(() => rm(temp, { recursive: true, force: true }));
  for (const dir of ['tools', 'packaging', 'frontend', 'commands', 'dist/kafka-dashboard']) await mkdir(path.join(temp, dir), { recursive: true });
  await copyFile(path.join(root, 'tools/build-release.sh'), path.join(temp, 'tools/build-release.sh'));
  for (const file of ['start.sh', '.env.example', 'README.txt']) await copyFile(path.join(root, 'packaging', file), path.join(temp, 'packaging', file));
  await writeFile(path.join(temp, 'dist/kafka-dashboard/old'), 'previous release');
  await writeFile(path.join(temp, 'dist/kafka-dashboard.tar.gz'), 'previous archive');
  await executable(path.join(temp, 'commands/uname'), '#!/bin/sh\ncase "$1" in -s) echo Darwin;; -m) echo arm64;; esac\n');
  // Only external compilers are faked: package assembly, hashing, and archive are real.
  await executable(path.join(temp, 'commands/npm'), '#!/bin/sh\n[ "$FAIL_STEP" != npm ] || exit 12\nmkdir -p frontend/dist/assets\nprintf "<html>release</html>" > frontend/dist/index.html\nprintf "asset" > frontend/dist/assets/app.js\n');
  await executable(path.join(temp, 'commands/cargo'), '#!/bin/sh\n[ "$FAIL_STEP" != cargo ] || exit 13\nmkdir -p target/aarch64-apple-darwin/release\nprintf "mac binary" > target/aarch64-apple-darwin/release/kafka-dashboard\n');
  await executable(path.join(temp, 'commands/docker'), '#!/bin/sh\n[ "$FAIL_STEP" != docker ] || exit 14\nfor arg do\ncase "$arg" in type=local,dest=*) dest=${arg#type=local,dest=}; mkdir -p "$dest"; printf "linux binary" > "$dest/kafka-dashboard-linux-amd64";; esac\ndone\n');
  return { temp, run() { return spawnSync('/bin/sh', [path.join(temp, 'tools/build-release.sh')], { cwd: temp, encoding: 'utf8', env: { ...process.env, PATH: `${temp}/commands:${process.env.PATH}`, FAIL_STEP: fail } }); } };
}
test('build assembles a complete checksummed archive without stale files', async t => {
  const f = await buildFixture(t);
  const result = f.run();
  assert.equal(result.status, 0, result.stderr);
  const release = path.join(f.temp, 'dist/kafka-dashboard');
  assert.deepEqual((await readdir(release)).sort(), ['.env.example', 'README.txt', 'SHA256SUMS', 'bin', 'start.sh', 'web']);
  assert.equal(spawnSync('shasum', ['-a', '256', '-c', 'SHA256SUMS'], { cwd: release }).status, 0);
  assert.equal((await stat(path.join(release, 'bin/kafka-dashboard-linux-amd64'))).mode & 0o111, 0o111);
  const archive = spawnSync('tar', ['-tzf', path.join(f.temp, 'dist/kafka-dashboard.tar.gz')], { encoding: 'utf8' });
  assert.equal(archive.status, 0);
  assert.match(archive.stdout, /kafka-dashboard\/web\/index.html/);
  assert.doesNotMatch(archive.stdout, /\/old/);
});
for (const step of ['npm', 'cargo', 'docker']) {
  test(`${step} failure preserves prior release and archive`, async t => {
    const f = await buildFixture(t, step);
    assert.notEqual(f.run().status, 0);
    assert.equal(await readFile(path.join(f.temp, 'dist/kafka-dashboard/old'), 'utf8'), 'previous release');
    assert.equal(await readFile(path.join(f.temp, 'dist/kafka-dashboard.tar.gz'), 'utf8'), 'previous archive');
    assert.deepEqual((await readdir(path.join(f.temp, 'dist'))).sort(), ['kafka-dashboard', 'kafka-dashboard.tar.gz']);
  });
}

test('archive publication failure rolls back the directory and prior archive', async t => {
  const f = await buildFixture(t);
  await executable(path.join(f.temp, 'commands/mv'), '#!/bin/sh\ncase "$1" in */.release.*/kafka-dashboard.tar.gz) exit 19;; esac\nexec /bin/mv "$@"\n');
  const result = f.run();
  assert.notEqual(result.status, 0);
  assert.equal(await readFile(path.join(f.temp, 'dist/kafka-dashboard/old'), 'utf8'), 'previous release');
  assert.equal(await readFile(path.join(f.temp, 'dist/kafka-dashboard.tar.gz'), 'utf8'), 'previous archive');
  assert.deepEqual((await readdir(path.join(f.temp, 'dist'))).sort(), ['kafka-dashboard', 'kafka-dashboard.tar.gz']);
});

// Detect skipping artifact hygiene or checksum validation before executing binaries.
for (const [name, file, message] of [
  ['secret file', '.env', /Unexpected release file/],
  ['database', 'data/config.db', /Unexpected release/],
  ['source', 'web/main.ts', /Unexpected release file/],
  ['dependencies', 'web/node_modules/pkg/index.js', /Unexpected release/],
  ['tampering', 'web/index.html', /checksum/i],
]) {
  test(`verification rejects ${name} before running binaries`, async t => {
    const f = await buildFixture(t);
    assert.equal(f.run().status, 0);
    const release = path.join(f.temp, 'dist/kafka-dashboard');
    await mkdir(path.dirname(path.join(release, file)), { recursive: true });
    await writeFile(path.join(release, file), 'unexpected contents');
    const result = spawnSync('/bin/sh', [path.join(root, 'tools/verify-release.sh'), release], { encoding: 'utf8', timeout: 10000 });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, message);
  });
}

test('failed runtime verification reaps even a server that ignores TERM', async t => {
  const f = await buildFixture(t);
  assert.equal(f.run().status, 0);
  const release = path.join(f.temp, 'dist/kafka-dashboard');
  const pidFile = path.join(f.temp, 'server.pid');
  let serverPid;
  t.after(() => {
    try { if (serverPid) process.kill(serverPid, 'SIGKILL'); } catch {}
  });
  await executable(path.join(release, 'bin/kafka-dashboard-darwin-arm64'), `#!${process.execPath}\nif (process.argv.includes('--version')) process.exit(0);\nrequire('node:fs').writeFileSync(process.env.SERVER_PID_FILE, String(process.pid));\nprocess.on('SIGTERM', () => {});\nif (process.env.RUST_LOG === 'info') console.log('address=127.0.0.1:12345');\nsetInterval(() => {}, 1000);\n`);
  const manifest = await readFile(path.join(release, 'SHA256SUMS'), 'utf8');
  const digest = createHash('sha256').update(await readFile(path.join(release, 'bin/kafka-dashboard-darwin-arm64'))).digest('hex');
  await writeFile(path.join(release, 'SHA256SUMS'), manifest.replace(/^[a-f0-9]{64}(  \.\/bin\/kafka-dashboard-darwin-arm64)$/m, `${digest}$1`));
  await executable(path.join(f.temp, 'commands/file'), '#!/bin/sh\nprintf "Mach-O 64-bit executable arm64 ELF 64-bit x86-64 statically linked\\n"\n');
  await executable(path.join(f.temp, 'commands/otool'), '#!/bin/sh\nprintf "binary:\\n"\n');
  await executable(path.join(f.temp, 'commands/docker'), '#!/bin/sh\nexit 0\n');
  await executable(path.join(f.temp, 'commands/curl'), '#!/bin/sh\nexit 1\n');
  const result = spawnSync('/bin/sh', [path.join(root, 'tools/verify-release.sh'), release], {
    encoding: 'utf8', timeout: 30000,
    env: { ...process.env, PATH: `${f.temp}/commands:${process.env.PATH}`, SERVER_PID_FILE: pidFile, RUST_LOG: 'error' },
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Health timeout/);
  const pid = Number(await readFile(pidFile, 'utf8'));
  serverPid = pid;
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
});
