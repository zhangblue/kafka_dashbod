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
// Startup includes a 1s liveness wait plus process/spawn overhead. Shutdown can
// wait 10s, and restart adds another startup; keep bounded outer headroom.
const launcherTimeoutMs = 10_000;
const lifecycleTimeoutMs = 30_000;
// These fixtures make curl return immediately: listener/health polling and stop
// can each take 10s, reaping 2s, startup 1s. Allow subprocess scheduling overhead.
const runtimeVerificationTimeoutMs = 60_000;
async function executable(file, content) {
  await writeFile(file, content, { mode: 0o755 });
}
async function fixture(t, osName = 'Darwin', arch = 'arm64') {
  const temp = await realpath(await mkdtemp(path.join(os.tmpdir(), 'release-test-')));
  const startedPids = path.join(temp, 'started-pids');
  const events = path.join(temp, 'service-events');
  const readyFile = path.join(temp, 'service-ready');
  const launcherPids = new Set();
  t.after(async () => {
    if (existsSync(startedPids)) {
      for (const pid of (await readFile(startedPids, 'utf8')).trim().split('\n').map(Number).filter(pid => Number.isSafeInteger(pid) && pid > 0)) {
        launcherPids.add(pid);
      }
    }
    for (const pid of launcherPids) {
      try { process.kill(pid, 'SIGKILL'); } catch {}
    }
    for (const pid of launcherPids) {
      for (let attempt = 0; attempt < 100 && isRunning(pid); attempt++) {
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      assert.equal(isRunning(pid), false, `fixture leaked service PID ${pid}`);
    }
    await rm(temp, { recursive: true, force: true });
  });
  const release = path.join(temp, 'movable release');
  const commands = path.join(temp, 'commands');
  await mkdir(path.join(release, 'bin'), { recursive: true });
  await mkdir(path.join(release, 'web'));
  await mkdir(commands);
  await copyFile(path.join(root, 'packaging/start.sh'), path.join(release, 'start.sh'));
  await executable(path.join(commands, 'uname'), `#!/bin/sh\ncase "$1" in -s) echo '${osName}';; -m) echo '${arch}';; esac\n`);
  for (const platform of ['darwin-arm64', 'linux-amd64']) {
    await executable(path.join(release, 'bin', `kafka-dashboard-${platform}`), `#!/bin/sh
[ "\${1-}" != --version ] || { echo 'kafka-dashboard 0.2.0'; exit 0; }
printf '%s\\n' "$$" >> "$FAKE_PID_FILE"
[ -z "\${FAKE_EVENTS_FILE-}" ] || printf 'START:%s\\n' "$$" >> "$FAKE_EVENTS_FILE"
printf '%s\\n' '${platform}' "$PWD" "\${DATABASE_URL-}" "\${KAFKA_DASHBOARD_WEB_DIR-}" "$@"
[ "\${FAKE_EXIT-}" != immediate ] || exit 17
trap '[ -z "\${FAKE_EVENTS_FILE-}" ] || printf "TERM:%s\\n" "$$" >> "$FAKE_EVENTS_FILE"; if [ "\${FAKE_IGNORE_TERM-}" != 1 ]; then sleep 0.2; exit 0; fi' TERM
printf '%s\\n' "$$" > "$FAKE_READY_FILE"
while :; do sleep 1; done
`);
  }
  return { release, temp, commands, events, readyFile, run(args = [], env = {}, location = release) {
    const cleanEnv = { ...process.env };
    delete cleanEnv.DATABASE_URL;
    delete cleanEnv.KAFKA_DASHBOARD_WEB_DIR;
    const result = spawnSync('/bin/sh', [path.join(location, 'start.sh'), ...args], {
      cwd: temp, encoding: 'utf8', timeout: ['stop', 'restart'].includes(args[0]) ? lifecycleTimeoutMs : launcherTimeoutMs,
      env: { ...cleanEnv, PATH: `${commands}:${process.env.PATH}`, FAKE_PID_FILE: startedPids, FAKE_EVENTS_FILE: events, FAKE_READY_FILE: readyFile, ...env },
    });
    const started = result.stdout?.match(/已在后台启动，PID: ([1-9][0-9]*)/);
    if (started) launcherPids.add(Number(started[1]));
    return result;
  } };
}

function isRunning(pid) {
  try { process.kill(pid, 0); return true; } catch (error) {
    if (error.code === 'ESRCH') return false;
    throw error;
  }
}

async function readPid(release) {
  return Number((await readFile(path.join(release, 'data/kafka-dashboard.pid'), 'utf8')).trim());
}
async function serviceReady(f, pid) {
  const deadline = Date.now() + launcherTimeoutMs;
  while (Date.now() < deadline) {
    try {
      if (await readFile(f.readyFile, 'utf8') === `${pid}\n`) return;
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  assert.fail(`fixture PID ${pid} did not install its TERM handler and write startup events`);
}
const logPath = release => path.join(release, 'data/kafka-dashboard.log');
const pidPath = release => path.join(release, 'data/kafka-dashboard.pid');
async function serviceLog(release) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const log = await readFile(logPath(release), 'utf8');
    if (log.includes(`${release}\n`)) return log;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  assert.fail('service did not write its startup log');
}

// Detect foreground blocking, injected configuration, and a wrong working directory or binary.
for (const [osName, arch, platform] of [['Darwin', 'arm64', 'darwin-arm64'], ['Linux', 'x86_64', 'linux-amd64']]) {
  test(`${osName}/${arch} background start selects its binary and working directory`, async t => {
    const f = await fixture(t, osName, arch);
    const result = f.run(['start']);
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.error, undefined, 'start should return without waiting for service exit');
    const pid = await readPid(f.release);
    assert.ok(Number.isSafeInteger(pid) && pid > 0);
    process.kill(pid, 0);
    assert.match(result.stdout, new RegExp(`Kafka Lens 已在后台启动，PID: ${pid}`));
    assert.deepEqual((await serviceLog(f.release)).split('\n'), [platform, f.release, '', '', '']);
    assert.equal((await stat(path.join(f.release, 'data'))).isDirectory(), true);
    for (const file of ['data', 'data/kafka-dashboard.pid', 'data/kafka-dashboard.log']) {
      assert.equal((await stat(path.join(f.release, file))).mode & 0o077, 0, file);
    }
  });
}
test('default command starts in background without reading an env file', async t => {
  const f = await fixture(t);
  const result = f.run();
  assert.equal(result.status, 0, result.stderr);
  process.kill(await readPid(f.release), 0);
  assert.match(result.stdout, /Kafka Lens 已在后台启动，PID: [1-9][0-9]*/);
  assert.equal((await serviceLog(f.release)).split('\n')[0], 'darwin-arm64');
});
test('unsupported platform reports both supported platforms', async t => {
  const f = await fixture(t, 'Linux', 'aarch64');
  const result = f.run();
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Darwin\/arm64/);
  assert.match(result.stderr, /Linux\/x86_64/);
});
test('moved package background startup resolves its working directory', async t => {
  const f = await fixture(t);
  const moved = path.join(f.temp, 'new location');
  await rename(f.release, moved);
  const result = f.run([], {}, moved);
  assert.equal(result.status, 0, result.stderr);
  assert.equal((await serviceLog(moved)).split('\n')[1], moved);
});
test('background startup preserves explicit database and web paths', async t => {
  const f = await fixture(t);
  const result = f.run([], { DATABASE_URL: 'sqlite:///custom.db', KAFKA_DASHBOARD_WEB_DIR: '/custom/web' });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual((await serviceLog(f.release)).split('\n').slice(2, 4), ['sqlite:///custom.db', '/custom/web']);
});
test('C locale manages the complete lifecycle in a Chinese release directory', async t => {
  const f = await fixture(t);
  const moved = path.join(f.temp, '中文 movable release');
  await rename(f.release, moved);
  const env = { LC_ALL: 'C' };
  assert.equal(f.run(['start'], env, moved).status, 0);
  const pid = await readPid(moved);
  const running = f.run(['status'], env, moved);
  assert.equal(running.status, 0, running.stderr);
  assert.equal(running.stdout.trim(), `Kafka Lens 正在运行，PID: ${pid}`);
  const stopped = f.run(['stop'], env, moved);
  assert.equal(stopped.status, 0, stopped.stderr);
  assert.equal(isRunning(pid), false);
  assert.equal(existsSync(pidPath(moved)), false);
  const status = f.run(['status'], env, moved);
  assert.equal(status.status, 0, status.stderr);
  assert.equal(status.stdout.trim(), 'Kafka Lens 未运行');
});

test('immediate startup failure removes pid and points to the log', async t => {
  const f = await fixture(t);
  const result = f.run([], { FAKE_EXIT: 'immediate' });
  assert.notEqual(result.status, 0);
  assert.equal(existsSync(pidPath(f.release)), false);
  assert.match(result.stderr, /启动失败.*日志/);
});
test('duplicate background startup keeps the existing pid and log', async t => {
  const f = await fixture(t);
  assert.equal(f.run().status, 0);
  const pid = await readPid(f.release);
  const log = await serviceLog(f.release);
  const result = f.run(['start']);
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}${result.stderr}`, new RegExp(`Kafka Lens 已在运行，PID: ${pid}`));
  assert.equal(await readPid(f.release), pid);
  assert.equal(await readFile(logPath(f.release), 'utf8'), log);
});
test('background startup uses an append-only log', async t => {
  const f = await fixture(t);
  await mkdir(path.join(f.release, 'data'));
  await writeFile(logPath(f.release), 'previous run\n');
  assert.equal(f.run().status, 0);
  assert.match(await serviceLog(f.release), /^previous run\ndarwin-arm64\n/);
});

test('status describes both running and stopped states with exit code zero', async t => {
  const f = await fixture(t);
  const stopped = f.run(['status']);
  assert.equal(stopped.status, 0, stopped.stderr);
  assert.match(stopped.stdout, /^Kafka Lens 未运行/m);
  assert.equal(f.run(['start']).status, 0);
  const pid = await readPid(f.release);
  const running = f.run(['status']);
  assert.equal(running.status, 0, running.stderr);
  assert.match(running.stdout, new RegExp(`Kafka Lens 正在运行，PID: ${pid}`));
});
test('status clears a stale pid and reports stopped', async t => {
  const f = await fixture(t);
  const child = spawn('/bin/sh', ['-c', 'exit 0']);
  await new Promise(resolve => child.once('exit', resolve));
  await mkdir(path.join(f.release, 'data'));
  await writeFile(pidPath(f.release), `${child.pid}\n`);
  const result = f.run(['status']);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /^Kafka Lens 未运行/m);
  assert.equal(existsSync(pidPath(f.release)), false);
});

// Catch early success, repeated signals, forced kills, and starting before a successful stop.
test('stop waits for the matching service to exit and removes its pid file', async t => {
  const f = await fixture(t);
  assert.equal(f.run(['start']).status, 0);
  const pid = await readPid(f.release);
  await serviceReady(f, pid);
  const stopped = f.run(['stop']);
  assert.equal(stopped.status, 0, stopped.stderr);
  assert.equal(stopped.error, undefined);
  assert.match(stopped.stdout, /Kafka Lens 已停止/);
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
  assert.equal(existsSync(pidPath(f.release)), false);
  assert.equal(await readFile(f.events, 'utf8'), `START:${pid}\nTERM:${pid}\n`);
});
for (const stale of [false, true]) {
  test(`stop succeeds when ${stale ? 'a stale pid is present' : 'the service has not started'}`, async t => {
    const f = await fixture(t);
    if (stale) {
      const child = spawn('/bin/sh', ['-c', 'exit 0']);
      await new Promise(resolve => child.once('exit', resolve));
      await mkdir(path.join(f.release, 'data'));
      await writeFile(pidPath(f.release), `${child.pid}\n`);
    }
    const stopped = f.run(['stop']);
    assert.equal(stopped.status, 0, stopped.stderr);
    assert.match(stopped.stdout, /Kafka Lens 未运行/);
    assert.equal(existsSync(pidPath(f.release)), false);
    assert.equal(existsSync(f.events), false);
  });
}
for (const command of ['stop', 'restart']) {
  test(`${command} timeout sends TERM once, preserves the live pid, and never starts again`, async t => {
    const f = await fixture(t);
    assert.equal(f.run(['start'], { FAKE_IGNORE_TERM: '1' }).status, 0);
    const pid = await readPid(f.release);
    await serviceReady(f, pid);
    const began = Date.now();
    const result = f.run([command]);
    assert.equal(result.error, undefined, 'launcher must finish its own bounded wait');
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /在等待时间内未退出，PID 文件已保留/);
    assert.ok(Date.now() - began >= 9000, 'stop must allow the configured graceful shutdown wait');
    process.kill(pid, 0);
    assert.equal(await readPid(f.release), pid);
    assert.equal(await readFile(f.events, 'utf8'), `START:${pid}\nTERM:${pid}\n`);
    assert.equal(existsSync(path.join(f.release, 'data/kafka-dashboard.lock')), false);
  });
}
test('restart waits for old TERM and exit before starting a new service', async t => {
  const f = await fixture(t);
  assert.equal(f.run(['start']).status, 0);
  const oldPid = await readPid(f.release);
  await serviceReady(f, oldPid);
  const result = f.run(['restart']);
  assert.equal(result.status, 0, result.stderr);
  const newPid = await readPid(f.release);
  await serviceReady(f, newPid);
  assert.notEqual(newPid, oldPid);
  assert.throws(() => process.kill(oldPid, 0), { code: 'ESRCH' });
  process.kill(newPid, 0);
  assert.match(result.stdout, /Kafka Lens 已停止/);
  assert.match(result.stdout, new RegExp(`Kafka Lens 已在后台启动，PID: ${newPid}`));
  assert.equal(await readFile(f.events, 'utf8'), `START:${oldPid}\nTERM:${oldPid}\nSTART:${newPid}\n`);
});
test('restart preserves the pid and never starts again when its shutdown wait fails', async t => {
  const f = await fixture(t);
  assert.equal(f.run(['start']).status, 0);
  const pid = await readPid(f.release);
  await serviceReady(f, pid);
  // Let the old service exit before the failed wait returns, exercising the
  // tempting success path on the following process check.
  await executable(path.join(f.commands, 'sleep'), `#!/bin/sh
if [ "$1" = 0.1 ]; then
    /bin/sleep 2
    exit 19
fi
exec /bin/sleep "$@"
`);
  const result = f.run(['restart']);
  assert.equal(result.error, undefined);
  assert.notEqual(result.status, 0, 'a failed shutdown wait must abort restart');
  assert.match(result.stderr, /无法等待 Kafka Lens 退出，PID 文件已保留/);
  assert.doesNotMatch(result.stdout, /已停止|已在后台启动/);
  assert.equal(await readPid(f.release), pid);
  assert.equal(await readFile(f.events, 'utf8'), `START:${pid}\nTERM:${pid}\n`);
  assert.equal(existsSync(path.join(f.release, 'data/kafka-dashboard.lock')), false);
});
test('restart starts a service when none is running', async t => {
  const f = await fixture(t);
  const result = f.run(['restart']);
  assert.equal(result.status, 0, result.stderr);
  const pid = await readPid(f.release);
  await serviceReady(f, pid);
  process.kill(pid, 0);
  assert.match(result.stdout, /Kafka Lens 未运行/);
  assert.equal(await readFile(f.events, 'utf8'), `START:${pid}\n`);
});
for (const content of ['not-a-pid\n', '0\n', '-1\n', '12\n34\n', '']) {
  test(`invalid pid ${JSON.stringify(content)} is an explicit script error`, async t => {
    const f = await fixture(t);
    await mkdir(path.join(f.release, 'data'));
    await writeFile(pidPath(f.release), content);
    const result = f.run(['status']);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /PID 文件内容无效/);
    assert.equal(await readFile(pidPath(f.release), 'utf8'), content);
  });
}
test('unreadable pid path is an explicit script error', async t => {
  const f = await fixture(t);
  await mkdir(pidPath(f.release), { recursive: true });
  const result = f.run(['status']);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /无法读取 PID 文件/);
});
test('live pid belonging to another process is rejected without deleting it', async t => {
  const f = await fixture(t);
  await mkdir(path.join(f.release, 'data'));
  await writeFile(pidPath(f.release), `${process.pid}\n`);
  for (const command of ['status', 'start', 'stop', 'restart']) {
    const result = f.run([command]);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /进程身份不匹配/);
    assert.equal(await readPid(f.release), process.pid);
  }
});
for (const kind of ['similar executable path', 'unrelated argument containing binary path']) {
  test(`process identity rejects ${kind}`, async t => {
    const f = await fixture(t);
    const binary = path.join(f.release, 'bin/kafka-dashboard-darwin-arm64');
    let child;
    if (kind === 'similar executable path') {
      await copyFile(binary, `${binary}.old`);
      child = spawn('/bin/sh', [`${binary}.old`], {
        env: { ...process.env, FAKE_PID_FILE: path.join(f.temp, 'started-pids') }, stdio: 'ignore',
      });
    } else {
      child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)', binary], { stdio: 'ignore' });
    }
    const exited = new Promise(resolve => child.once('exit', resolve));
    t.after(async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      await exited;
    });
    await new Promise(resolve => child.once('spawn', resolve));
    await mkdir(path.join(f.release, 'data'));
    await writeFile(pidPath(f.release), `${child.pid}\n`);
    for (const command of ['status', 'start', 'stop', 'restart']) {
      const result = f.run([command]);
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, /进程身份不匹配/);
      assert.equal(await readPid(f.release), child.pid);
      process.kill(child.pid, 0);
    }
  });
}
for (const reopenScript of [false, true]) {
  test(`process identity rejects a script path and argument that concatenate to the binary path${reopenScript ? ' with a reopened script descriptor' : ''}`, async t => {
    const f = await fixture(t);
    const prefixScript = path.join(f.temp, 'movable');
    await executable(prefixScript, `#!/bin/sh
{
  ${reopenScript ? 'exec 255<"$EXPECTED_SCRIPT"' : ':'}
  printf ready
  trap 'exit 0' TERM
  while :; do sleep 0.1; done
}
`);
    const child = spawn('/bin/sh', [prefixScript, 'release/bin/kafka-dashboard-darwin-arm64'], {
      stdio: ['ignore', 'pipe', 'ignore'],
      env: { ...process.env, EXPECTED_SCRIPT: path.join(f.release, 'bin/kafka-dashboard-darwin-arm64') },
    });
    const exited = new Promise(resolve => child.once('exit', resolve));
    t.after(async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      await exited;
    });
    await new Promise((resolve, reject) => {
      child.stdout.once('data', resolve);
      child.once('error', reject);
      child.once('exit', () => reject(new Error('script fixture exited before readiness')));
    });
    await mkdir(path.join(f.release, 'data'));
    await writeFile(pidPath(f.release), `${child.pid}\n`);
    for (const command of ['status', 'start', 'stop', 'restart']) {
      const result = f.run([command]);
      assert.notEqual(result.status, 0, `must reject ambiguous argv during ${command}`);
      assert.match(result.stderr, /进程身份不匹配/);
      assert.equal(await readPid(f.release), child.pid);
      process.kill(child.pid, 0);
    }
  });
}
test('process identity rejects a native process with the binary path forged as argv zero', async t => {
  const f = await fixture(t);
  const child = spawn('/bin/cat', [], {
    argv0: path.join(f.release, 'bin/kafka-dashboard-darwin-arm64'), stdio: ['pipe', 'ignore', 'ignore'],
  });
  const exited = new Promise(resolve => child.once('exit', resolve));
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await exited;
  });
  await new Promise(resolve => child.once('spawn', resolve));
  await mkdir(path.join(f.release, 'data'));
  await writeFile(pidPath(f.release), `${child.pid}\n`);
  const result = f.run(['stop']);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /进程身份不匹配/);
  process.kill(child.pid, 0);
});
test('process identity rejects a forged native argv zero even when the target is memory mapped', async t => {
  const f = await fixture(t);
  const binary = path.join(f.release, 'bin/kafka-dashboard-darwin-arm64');
  const python = spawnSync('/usr/bin/python3', ['-c', 'import sys; print(sys.prefix)'], { encoding: 'utf8' });
  assert.equal(python.status, 0, python.stderr);
  const pythonHome = python.stdout.trim();
  const child = spawn(path.join(pythonHome, 'Resources/Python.app/Contents/MacOS/Python'), [], {
    argv0: binary, stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, PYTHONHOME: pythonHome },
  });
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  const exited = new Promise(resolve => child.once('exit', resolve));
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await exited;
  });
  const ready = new Promise((resolve, reject) => {
    child.stdout.once('data', resolve);
    child.once('error', reject);
    child.once('exit', () => reject(new Error(`memory-map fixture exited before readiness: ${stderr}`)));
  });
  child.stdin.end(`import mmap, time
target = open(${JSON.stringify(binary)}, 'rb')
mapping = mmap.mmap(target.fileno(), 0, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ)
print('ready', flush=True)
time.sleep(60)
`);
  await ready;
  await mkdir(path.join(f.release, 'data'));
  await writeFile(pidPath(f.release), `${child.pid}\n`);
  const result = f.run(['status']);
  assert.notEqual(result.status, 0, 'a mapping must not be accepted as the executed binary');
  assert.match(result.stderr, /进程身份不匹配/);
  process.kill(child.pid, 0);
});
test('process inspection consumes a large table without broken pipe diagnostics', async t => {
  const f = await fixture(t);
  assert.equal(f.run(['start']).status, 0);
  const pid = await readPid(f.release);
  await executable(path.join(f.commands, 'ps'), `#!/bin/sh
[ "$1" = -e ] || exec /bin/ps "$@"
awk 'BEGIN {
  print ENVIRON["TEST_PID"] " /bin/sh " ENVIRON["TEST_BINARY"]
  for (i = 0; i < 10000; i++) print 100000 + i " /unrelated/background-service"
}'
`);
  const env = { TEST_PID: String(pid), TEST_BINARY: path.join(f.release, 'bin/kafka-dashboard-darwin-arm64') };
  for (const action of ['status', 'stop']) {
    const result = f.run([action], env);
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stderr, '', 'successful process inspection must not emit pipe errors');
  }
  assert.equal(isRunning(pid), false);
});
for (const kind of ['exact command', 'similar first path', 'similar second path', 'extra argument', 'unrelated prefix']) {
  test(`QEMU process identity rejects unproven ${kind} from a process table alone`, async t => {
    const f = await fixture(t);
    assert.equal(f.run(['start']).status, 0);
    const pid = await readPid(f.release);
    await serviceReady(f, pid);
    const binary = path.join(f.release, 'bin/kafka-dashboard-darwin-arm64');
    const command = {
      'exact command': `[qemu] ${binary} ${binary}`,
      'similar first path': `[qemu] ${binary}.old ${binary}`,
      'similar second path': `[qemu] ${binary} ${binary}.old`,
      'extra argument': `[qemu] ${binary} ${binary} --unrelated`,
      'unrelated prefix': `wrapper [qemu] ${binary} ${binary}`,
    }[kind];
    // Flattened process text cannot supply QEMU's executable or NUL argv proof.
    // The real Linux/amd64 release verification covers the accepted QEMU process.
    await executable(path.join(f.commands, 'ps'), '#!/bin/sh\nprintf "%s {kafka-dashboard} %s\\n" "$TEST_PID" "$TEST_COMMAND"\n');
    const env = { TEST_PID: String(pid), TEST_COMMAND: command };
    for (const action of ['status', 'stop', 'restart']) {
      const result = f.run([action], env);
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, /进程身份不匹配/);
      assert.equal(await readPid(f.release), pid);
      assert.equal(isRunning(pid), true);
    }
    assert.equal(await readFile(f.events, 'utf8'), `START:${pid}\n`);
  });
}
test('failed process inspection is a pid error and preserves the pid file', async t => {
  const f = await fixture(t);
  await mkdir(path.join(f.release, 'data'));
  await writeFile(pidPath(f.release), `${process.pid}\n`);
  await executable(path.join(f.commands, 'ps'), '#!/bin/sh\nexit 19\n');
  for (const command of ['status', 'stop', 'restart']) {
    const result = f.run([command]);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /无法检查进程/);
    assert.equal(await readPid(f.release), process.pid);
  }
});
test('existing lock rejects concurrent management without removing the lock', async t => {
  const f = await fixture(t);
  const lock = path.join(f.release, 'data/kafka-dashboard.lock');
  await mkdir(lock, { recursive: true });
  for (const command of ['start', 'status']) {
    const result = f.run([command]);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /另一个启动器管理操作正在进行/);
    assert.equal(existsSync(lock), true);
  }
});
test('signal exit releases its lock once and preserves a replacement owner lock', async t => {
  const f = await fixture(t);
  const lock = path.join(f.release, 'data/kafka-dashboard.lock');
  const ready = path.join(f.temp, 'inspection-ready');
  const releases = path.join(f.temp, 'lock-releases');
  await mkdir(path.join(f.release, 'data'));
  await writeFile(pidPath(f.release), `${process.pid}\n`);
  await executable(path.join(f.commands, 'ps'), '#!/bin/sh\nprintf ready > "$INSPECTION_READY"\nsleep 1\n');
  // Simulate another launcher acquiring the directory immediately after release.
  await executable(path.join(f.commands, 'rmdir'), `#!/bin/sh
printf 'release\\n' >> "$LOCK_RELEASES"
/bin/rmdir "$@" || exit
if [ ! -e "$LOCK_REPLACED" ]; then
    : > "$LOCK_REPLACED"
    mkdir "$@"
fi
`);
  const child = spawn('/bin/sh', [path.join(f.release, 'start.sh'), 'status'], {
    cwd: f.temp, stdio: 'ignore',
    env: { ...process.env, PATH: `${f.commands}:${process.env.PATH}`, INSPECTION_READY: ready,
      LOCK_RELEASES: releases, LOCK_REPLACED: path.join(f.temp, 'replacement-owner') },
  });
  const exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await exited;
  });
  for (let attempt = 0; attempt < 100 && !existsSync(ready); attempt++) {
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  assert.equal(existsSync(ready), true, 'launcher should hold the lock during inspection');
  child.kill('SIGTERM');
  assert.deepEqual(await exited, { code: 143, signal: null });
  assert.equal(await readFile(releases, 'utf8'), 'release\n');
  assert.equal(existsSync(lock), true, 'replacement owner must retain its lock');
});
test('version runs in the foreground without creating data, pid, or log', async t => {
  const f = await fixture(t);
  const result = f.run(['--version']);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, 'kafka-dashboard 0.2.0\n');
  assert.equal(existsSync(path.join(f.release, 'data')), false);
});
for (const args of [['unknown'], ['--bind', '127.0.0.1:9090'], ['start', 'extra'], ['--version', 'extra']]) {
  test(`unknown argument ${JSON.stringify(args)} reports Chinese usage`, async t => {
    const f = await fixture(t);
    const result = f.run(args);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /用法: .*start\|stop\|status\|restart\|--version/);
    assert.equal(existsSync(path.join(f.release, 'data')), false);
  });
}

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
    await writeFile(path.join(f.release, '.env'), [
      'DATABASE_URL=sqlite://data/kafka-dashboard.db?mode=rwc',
      'KAFKA_DASHBOARD_WEB_DIR=web',
      '',
    ].join('\n'));
    const moved = path.join(f.temp, directory);
    await rename(f.release, moved);
    const started = f.run([], { KAFKA_DASHBOARD_BIND: '127.0.0.1:0', RUST_LOG: 'info', NO_COLOR: '1' }, moved);
    assert.equal(started.status, 0, started.stderr);
    const pid = await readPid(moved);
    try {
      let port;
      for (let attempt = 0; attempt < 100; attempt++) {
        port = (await readFile(logPath(moved), 'utf8')).match(/address=127\.0\.0\.1:(\d+)/)?.[1];
        if (port) break;
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      assert.ok(port, 'server failed to log its listening port');
      const response = await fetch(`http://127.0.0.1:${port}/api/health`);
      assert.equal(response.status, 200);
      assert.deepEqual(await response.json(), { status: 'ok' });
      const db = await readFile(path.join(moved, 'data/kafka-dashboard.db'));
      assert.equal(db.subarray(0, 16).toString(), 'SQLite format 3\0');
      assert.equal(existsSync(path.join(f.temp, 'data/kafka-dashboard.db')), false);
      process.kill(pid, 'SIGTERM');
      for (let attempt = 0; attempt < 100; attempt++) {
        try { process.kill(pid, 0); } catch { break; }
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
    } finally {
      try { process.kill(pid, 'SIGKILL'); } catch {}
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

async function runtimeFixture(t, ignoreTerm = false) {
  const f = await buildFixture(t);
  assert.equal(f.run().status, 0);
  const release = path.join(f.temp, 'dist/kafka-dashboard');
  await executable(path.join(release, 'bin/kafka-dashboard-darwin-arm64'), `#!/bin/sh
[ "\${1-}" != --version ] || exit 0
printf '%s\\n' "$$" > "$SERVER_PID_FILE"
trap '${ignoreTerm ? ':' : 'exit 0'}' TERM
[ "$RUST_LOG" != info ] || echo 'address=127.0.0.1:12345'
while :; do sleep 0.1; done
`);
  const manifest = await readFile(path.join(release, 'SHA256SUMS'), 'utf8');
  const digest = createHash('sha256').update(await readFile(path.join(release, 'bin/kafka-dashboard-darwin-arm64'))).digest('hex');
  await writeFile(path.join(release, 'SHA256SUMS'), manifest.replace(/^[a-f0-9]{64}(  \.\/bin\/kafka-dashboard-darwin-arm64)$/m, `${digest}$1`));
  await executable(path.join(f.temp, 'commands/file'), '#!/bin/sh\nprintf "Mach-O 64-bit executable arm64 ELF 64-bit x86-64 statically linked\\n"\n');
  await executable(path.join(f.temp, 'commands/otool'), '#!/bin/sh\nprintf "binary:\\n"\n');
  // Trace the real launcher, keeping its startup, identity and signal handling.
  const launcher = path.join(release, 'start.sh');
  const launcherSource = await readFile(launcher, 'utf8');
  await executable(launcher, launcherSource.replace('set -eu', `set -eu
[ -z "\${MAC_EVENTS-}" ] || printf '%s\\n' "\${1-start}" >> "$MAC_EVENTS"
if [ -n "\${MAC_REFUSE_IDENTITY_FILE-}" ] && [ -e "$MAC_REFUSE_IDENTITY_FILE" ]; then
    case "\${1-}" in stop|status) echo 'PID 文件对应的进程身份不匹配' >&2; exit 2 ;; esac
fi
if [ "\${1-}" = status ] && [ -n "\${MAC_STATUS_FAULT-}" ]; then
    if [ -f "$(dirname "$0")/data/kafka-dashboard.pid" ]; then
        case "$MAC_STATUS_FAULT" in
            wrong-pid) echo 'Kafka Lens 正在运行，PID: 1'; exit 0 ;;
            running-error) exit 17 ;;
        esac
    else
        case "$MAC_STATUS_FAULT" in
            wrong-stopped) echo 'Kafka Lens 未运行 extra'; exit 0 ;;
            stopped-error) exit 17 ;;
        esac
    fi
fi`));
  const launcherDigest = createHash('sha256').update(await readFile(launcher)).digest('hex');
  await writeFile(path.join(release, 'SHA256SUMS'), (await readFile(path.join(release, 'SHA256SUMS'), 'utf8'))
    .replace(/^[a-f0-9]{64}(  \.\/start\.sh)$/m, `${launcherDigest}$1`));
  return { ...f, release };
}

test('failed runtime verification reaps even a server that ignores TERM', async t => {
  const f = await runtimeFixture(t, true);
  const serverPidFile = path.join(f.temp, 'server.pid');
  let serverPid;
  t.after(() => {
    try { if (serverPid) process.kill(serverPid, 'SIGKILL'); } catch {}
  });
  await executable(path.join(f.temp, 'commands/docker'), '#!/bin/sh\nexit 0\n');
  await executable(path.join(f.temp, 'commands/curl'), '#!/bin/sh\nexit 1\n');
  const result = spawnSync('/bin/sh', [path.join(root, 'tools/verify-release.sh'), f.release], {
    encoding: 'utf8', timeout: runtimeVerificationTimeoutMs,
    env: { ...process.env, PATH: `${f.temp}/commands:${process.env.PATH}`, SERVER_PID_FILE: serverPidFile, RUST_LOG: 'error' },
  });
  serverPid = Number(await readFile(serverPidFile, 'utf8'));
  assert.equal(result.error, undefined, 'verification must finish its own failure cleanup before timeout');
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Health timeout/);
  assert.throws(() => process.kill(serverPid, 0), { code: 'ESRCH' });
});

test('failed verification never kills a pid after the launcher refuses its identity', async t => {
  const f = await runtimeFixture(t);
  const serverPidFile = path.join(f.temp, 'server.pid');
  const refusal = path.join(f.temp, 'identity-refused');
  let serverPid;
  t.after(async () => {
    if (!serverPid && existsSync(serverPidFile)) serverPid = Number(await readFile(serverPidFile, 'utf8'));
    try { if (serverPid) process.kill(serverPid, 'SIGKILL'); } catch {}
    for (let attempt = 0; serverPid && isRunning(serverPid) && attempt < 100; attempt++) {
      await new Promise(resolve => setTimeout(resolve, 20));
    }
    if (serverPid) assert.equal(isRunning(serverPid), false, 'identity refusal fixture must be reaped by its owner');
  });
  await executable(path.join(f.temp, 'commands/docker'), '#!/bin/sh\nexit 0\n');
  await executable(path.join(f.temp, 'commands/curl'), '#!/bin/sh\ntouch "$MAC_REFUSE_IDENTITY_FILE"\nexit 1\n');
  const result = spawnSync('/bin/sh', [path.join(root, 'tools/verify-release.sh'), f.release], {
    encoding: 'utf8', timeout: runtimeVerificationTimeoutMs,
    env: { ...process.env, PATH: `${f.temp}/commands:${process.env.PATH}`, SERVER_PID_FILE: serverPidFile,
      MAC_REFUSE_IDENTITY_FILE: refusal },
  });
  serverPid = Number(await readFile(serverPidFile, 'utf8'));
  assert.equal(result.error, undefined);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Health timeout/);
  assert.equal(isRunning(serverPid), true, 'identity refusal must prevent forced cleanup of this pid');
});

async function verifyDualPlatformLifecycle(t, fault) {
  const f = await runtimeFixture(t);
  const serverPidFile = path.join(f.temp, 'server.pid');
  const dockerEvents = path.join(f.temp, 'docker-events');
  const macEvents = path.join(f.temp, 'mac-events');
  let serverPid;
  t.after(() => {
    try { if (serverPid) process.kill(serverPid, 'SIGKILL'); } catch {}
  });
  await executable(path.join(f.temp, 'commands/docker'), `#!/bin/sh
case "$1" in
  run)
    case " $* " in
      *' --rm '*) exit 0 ;;
    esac
    for argument do
      case "$argument" in */start.sh) echo 'launcher cannot be container PID 1' >&2; exit 1;; esac
    done
    printf 'container\\n' >> "$DOCKER_EVENTS"
    echo release-container ;;
  exec)
    [ "$2" = release-container ] || exit 2
    case "$3" in
      '/release?%20 中文/start.sh')
        printf '%s\\n' "$4" >> "$DOCKER_EVENTS"
        case "$4" in
          start) : ;;
          stop) touch "$DOCKER_EVENTS.stopped" ;;
          status)
            if [ -e "$DOCKER_EVENTS.stopped" ]; then echo 'Kafka Lens 未运行'; else echo 'Kafka Lens 正在运行，PID: 12345'; fi ;;
          *) exit 3 ;;
        esac ;;
      cat) echo 12345 ;;
      sh) exit 0 ;;
      *) exit 4 ;;
    esac ;;
  port) echo 127.0.0.1:23456 ;;
  rm) printf 'remove\\n' >> "$DOCKER_EVENTS" ;;
  *) exit 5 ;;
esac
`);
  await executable(path.join(f.temp, 'commands/curl'), `#!/bin/sh
for argument do url=$argument; done
case "$url" in
  */api/health) printf '{"status":"ok"}' ;;
  */assets/app.js) cat "$TEST_RELEASE/web/assets/app.js" ;;
  *) cat "$TEST_RELEASE/web/index.html" ;;
esac
`);
  await executable(path.join(f.temp, 'commands/sqlite3'), '#!/bin/sh\necho 2\n');
  await writeFile(path.join(f.release, 'web/index.html'), '<html><script src="/assets/app.js"></script></html>');
  const manifest = await readFile(path.join(f.release, 'SHA256SUMS'), 'utf8');
  const digest = createHash('sha256').update(await readFile(path.join(f.release, 'web/index.html'))).digest('hex');
  await writeFile(path.join(f.release, 'SHA256SUMS'), manifest.replace(/^[a-f0-9]{64}(  \.\/web\/index.html)$/m, `${digest}$1`));
  const result = spawnSync('/bin/sh', [path.join(root, 'tools/verify-release.sh'), f.release], {
    encoding: 'utf8', timeout: runtimeVerificationTimeoutMs,
    env: { ...process.env, PATH: `${f.temp}/commands:${process.env.PATH}`, SERVER_PID_FILE: serverPidFile, TEST_RELEASE: f.release, DOCKER_EVENTS: dockerEvents, MAC_EVENTS: macEvents, MAC_STATUS_FAULT: fault },
  });
  serverPid = Number(await readFile(serverPidFile, 'utf8'));
  assert.equal(result.error, undefined);
  if (fault) {
    assert.notEqual(result.status, 0, `verification must reject ${fault}`);
    const expectedError = {
      'wrong-pid': 'macOS status did not report the exact running service',
      'running-error': 'macOS running status command failed',
      'wrong-stopped': 'macOS status did not report the exact stopped state',
      'stopped-error': 'macOS stopped status command failed',
    }[fault];
    assert.ok(result.stderr.includes(expectedError), result.stderr);
    assert.deepEqual((await readFile(macEvents, 'utf8')).trim().split('\n'),
      fault.includes('stopped') ? ['start', 'status', 'stop', 'status', 'stop'] : ['start', 'status', 'stop']);
    assert.equal(existsSync(dockerEvents), false, 'macOS status failure must prevent Linux startup');
  } else {
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual((await readFile(macEvents, 'utf8')).trim().split('\n'), ['start', 'status', 'stop', 'status', 'stop']);
    assert.deepEqual((await readFile(dockerEvents, 'utf8')).trim().split('\n'), ['container', 'start', 'status', 'stop', 'status', 'remove']);
  }
  assert.throws(() => process.kill(serverPid, 0), { code: 'ESRCH' });
}
for (const fault of ['', 'wrong-pid', 'running-error', 'wrong-stopped', 'stopped-error']) {
  test(`dual-platform lifecycle verification checks both macOS statuses${fault ? ` and rejects ${fault}` : ''}`,
    t => verifyDualPlatformLifecycle(t, fault));
}
