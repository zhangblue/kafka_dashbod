import { test } from "node:test";
import assert from "node:assert/strict";
import {
  mkdtempSync,
  mkdirSync,
  writeFileSync,
  readFileSync,
  readdirSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
const script = resolve("tools/publish-release.mjs");
// 每个发布用例创建真实临时 Git 标签及三平台归档，只用本地 gh 替身模拟远端，绝不写入 GitHub。
function fixture(mode) {
  const root = mkdtempSync(join(tmpdir(), "publish-test-"));
  // fixture 装配期间同步执行真实 Git/tar，命令失败即中止，确保标签与压缩字节可信。
  const run = (cmd, args) => {
    const r = spawnSync(cmd, args, {
      cwd: root,
      encoding: "utf8",
      env: { ...process.env, COPYFILE_DISABLE: "1" },
    });
    assert.equal(r.status, 0, r.stderr);
    return r.stdout.trim();
  };
  mkdirSync(join(root, "backend"));
  writeFileSync(
    join(root, "backend/Cargo.toml"),
    '[package]\nversion = "1.2.3"\n',
  );
  // 先建立提交再创建精确标签，随后生成的 RELEASE.json 统一绑定同一份来源。
  run("git", ["init", "-q"]);
  run("git", ["add", "."]);
  run("git", [
    "-c",
    "user.name=Test",
    "-c",
    "user.email=test@example.invalid",
    "commit",
    "-qm",
    "fixture",
  ]);
  run("git", ["tag", "v1.2.3"]);
  const commit = run("git", ["rev-parse", "HEAD"]);
  for (const dir of ["dist", "remote", "fake"]) mkdirSync(join(root, dir));
  for (const [platform, target] of Object.entries({
    "linux-amd64": "x86_64-unknown-linux-musl",
    "linux-arm64": "aarch64-unknown-linux-musl",
    "darwin-arm64": "aarch64-apple-darwin",
  })) {
    const base = `kafka-dashboard-v1.2.3-${platform}`;
    mkdirSync(join(root, base));
    const meta = { version: "1.2.3", platform, target, commit, dirty: false };
    if (mode === "dirty") meta.dirty = true;
    if (mode === "commit") meta.commit = "0".repeat(40);
    if (mode === "target") meta.target = "wrong";
    if (mode === "platform") meta.platform = "wrong";
    if (mode === "version") meta.version = "0.0.0";
    const contents = {
      "RELEASE.json": Buffer.from(JSON.stringify(meta)),
      "start.sh": Buffer.from("#!/bin/sh\n"),
      ".env.example": Buffer.from("PLACEHOLDER=1\n"),
      "README.txt": Buffer.from("fixture"),
      "web/index.html": Buffer.from("<html></html>"),
      "web/assets/app.js": Buffer.from("fixture"),
    };
    let binary = Buffer.alloc(256);
    if (platform.startsWith("linux-")) {
      binary.set([0x7f, 0x45, 0x4c, 0x46, 2, 1]);
      binary.writeUInt16LE(platform === "linux-amd64" ? 62 : 183, 18);
      binary[6] = 1;
      binary.writeUInt16LE(2, 16);
      binary.writeUInt32LE(1, 20);
      binary.writeBigUInt64LE(0x400080n, 24);
      binary.writeBigUInt64LE(64n, 32);
      binary.writeUInt16LE(64, 52);
      binary.writeUInt16LE(56, 54);
      binary.writeUInt16LE(1, 56);
      binary.writeUInt32LE(mode === "dynamic-linux" ? 3 : 1, 64);
      binary.writeUInt32LE(5, 68);
      binary.writeBigUInt64LE(0x400000n, 80);
      binary.writeBigUInt64LE(256n, 96);
      binary.writeBigUInt64LE(256n, 104);
      binary.writeBigUInt64LE(0x1000n, 112);
      if (mode === "segment-outside") binary.writeBigUInt64LE(512n, 96);
      if (mode === "needed-linux") {
        binary.writeUInt16LE(2, 56);
        binary.writeUInt32LE(2, 120);
        binary.writeBigUInt64LE(192n, 128);
        binary.writeBigUInt64LE(32n, 152);
        binary.writeBigUInt64LE(32n, 160);
        binary.writeBigInt64LE(1n, 192);
      }
    } else {
      binary.set([0xcf, 0xfa, 0xed, 0xfe]);
      binary.writeUInt32LE(0x0100000c, 4);
      binary.writeUInt32LE(2, 12);
      binary.writeUInt32LE(2, 16);
      binary.writeUInt32LE(96, 20);
      binary.writeUInt32LE(0x19, 32);
      binary.writeUInt32LE(72, 36);
      binary.write("__TEXT", 40);
      binary.writeBigUInt64LE(0x100000000n, 56);
      binary.writeBigUInt64LE(4096n, 64);
      binary.writeBigUInt64LE(256n, 80);
      binary.writeUInt32LE(5, 88);
      binary.writeUInt32LE(5, 92);
      binary.writeUInt32LE(0x80000028, 104);
      binary.writeUInt32LE(24, 108);
      binary.writeBigUInt64LE(128n, 112);
      if (mode === "macho-truncated") binary = binary.subarray(0, 32);
      if (mode === "macho-command-outside") binary.writeUInt32LE(512, 36);
      if (mode === "macho-not-executable") binary.writeUInt32LE(6, 12);
    }
    if (mode === "truncated-body") binary = binary.subarray(0, 32);
    if (mode === "architecture") binary.fill(0);
    contents[`bin/kafka-dashboard-${platform}`] = binary;
    if (mode === "metadata-only")
      for (const name of Object.keys(contents))
        if (name !== "RELEASE.json") delete contents[name];
    if (mode === "secret") contents[".env"] = Buffer.from("fixture");
    contents.SHA256SUMS = Buffer.from(
      Object.entries(contents)
        .map(
          ([name, bytes]) =>
            `${createHash("sha256").update(bytes).digest("hex")}  ${name}\n`,
        )
        .join(""),
    );
    if (mode === "internal-checksum")
      contents["web/index.html"] = Buffer.from("changed");
    for (const [name, bytes] of Object.entries(contents)) {
      mkdirSync(join(root, base, name, ".."), { recursive: true });
      writeFileSync(join(root, base, name), bytes, {
        mode: name === "start.sh" || name.startsWith("bin/") ? 0o755 : 0o644,
      });
    }
    // 保留真实压缩与外层哈希流程，使篡改测试能区分包内 checksum 与归档字节校验。
    run("tar", ["-czf", join(root, "dist", base + ".tar.gz"), base]);
    const bytes = readFileSync(join(root, "dist", base + ".tar.gz"));
    writeFileSync(
      join(root, "dist", base + ".tar.gz.sha256"),
      `${createHash("sha256").update(bytes).digest("hex")}  ${base}.tar.gz\n`,
    );
    rmSync(join(root, base), { recursive: true });
  }
  if (mode === "missing")
    rmSync(join(root, "dist", readdirSync(join(root, "dist"))[0]));
  if (mode === "checksum")
    writeFileSync(
      join(
        root,
        "dist",
        readdirSync(join(root, "dist")).find((x) => x.endsWith(".sha256")),
      ),
      "bad",
    );
  writeFileSync(join(root, "commit"), commit);
  // 发布入口仅能调用此临时目录中的 gh 替身，所有上传、下载及取消草稿都停留在本地 fixture。
  writeFileSync(
    join(root, "fake", "gh"),
    readFileSync(new URL("./fixtures/publish-gh.cjs", import.meta.url)),
    { mode: 0o755 },
  );
  if (mode === "published")
    writeFileSync(join(root, "state"), JSON.stringify({ draft: false }));
  return {
    root,
    // 用例调用时以本地 gh 替身执行真实发布入口，可指定错误标签检验前置门禁。
    execute(tag = "v1.2.3") {
      return spawnSync(process.execPath, [script, tag, "dist"], {
        cwd: root,
        encoding: "utf8",
        env: {
          ...process.env,
          PATH: join(root, "fake") + ":" + process.env.PATH,
          MODE: mode,
        },
      });
    },
    // 发布断言读取替身记录的调用顺序；尚未访问远端时返回空列表。
    calls() {
      try {
        return readFileSync(join(root, "calls"), "utf8")
          .trim()
          .split("\n")
          .map(JSON.parse);
      } catch {
        return [];
      }
    },
  };
}
for (const mode of [
  "metadata-only",
  "truncated-body",
  "dynamic-linux",
  "needed-linux",
  "segment-outside",
  "macho-truncated",
  "macho-command-outside",
  "macho-not-executable",
  "secret",
  "internal-checksum",
  "architecture",
  "missing",
  "checksum",
  "dirty",
  "commit",
  "target",
  "platform",
  "version",
  "tamper",
  "upload",
  "published",
  "auth",
  "network",
  "not-found",
  "duplicate-releases",
  "moved",
  "extra",
  "remote-missing",
  "remote-size",
  "not-draft",
])
  // 参数化验证损坏附件、来源、远端状态及网络故障均阻止取消草稿。
  test(`reject ${mode} without publishing`, () => {
    const f = fixture(mode);
    try {
      const r = f.execute();
      assert.notEqual(r.status, 0);
      assert.match(r.stderr, /release:/);
      assert.equal(
        f.calls().some((a) => a[1] === "edit"),
        false,
      );
    } finally {
      rmSync(f.root, { recursive: true, force: true });
    }
  });
// 验证错误标签在任何 gh 远端调用之前就被拒绝。
test("wrong tag rejected before gh", () => {
  const f = fixture("");
  try {
    assert.notEqual(f.execute("v1.2.4").status, 0);
    assert.equal(f.calls().length, 0);
  } finally {
    rmSync(f.root, { recursive: true, force: true });
  }
});
// 验证完整六附件上传、六次下载逐字节核验均先于取消草稿，之后仍复核正式状态。
test("six uploads then six downloads then publish and final verification", () => {
  const f = fixture("");
  try {
    const r = f.execute();
    assert.equal(r.status, 0, r.stderr);
    const calls = f.calls();
    assert.equal(calls.find((a) => a[1] === "upload").length, 9);
    const edit = calls.findIndex((a) => a[1] === "edit");
    assert.equal(
      calls.slice(0, edit).filter((a) => a[1] === "download").length,
      6,
    );
    assert.equal(calls.at(-1)[1], "view");
    assert.match(r.stdout, /Published/);
  } finally {
    rmSync(f.root, { recursive: true, force: true });
  }
});
// 验证远端附注标签先解析到最终提交，再完成相同来源门禁。
test("annotated remote tag resolves to commit", () => {
  const f = fixture("annotated");
  try {
    assert.equal(f.execute().status, 0);
    assert.ok(f.calls().some((a) => a[1].includes("/git/tags/")));
  } finally {
    rmSync(f.root, { recursive: true, force: true });
  }
});
for (const changed of [false, true])
  // 验证已有草稿仅接受原始六附件相同字节的重试；不一致时既不覆盖也不发布。
  test(`existing draft retry with ${changed ? "different" : "identical"} bytes`, () => {
    const f = fixture("");
    try {
      writeFileSync(join(f.root, "state"), JSON.stringify({ draft: true }));
      for (const name of readdirSync(join(f.root, "dist")))
        writeFileSync(
          join(f.root, "remote", name),
          readFileSync(join(f.root, "dist", name)),
        );
      if (changed) {
        const name = readdirSync(join(f.root, "remote"))[0];
        const bytes = readFileSync(join(f.root, "remote", name));
        bytes[0] ^= 1;
        writeFileSync(join(f.root, "remote", name), bytes);
      }
      assert.equal(f.execute().status, changed ? 1 : 0);
      assert.equal(
        f.calls().some((a) => a[1] === "upload"),
        false,
      );
      assert.equal(
        f.calls().some((a) => a[1] === "edit"),
        !changed,
      );
    } finally {
      rmSync(f.root, { recursive: true, force: true });
    }
  });
