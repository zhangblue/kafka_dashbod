#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import {
  readFileSync,
  readdirSync,
  lstatSync,
  mkdtempSync,
  rmSync,
} from "node:fs";
import { resolve, join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

const platforms = {
  "linux-amd64": "x86_64-unknown-linux-musl",
  "linux-arm64": "aarch64-unknown-linux-musl",
  "darwin-arm64": "aarch64-apple-darwin",
};
// 发布门禁失败时抛出安全摘要，由入口统一报告并保持非零退出状态。
const fail = (message) => {
  throw new Error(message);
};
// 来源检查和发布步骤调用外部命令；失败即终止，避免把鉴权或网络错误解释为资源不存在。
function command(program, args, allowFailure = false) {
  const r = spawnSync(program, args, {
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024,
  });
  if (r.error || (!allowFailure && r.status !== 0))
    fail(`${program} ${args.slice(0, 2).join(" ")} failed`);
  return r;
}
// 本地来源及标签复核时执行 Git，并去除命令输出末尾空白。
const git = (...args) => command("git", args).stdout.trim();
// 远端读取和受控发布时执行 GitHub CLI；任何命令失败都阻断后续正式发布。
const gh = (...args) => command("gh", args).stdout;
// 六附件预检时对压缩原始字节计算 SHA-256，与 sidecar 的精确内容比较。
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");

// CLI 入口先验证三平台六附件与精确来源，再以草稿完成传输核验，最后转为正式发布。
// CI 仅在三平台 build/verify 全部完成后由唯一发布 job 调用；workflow 按标签 concurrency 串行且不取消在途发布。
function main() {
  const [tag, directory, ...extra] = process.argv.slice(2);
  if (
    extra.length ||
    !directory ||
    !/^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.test(tag ?? "")
  )
    fail("usage: publish-release.mjs <vX.Y.Z> <artifacts-dir>");
  const commit = git("rev-parse", "HEAD");
  // 初始及远端复核阶段解析本地标签到提交，确保标签与当前 checkout 始终对应。
  const localTag = () => git("rev-parse", `refs/tags/${tag}^{commit}`);
  if (localTag() !== commit) fail("tag does not match checkout");
  const cargo = readFileSync("backend/Cargo.toml", "utf8")
    .split(/^\[/m)
    .find((s) => s.startsWith("package]"));
  if (cargo?.match(/^version\s*=\s*"([^"]+)"/m)?.[1] !== tag.slice(1))
    fail("tag does not match Cargo version");
  // Require tracked checkout content to be exactly the tagged tree; downloaded artifacts may be untracked.
  // 先拒绝跟踪源码的脏状态；下载的未跟踪附件不代表已打包源码发生变更。
  if (git("status", "--porcelain", "--untracked-files=no"))
    fail("tracked checkout is dirty");
  const dir = resolve(directory),
    files = new Map();
  for (const [platform, target] of Object.entries(platforms)) {
    const base = `kafka-dashboard-${tag}-${platform}`,
      archive = `${base}.tar.gz`,
      sidecar = `${archive}.sha256`;
    for (const name of [archive, sidecar]) {
      if (!lstatSync(join(dir, name)).isFile())
        fail("artifact is not a regular file");
      files.set(name, readFileSync(join(dir, name)));
    }
    if (
      files.get(sidecar).toString() !==
      `${hash(files.get(archive))}  ${archive}\n`
    )
      fail("archive checksum mismatch");
    const validator = fileURLToPath(
      new URL("./validate-release-archive.py", import.meta.url),
    );
    // 校验归档内部清单和二进制结构但不运行包内程序，之后再核对提交、目标与 dirty=false。
    const metadata = JSON.parse(
      command("python3", [validator, join(dir, archive), base, platform])
        .stdout,
    );
    if (
      Object.keys(metadata).sort().join(",") !==
        "commit,dirty,platform,target,version" ||
      metadata.version !== tag.slice(1) ||
      metadata.platform !== platform ||
      metadata.target !== target ||
      metadata.commit !== commit ||
      metadata.dirty !== false
    )
      fail("archive source metadata mismatch");
  }
  if (readdirSync(dir).some((n) => !files.has(n)))
    fail("artifacts directory must contain exactly six expected files");
  // 草稿操作前及转正式前递归解析远端标签，并重新检查本地 HEAD/标签，拒绝来源漂移。
  const remoteTag = () => {
    let object = JSON.parse(
      gh("api", `repos/{owner}/{repo}/git/ref/tags/${tag}`),
    ).object;
    for (let depth = 0; object.type === "tag" && depth < 8; depth++)
      object = JSON.parse(
        gh("api", `repos/{owner}/{repo}/git/tags/${object.sha}`),
      ).object;
    if (
      object.type !== "commit" ||
      object.sha !== commit ||
      localTag() !== commit ||
      git("rev-parse", "HEAD") !== commit
    )
      fail("remote tag does not match source commit");
  };
  // 所有本地附件门禁通过后才访问远端；标签确认失败不得创建草稿。
  remoteTag();
  // The by-tag REST endpoint only promises published releases. A successful,
  // authenticated paginated list includes drafts visible to this publisher.
  // 必须成功鉴权并分页枚举含草稿的列表，不能将 by-tag 的 404 或访问失败当作可新建证据。
  const pages = JSON.parse(
    gh(
      "api",
      "repos/{owner}/{repo}/releases?per_page=100",
      "--paginate",
      "--slurp",
    ),
  );
  if (!Array.isArray(pages) || pages.some((page) => !Array.isArray(page)))
    fail("invalid release listing");
  const matches = pages.flat().filter((release) => release.tag_name === tag);
  if (matches.length > 1) fail("multiple releases for the same tag");
  if (matches.length === 1 && matches[0].draft !== true)
    fail("published releases cannot be overwritten");
  // 仅在成功枚举且确无同标签 Release 时创建草稿，已有正式版本始终拒绝覆盖。
  if (!matches.length)
    gh(
      "release",
      "create",
      tag,
      "--draft",
      "--verify-tag",
      "--title",
      tag,
      "--notes",
      `Kafka Lens ${tag}`,
    );
  // 上传、转正式前后读取远端当前状态，供每个阶段核对附件与草稿标志。
  const view = () =>
    JSON.parse(
      gh("release", "view", tag, "--json", "assets,isDraft,tagName,url"),
    );
  // 每次远端状态复核时验证标签、草稿状态及附件唯一名称/大小，完整阶段还要求恰好六件。
  const check = (release, draft, complete) => {
    if (
      release.tagName !== tag ||
      release.isDraft !== draft ||
      !Array.isArray(release.assets)
    )
      fail("release state mismatch; published releases cannot be overwritten");
    const names = new Set();
    for (const asset of release.assets) {
      if (
        names.has(asset.name) ||
        !files.has(asset.name) ||
        asset.size !== files.get(asset.name).length
      )
        fail("remote asset inventory mismatch");
      names.add(asset.name);
    }
    if (complete && names.size !== files.size)
      fail("remote release must contain exactly six assets");
    return names;
  };
  // 下载到独占临时目录，finally 总会回收；本地六附件原字节保持不变供重试比对。
  const temp = mkdtempSync(join(tmpdir(), "kafka-release-download-"));
  try {
    const existing = check(view(), true, false);
    // 重试既有附件及上传后调用；先移除旧下载，再逐字节核对远端文件与本地原件。
    const download = (name) => {
      const destination = join(temp, name);
      rmSync(destination, { force: true });
      gh("release", "download", tag, "--pattern", name, "--dir", temp);
      if (
        !lstatSync(destination).isFile() ||
        !readFileSync(destination).equals(files.get(name))
      )
        fail("download bytes differ from local artifact");
    };
    // 先核验已有草稿附件，只续传缺少文件，不覆盖与本地不同的历史字节。
    for (const name of existing) download(name);
    const missing = [...files.keys()].filter((name) => !existing.has(name));
    if (missing.length)
      gh("release", "upload", tag, ...missing.map((name) => join(dir, name)));
    check(view(), true, true);
    // 上传后的名称/大小检查不足以证明内容正确，必须重新下载六附件逐字节比较。
    for (const name of files.keys()) download(name);
    remoteTag();
    check(view(), true, true);
    // 六附件字节、标签及草稿状态全部复核后才取消草稿；此前任一步失败均保留草稿供检查重试。
    gh("release", "edit", tag, "--draft=false");
    // 取消草稿后仍需确认正式状态及六附件清单，不能仅凭 edit 命令成功宣称发布完成。
    const published = view();
    check(published, false, true);
    console.log(`Published ${tag}: ${published.url}`);
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
}
try {
  main();
} catch (error) {
  console.error(`release: ${error.message}`);
  process.exitCode = 1;
}
