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
const fail = (message) => {
  throw new Error(message);
};
function command(program, args, allowFailure = false) {
  const r = spawnSync(program, args, {
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024,
  });
  if (r.error || (!allowFailure && r.status !== 0))
    fail(`${program} ${args.slice(0, 2).join(" ")} failed`);
  return r;
}
const git = (...args) => command("git", args).stdout.trim();
const gh = (...args) => command("gh", args).stdout;
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");

function main() {
  const [tag, directory, ...extra] = process.argv.slice(2);
  if (
    extra.length ||
    !directory ||
    !/^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.test(tag ?? "")
  )
    fail("usage: publish-release.mjs <vX.Y.Z> <artifacts-dir>");
  const commit = git("rev-parse", "HEAD");
  const localTag = () => git("rev-parse", `refs/tags/${tag}^{commit}`);
  if (localTag() !== commit) fail("tag does not match checkout");
  const cargo = readFileSync("backend/Cargo.toml", "utf8")
    .split(/^\[/m)
    .find((s) => s.startsWith("package]"));
  if (cargo?.match(/^version\s*=\s*"([^"]+)"/m)?.[1] !== tag.slice(1))
    fail("tag does not match Cargo version");
  // Require tracked checkout content to be exactly the tagged tree; downloaded artifacts may be untracked.
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
  remoteTag();
  // The by-tag REST endpoint only promises published releases. A successful,
  // authenticated paginated list includes drafts visible to this publisher.
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
  const view = () =>
    JSON.parse(
      gh("release", "view", tag, "--json", "assets,isDraft,tagName,url"),
    );
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
  const temp = mkdtempSync(join(tmpdir(), "kafka-release-download-"));
  try {
    const existing = check(view(), true, false);
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
    for (const name of existing) download(name);
    const missing = [...files.keys()].filter((name) => !existing.has(name));
    if (missing.length)
      gh("release", "upload", tag, ...missing.map((name) => join(dir, name)));
    check(view(), true, true);
    for (const name of files.keys()) download(name);
    remoteTag();
    check(view(), true, true);
    gh("release", "edit", tag, "--draft=false");
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
