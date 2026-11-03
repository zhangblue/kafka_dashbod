#!/usr/bin/env node
// External GitHub boundary only; repository, tags, tar and hashes remain real.
// 发布回归调用 gh 时仅在本地文件中模拟远端标签、草稿和附件；不访问或写入 GitHub。
const fs = require("node:fs");
const path = require("node:path");
const args = process.argv.slice(2);
const mode = process.env.MODE;
const remote = "remote";
fs.appendFileSync("calls", JSON.stringify(args) + "\n");
const state = fs.existsSync("state")
  ? JSON.parse(fs.readFileSync("state"))
  : null;
// 模拟创建、改写或发布 Release 时保存草稿状态，供后续查询检验发布顺序。
const setDraft = (draft) =>
  fs.writeFileSync("state", JSON.stringify({ draft }));
// 模拟查询远端标签时读取 fixture 固定提交，供真实发布脚本核对来源。
const commit = () => fs.readFileSync("commit", "utf8");
if (args[0] === "api") {
  const endpoint = args[1];
  if (endpoint.includes("/git/ref/")) {
    const object =
      mode === "annotated"
        ? { type: "tag", sha: "a".repeat(40) }
        : {
            type: "commit",
            sha: mode === "moved" && state ? "0".repeat(40) : commit(),
          };
    console.log(JSON.stringify({ object }));
  } else if (endpoint.includes("/git/tags/")) {
    console.log(JSON.stringify({ object: { type: "commit", sha: commit() } }));
  } else if (mode === "network") {
    process.exitCode = 1;
  } else if (mode === "auth") {
    console.log("HTTP/2.0 401 Unauthorized\n\n{}");
    process.exitCode = 1;
  } else if (mode === "not-found") {
    console.log("HTTP/2.0 404 Not Found\n\n{}");
    process.exitCode = 1;
  } else if (mode === "duplicate-releases") {
    console.log(
      JSON.stringify([
        [{ tag_name: "v1.2.3", draft: true }],
        [{ tag_name: "v1.2.3", draft: true }],
      ]),
    );
  } else if (!state) {
    console.log(JSON.stringify([[]]));
  } else {
    console.log(JSON.stringify([[{ tag_name: "v1.2.3", draft: state.draft }]]));
  }
} else if (args[1] === "create") {
  setDraft(true);
} else if (args[1] === "view") {
  console.log(
    JSON.stringify({
      isDraft: state.draft,
      tagName: "v1.2.3",
      url: "https://example.invalid/release",
      assets: fs.readdirSync(remote).map((name) => ({
        name,
        size: fs.statSync(path.join(remote, name)).size,
      })),
    }),
  );
} else if (args[1] === "upload") {
  if (mode === "upload") process.exit(1);
  for (const file of args.slice(3))
    fs.copyFileSync(file, path.join(remote, path.basename(file)));
  // 上传后的故障注入阶段选取一个本地附件，模拟远端缺件或字节大小变化。
  const first = () => path.join(remote, fs.readdirSync(remote)[0]);
  if (mode === "extra") fs.writeFileSync(path.join(remote, "unexpected"), "X");
  if (mode === "remote-missing") fs.unlinkSync(first());
  if (mode === "remote-size") fs.appendFileSync(first(), "X");
  if (mode === "not-draft") setDraft(false);
} else if (args[1] === "download") {
  const name = args[args.indexOf("--pattern") + 1];
  const directory = args[args.indexOf("--dir") + 1];
  fs.copyFileSync(path.join(remote, name), path.join(directory, name));
  if (mode === "tamper") fs.appendFileSync(path.join(directory, name), "X");
} else if (args[1] === "edit") {
  setDraft(false);
} else {
  process.exitCode = 2;
}
