# EasyTier-iOS Bug 库

跟主仓库（GreatMichaelLee/Easytier）的 `docs/bugs.md` 一个思路：每次发版/CI 跑出来的问题记在这里，关联对应的修复提交和 Build 号。

| 编号 | 发现于 Build | 描述 | 状态 | 修复提交 | 修复于 Build |
|---|---|---|---|---|---|
| BUG-001 | 001 | nightly.yml 发布 release 时同时传了 `--prerelease` 和后续的 `--latest`，GitHub API 拒绝（"Latest release cannot be draft or prerelease"），Build 001 的发布步骤失败 | 已修复 | b64b5d9 之后（本次修复） | 002 |
