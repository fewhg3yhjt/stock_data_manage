# Project Agent Requirements

## Change And Delivery Workflow

- Treat every file edit as saved immediately; do not leave requested code changes only in an unreconciled working buffer.
- After completing a requested change, inspect `git status` and `git diff` before committing.
- Run the most relevant tests, checks, or smoke verification before committing. Do not commit a change that has not been verified unless the user explicitly asks for an unverified checkpoint.
- Stage only files changed for the current request. Never stage `.env` files, credentials, tokens, private keys, or unrelated user changes.
- If unrelated pre-existing changes are present, leave them untouched and do not include them in the commit.
- Create a focused commit after verification. Use a concise message describing the change.
- The repository `post-commit` hook automatically pushes successful commits on `main` to `origin/main`. Do not run a second push unless the hook fails.
- If commit or push fails, preserve the local changes and report the exact failure; do not create duplicate commits or silently retry with a different remote.

## Safety

- Do not delete, overwrite, or reset existing user data or files without explicit confirmation.
- Do not modify unrelated files just to make a commit clean.
- Before pushing, confirm that the commit contains only the requested change and no sensitive content.

## Workspace Permissions

- Project source, test, configuration, and documentation files must be owned by the normal project user, not `root`.
- Do not run project editing, formatting, testing, installation, or build commands with `sudo` or as `root` unless the user explicitly requests it.
- Before editing a target file, check its owner when a permission error occurs or when the file was created by an external tool.
- If a required project file is owned by `root`, repair ownership only for the specific files needed by the current task; do not recursively `chown` the repository.
- Use the current project user and group for ownership repair, and verify ownership after the repair before editing.
- Do not change permissions or ownership of `.git`, credentials, mounted data directories, or unrelated files as part of routine development.
- New files created during development must be checked to ensure they are owned by the normal project user.
- If ownership cannot be repaired without broad or destructive changes, stop and report the affected paths instead of bypassing permissions.

## 方案讨论语言

- 进行架构设计、方案讨论、需求分析、实施计划和技术评审时，默认使用中文。
- 专业术语第一次出现时优先使用“中文名称（English Term）”，后续优先使用中文名称。
- 代码标识符、配置字段、协议原名和第三方产品名称可以保留英文，但应同时说明其中文含义。
- 除引用原文、代码或外部协议外，避免使用纯英文标题、流程图和大段说明。
