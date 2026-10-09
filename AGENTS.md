# Project Agent Requirements

## Change And Delivery Workflow

- Before starting a non-trivial task, state the target outcome, current situation, scope, execution plan, verification standard, risks, and confirmation points.
- After the user explicitly confirms the plan, execute the confirmed scope continuously without repeatedly asking for approval on ordinary implementation details.
- Do not expand the confirmed scope, add adjacent features, introduce new management layers, or change data semantics without discussing the expansion with the user and receiving explicit confirmation.
- Pause and ask again when the scope must expand, a destructive or sensitive operation is required, user changes conflict with the task, an external side effect was not covered by the plan, a data-semantic decision cannot be inferred safely, or execution is blocked.
- At task completion, report the actual changes, verification results, residual risks, and any intentionally unimplemented items.
- Treat every file edit as saved immediately; do not leave requested code changes only in an unreconciled working buffer.
- After completing a requested change, inspect `git status` and `git diff` before committing.
- Run the most relevant tests, checks, or smoke verification before committing. Do not commit a change that has not been verified unless the user explicitly asks for an unverified checkpoint.
- Stage only files changed for the current request. Never stage `.env` files, credentials, tokens, private keys, or unrelated user changes.
- If unrelated pre-existing changes are present, leave them untouched and do not include them in the commit.
- Create a focused commit after verification. Use a concise message describing the change.
- The repository `post-commit` hook automatically pushes successful commits on `main` to `origin/main`. Do not run a second push unless the hook fails.
- If commit or push fails, preserve the local changes and report the exact failure; do not create duplicate commits or silently retry with a different remote.

## Provider Verification

- A provider adapter existing in code is not evidence that the provider is production-ready.
- User-provided runnable probes, scripts, or adapters with observed successful results are source behavior contracts.
- Migrating a verified script into a formal Provider must preserve request parameters, headers, Referer, Session behavior, proxy handling, retries, backoff, host fallback, response parsing, and business-validity checks.
- Do not replace a verified script's transport layer merely to reuse a project abstraction without discussing it with the user and proving behavioral equivalence.
- Before labeling a source unavailable, compare the original script and formal Provider request/response behavior; a migration mismatch is an implementation defect, not source evidence.
- Keep an explicit original-vs-Provider comparison for request URL, parameters, headers, status, field set, row count, returned window, and failure classification.
- Validate providers in four layers: offline Provider Contract fixtures, small live probes, semantic verification, and end-to-end data production verification.
- Live probes must use a small symbol set and must not write production data directories.
- Persist probe evidence with provider, endpoint, request scope, response status, returned window, field semantics, units, capability version, validation time, expiry, and routing eligibility.
- Do not enable a provider in formal routing until the matching capability is evidence-backed, unexpired, within its configured scope, and not cooling down.
- Keep disabled or unimplemented providers explicitly marked; configuration declarations alone do not count as implementations.

## Development Structure

- Modify the existing architecture modules before creating new modules.
- Do not create parallel implementations, compatibility shims, or temporary duplicate entry points unless an external contract requires them and the exception is documented.
- Do not create a new service, manager, registry, backend, UI, configuration system, or abstraction layer merely to organize an existing feature unless that expansion is explicitly discussed and approved by the user.
- If the requested work can be completed with existing configuration, metadata, routing, or storage modules, prefer that minimal implementation.
- New provider functionality belongs under `providers/`; routing under `routing/`; normalization and quality rules under `quality/`; production flows under `pipeline/`; persistence under `storage/`.
- Update `CODE_STRUCTURE.md` whenever a file is added, moved, merged, or its responsibility changes.

## 文档管理

- `provider_validation/` 是正式功能实现前的来源可行性验证区，保存验证脚本、接口原始返回、解析证据和可用性结论。某接口进入正式业务开发后，业务实现放在 `src/`，实现测试放在 `tests/`，正式说明放在 `docs/`；业务原始响应、联调、来源复核、质量检查和端到端验收产物不得再写入 `provider_validation/`。正式运行使用 `data/`，隔离回放与故障测试使用 `tmp/` 下的独立运行根目录。
- 正式开发可以只读引用已有接口验证证据；现存历史材料保留原路径和哈希，搬迁或删除另列清单确认。新增接口在进入正式功能实现前仍可在验证区做可行性探针，不以“验证”命名将已经开发的业务流程输出回该区。
- raw 的活动结果按“来源、接口、数据所属日期、请求范围”唯一；不同日期分别保存，同一天重做仅更新该日期和范围。先在 `data/raw/_tmp/` 暂存，完整流程成功后更新对应日期的 raw；数据日期、请求窗口和实际采集时间分别记录，原始响应字节保持可核验。此规则是实现约束，文档必须注明尚未落地的代码差距。
- 阶段成果必须关联实际入口、参数范围、任务状态、自动生成的运行文件和验收结果，分别标明代码实现、离线验证、真实业务运行及正式发布状态；手工接口取数或验证报告不能替代正式任务验收。
- 正式功能说明、接口说明和使用文档按现有模块职责归入 `docs/`，不使用开发批次或阶段编号作为长期目录分类。
- `docs/providers/` 保存源头采集接口、参数、来源字段、采集限制和接入状态；`docs/routing/` 保存来源选择、补缺与回退；`docs/quality/` 保存标准字段、归一化与质量规则；`docs/pipeline/` 保存数据构建、合并、重做与发布；`docs/storage/` 保存存储与归档；`docs/service/` 保存面向业务的数据查询接口；`docs/worker/` 保存任务调度、执行与恢复说明。
- 按实际文档需要创建目录，不预建空目录。跨模块总体设计可放在 `docs/` 根目录，并链接各模块说明；现有根目录文档保持原位，未经确认不集中搬迁。
- `provider_validation/` 保存正式实现前的验证脚本、验证方法、测试覆盖报告、原始响应及派生证据；正式能力说明保存于 `docs/`，通过链接只读引用历史验证证据，不复制或替换原始证据。
- `README.md` 维护项目入口和文档导航，`CODE_STRUCTURE.md` 维护目录与文件职责；新增、移动文档或改变职责时同步更新对应入口，链接只指向已存在的文件，规划中的目录或文件明确标为待建立。
- YAML 与可执行代码是配置和实现依据，Excel、CSV、JSON 等说明文件是派生文档；生成说明时保留来源路径、版本及验证范围，不能把编辑说明表当作修改运行配置。
- 接口说明必须区分已实现能力、实际验证范围、生产路由资格和调度启用状态；源头采集接口与面向业务的数据服务接口分别归档。

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
