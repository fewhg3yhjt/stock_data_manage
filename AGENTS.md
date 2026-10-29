# Project Agent Requirements

## Change And Delivery Workflow

- Before starting a non-trivial task, state the target outcome, current situation, scope, execution plan, verification standard, risks, and confirmation points.
- After the user explicitly confirms the plan, execute the confirmed scope continuously without repeatedly asking for approval on ordinary implementation details.
- 按“明确设计与职责合同 → 按设计实现 → 按验收标准验证 → 对照设计复核 → 更新 `PROJECT_PROGRESS.md`”完成开发闭环。每完成一个已确认的功能或阶段，都在该阶段收尾时同步更新现有进度文档，记录实际范围、验证层级与证据、实现是否符合设计、偏差/风险及剩余事项；不等到多个阶段全部结束后再补记。
- 设计对照发现偏差时，若能在已确认范围内修正，应先修复并重新验证，再记录完成；若需要改变设计、数据语义或范围，先说明偏差及影响并取得用户确认，不得把偏差静默记为完成。
- Do not expand the confirmed scope, add adjacent features, introduce new management layers, or change data semantics without discussing the expansion with the user and receiving explicit confirmation.
- Pause and ask again when the scope must expand, a destructive or sensitive operation is required, user changes conflict with the task, an external side effect was not covered by the plan, a data-semantic decision cannot be inferred safely, or execution is blocked.
- At task completion, report the actual changes, verification results, residual risks, and any intentionally unimplemented items.
- Treat every file edit as saved immediately; do not leave requested code changes only in an unreconciled working buffer.
- After completing a requested change, inspect `git status` and `git diff` before committing.
- Run the most relevant tests, checks, or smoke verification before committing. Do not commit a change that has not been verified unless the user explicitly asks for an unverified checkpoint.
- Stage only files changed for the current request. Never stage `.env` files, credentials, tokens, private keys, or unrelated user changes.
- If unrelated pre-existing changes are present, leave them untouched and do not include them in the commit.
- Create a focused commit after verification. Use a concise message describing the change.
- Once the user explicitly approves a defined change or confirms its implementation scope, complete and verify that scope, then commit and push it without asking for another approval. This authorization covers only the approved scope; leave unrelated or unverified working-tree changes untouched.
- Check whether the repository has an active `post-commit` push hook. If it does, do not push a second time unless the hook fails. If no such hook is active, push the authorized commit to its intended remote directly.
- If commit or push fails, preserve the local changes and report the exact failure; do not create duplicate commits or silently retry with a different remote.

## 项目Python环境与测试执行

- 项目 `pyproject.toml` 要求 Python `>=3.11`。每次测试前必须在即将运行测试的同一个解释器中记录 `sys.executable`、Python版本，并检查该次测试所需依赖；不能仅凭已激活环境、其他终端或机器上某个环境安装了依赖，就假定当前解释器可用。
- 最近核验可完整运行本项目定向测试的解释器是 `D:\Program Files\Python\python.exe`（Python 3.13.0rc3）；该解释器同时具备 pytest、FastAPI、DuckDB、PyArrow、PyYAML 和 requests，并满足项目最低版本要求。运行项目测试时显式调用该绝对路径。它是预发布版本；若之后发现依赖齐全的稳定版项目环境，应先核对并更新本条约定。
- Windows机器级 `PATH` 已包含 `D:\Program Files\Python`，无需为了此次问题修改系统 PATH。但Codex的PowerShell执行进程在登录与非登录模式下都未继承任何Python/Conda PATH项，虽然进程变量 `CONDA_PREFIX` 指向 `D:\Program Files\miniconda3`；因此 `Get-Command python` 无法解析。Codex命令必须显式使用上面的解释器路径，不以进程默认 `python` 或 `CONDA_PREFIX` 推断实际解释器。
- `D:\tools\miniconda3\python.exe`（Python 3.13.12）有 pytest、DuckDB、PyArrow、PyYAML 和 requests，但没有 FastAPI；它是核心测试依赖不完整的另一解释器，不作为完整日线测试的默认环境。
- `D:\Program Files\miniconda3\python.exe`（Python 3.9.21）有 FastAPI，但低于项目要求，且没有 pytest、DuckDB 和 PyArrow；不要用它运行项目测试。`D:\Program Files\miniconda3\envs\ailearn\python.exe`（Python 3.11.15）有 pytest 和 FastAPI，但缺少 DuckDB 与 PyArrow，也不能单独运行完整日线测试。
- 不要混用不同Python版本的 `site-packages`，也不要为绕过环境差异擅自安装依赖。若上述完整解释器不可用，准确报告解释器、已验证的依赖和未能运行的测试；只有用户确认环境安装或调整后再补跑。
- 以上路径和包状态是最近一次实机核验结果，不视为永久状态。后续每次任务仍须复核当前解释器与依赖，并优先遵守用户明确指定的项目环境。

## 需求与方案思考

- 讨论需求、报告、日志、数据产物或架构改造时，先回答“是什么、为什么、怎么样”：说明对象和用途、当前问题及必要性、拟议实现及其理由。
- 明确谁会读取或消费产物、由什么入口生成、与现有报告/原始证据/运行日志的关系，以及它解决的具体问题。先检查现有机制是否已经满足需求，避免只因某种格式或抽象可用就新增文件、服务或流程。
- 项目运行时的标识和行为必须由项目自身的任务、配置及输入定义；不得让采集、存储、日志或恢复依赖 Codex/Agent 对话标题、线程标识或其他协作环境元数据，除非用户明确提出并批准了产品级接口需求。
- 评估方案本身是否必要，并说明成本、重复信息、维护负担和验证方式。若现有产物足够，应直接说明无需新增；若保留机器记录并新增人读报告，必须指出各自独立消费者及为什么不能由现有记录承担，避免形成两份互相漂移的事实来源。
- 报告的组织粒度应由用户任务和实际阅读场景决定。不同 Provider 有不同接口不自动意味着每个 Provider 都要一份 Markdown；说明采用单份任务报告、按来源分节或独立报告的依据。
- 区分原始来源响应、结构化机器审计记录、面向人的报告和执行日志。说明每类记录保存什么、供谁使用、是否已经存在；不得把文件指纹或机器字段称作业务数据或运行日志。
- 面向用户解释配置、来源、任务入口和内部状态时，必须同时说明其业务含义及对用户可见行为的影响；首次出现 `source_inputs`、资格门禁、覆盖组等内部术语时，紧接着用具体例子解释“系统因此会/不会做什么”，不能只报文件名、字段名或代码状态。
- 说明任务被阻断或能力未启用时，区分阻断阶段、直接原因和后续影响，并检查项目是否已有可复用的解决能力（例如交易日历获取/导入）；给出最小可行的下一步及其副作用，不把缺少运行证据误说成缺少代码能力，也不把“实现了”误说成“本次已运行/可生产”。
- 需要用户判断、确认方案或解释阻断时，先把问题讲成可核验的事实：业务目标和对象、当前入口及实现顺序、实际输入（配置、请求参数、范围和日期）、实际输出（原始响应、处理结果或错误及发生阶段）、对下游行为的影响。至少提供一个对应本项目原始证据的真实数据示例及可打开路径；明确区分已观察事实、推断和仍未知的原因。不能只报字段/状态名或概括性结论后就要求用户判断。
- 先从用户目标描述端到端行为，再解释配置如何决定该行为；避免把“某配置未选入某来源”作为孤立结论，必须说明它意味着该任务实际会跳过什么、为什么当前配置如此，以及改动或补齐后会发生什么。
- 持久化文件和目录必须使用能表达业务内容、日期或范围的可读名称；不得将 UUID、随机串或内容哈希作为唯一可见文件名。随机ID可用于数据库主键、并发控制及短暂原子写入临时文件；被替换内容需要保留时，审计路径也须使用可读来源/日期/任务标识并处理同名冲突。
- 方案说明按“是什么、为什么、怎么样、必要性与取舍”组织，再列实施范围、验证标准、风险和检查点。没有用户确认时，不把讨论中的方案当作已批准的实施范围。

## 设计先行与领域建模

- 正式开发遵循“明确业务目标与对象 → 明确职责和配置合同 → 确认端到端任务流程 → 实现 → 验证”的顺序。新增能力或架构调整前，先检查现有设计及实现差距，在已有设计文档中说明对象关系、成功与失败路径、数据落点、恢复行为和验收标准，经用户确认后实施。已确认设计范围内的常规实现和缺陷修复连续执行，不机械重复完整设计或要求再次批准。
- 不采用面向零散功能堆分支的开发方式：不得因为增加一个接口、命令或页面，就直接在中央流程持续追加来源判断、数据集特例或重复执行逻辑。先判断需求属于哪个领域对象和职责，再修改对应模块；不能用“先把功能做出来，后面再设计”替代正式设计。
- 明确区分数据集、来源输入、任务定义、调度规则、任务运行实例、数据分区与发布结果。数据集定义数据内容与质量，来源输入定义获取合同，任务定义生产目标，调度规则定义何时触发，运行实例记录某次实际执行，发布结果记录交付的数据；不能把一次接口调用等同于业务任务，也不能把候选完成等同于正式发布。
- YAML 管理声明式策略：数据字段、主键、分区与发布门槛，来源参数与限速，字段映射，任务范围与来源策略，以及调度频率与日历。优先明确现有配置的职责和引用关系；调度引用任务，任务引用数据集及来源策略，避免同一规则在多份配置或代码中重复维护。配置声明必须有对应执行实现与校验，不因写入 YAML 就视为能力完成。
- 代码实现协议、解析、业务构建与恢复等行为，不把 YAML 扩展为包含任意脚本和复杂控制流的编程语言。运行状态、断点、资格与健康信息保存在现有运行记录和元数据中，不回写 YAML。
- 调度器负责到期判断与触发；任务执行器负责运行生命周期、互斥、断点与恢复；来源适配器负责来源传输及解析；数据构建模块负责标准候选的业务合并、补缺与检查；存储模块负责持久化与发布。来源专属规则不散落到通用执行器，数据集差异不靠中央函数无限扩充分支承载。
- 任务首次执行时解析并持久化目标日期、证券范围、来源策略及相关规则版本；续跑使用已冻结的运行定义，不因名单或当前配置变化静默扩大、缩小或替换任务范围。需要变更范围或规则时，显式生成新运行或按经确认的重做规则处理；运行记录中的原始目标和证据保持可追溯。
- 面向对象设计以职责、合同和组合关系为依据，优先复用现有模型与模块；不以类数量、继承层次或框架数量衡量设计质量。不为统一外观建立庞大父类、通用管理器或另一套任务系统。验收应证明配置可驱动行为、职责可独立验证、恢复范围稳定及生产结果可追溯。
- 可行性测试允许在正式设计确认前，以最小探针验证接口、传输或数据语义，但必须明确测试问题、范围和证据，遵守既有验证区及业务运行目录边界，不写生产数据、不自动启用正式路由或调度。探针成功不等于正式功能完成；转入正式开发时仍须先确认设计与任务流程，不将探针直接扩展成平行业务入口。

## Provider Verification

- Treat AI-generated analyses, code, summaries, and data transformations as unverified hypotheses until independently checked against source data, executable code, authoritative documentation, or reproducible validation.
- Preserve evidence at every investigation stage: the capability inventory, source requests, original responses, parsed outputs, validation findings, and final conclusions must be saved and linked to one another. Never rely on ephemeral terminal output as the only evidence.
- Before parsing, normalizing, filtering, or otherwise transforming a live provider response, persist its exact response bytes. Save a manifest with provider, endpoint, method, sanitized request parameters and headers, request scope/date/window, fetch time, status, response headers/content type/encoding, byte length, and SHA-256. Compressed storage is allowed if the exact original bytes can be recovered and their hash is recorded. If a response contains credentials or secrets, do not persist those values; save a redacted representation or hash, mark the evidence as redacted, and state why exact retention was suppressed.
- Persist each derived dataset and validation result with the source-response hash, transformation/code version, row counts, coverage denominator, and validation time. Persist transport failures and empty responses as evidence too; distinguish them from a valid empty dataset.
- Before making a live request, search the evidence archive by provider, endpoint, canonical request parameters, scope/date/window, and code version. Reuse a matching response when it satisfies the requested freshness and completeness requirements; do not repeat calls solely to recreate an artifact or summary.
- When a request fails before an HTTP response, classify it as a transport failure and leave provider support unresolved. For `ProxyError`, connection-refused, or proxy timeout, inspect the process proxy variables (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`), the Windows proxy setting, and whether the configured local listener is reachable; record the effective proxy address with credentials redacted. A proxy URL in the environment does not prove a proxy client is listening, and a local proxy failure is not evidence that the provider rejected the endpoint or parameters. Loopback (`127.0.0.1`/`localhost`) is scoped to the current network namespace: an isolated Codex process may not be able to reach a proxy listening on the Windows host. A prior case is recorded in [phase-4-adjustment-probe.md](data/task_workspace/_checks/security-daily-20261023/phase-4-adjustment-probe.md): V2RayN's host listener was reachable from host execution even though isolated execution raised `ProxyError`; its `20171` port is historical evidence, not a current setting. Do not bypass or replace the configured proxy silently; preserve the original transport contract and retry a small sample only after the proxy path is understood or changed.
- Do not report a capability as verified when only documentation, an HTTP success status, a sample, or an AI-generated summary exists. State the exact tested scope and whether evidence is live, offline, partial, or blocked.
- Keep raw probe evidence in a persistent, queryable location outside production datasets. Do not automatically delete it as temporary data. Never persist credentials, authorization tokens, private keys, or unredacted secrets in request metadata.
- A provider adapter existing in code is not evidence that the provider is production-ready.
- User-provided runnable probes, scripts, or adapters with observed successful results are source behavior contracts.
- 用户提供来源脚本后，先形成逐调用能力清单，将每项对应到现有Provider、YAML参数和字段映射、任务选源及实际验证证据。不能仅补当前被指出的接口而遗漏同一脚本的其他能力。
- 宣称“没有来源”“不能获取”或安排暂缓前，必须复查用户脚本及结果、当前YAML、可执行实现和任务选源；区分未迁移、未配置选用、日期推算限制、验证失败和真正不支持。迁移后增加能力清单回归，并在正式说明中记录每项的验证范围与生产资格。
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
- 不为假设中的兼容性创建平行实现、兼容层或临时重复入口。确有外部合同要求时，先列明实际消费者、必须保留的行为、适用范围、维护成本和退出条件，经用户确认并记录后才实施；不能仅以“可能有人使用”或“为了让旧测试通过”为依据。
- Do not create a new service, manager, registry, backend, UI, configuration system, or abstraction layer merely to organize an existing feature unless that expansion is explicitly discussed and approved by the user.
- If the requested work can be completed with existing configuration, metadata, routing, or storage modules, prefer that minimal implementation.
- New provider functionality belongs under `providers/`; routing under `routing/`; normalization and quality rules under `quality/`; production flows under `pipeline/`; persistence under `storage/`.
- Update `CODE_STRUCTURE.md` whenever a file is added, moved, merged, or its responsibility changes.

### 兼容与兜底的复杂度约束

- 不以兼容、兜底、鲁棒性或避免报错为由叠加隐式分支、双轨配置、多套解析、重复状态或无边界重试。每条额外路径必须对应已确认的业务场景；先评估能否通过修正正式合同或显式迁移解决，不能默认永久保留新旧两套行为。
- 已设计并配置的来源补缺、有限重试、整份旧数据回退和发布恢复可在其明确合同内执行；不得因此增加未经确认的自动切源、换接口、猜单位、猜日期、降质量或绕过资格与发布门槛。可选来源不存在或不满足条件时，按设计返回失败、阻断或明确降级结果，不隐藏原因或伪装成功。
- 不捕获所有异常后静默返回默认值、空数据或成功状态。恢复措施必须针对明确故障类别，保留原错误与证据，并说明对业务结果的影响；程序缺陷不得被归类为来源不可用后绕过。
- 替换旧实现时先明确保留合同、迁移范围及删除清单，经确认后收敛为一个正式执行路径。历史证据与仍需保留的数据不随代码清理删除；不能为消除旧代码而擅自修改数据语义、覆盖用户文件或迁移生产数据。
- 评审除功能正确性外，还须检查是否增加重复事实来源、隐藏默认行为、兼容分支及维护成本；不需要的扩展不实现，无法说明边界和退出条件的临时兜底不进入正式代码。

## 文档管理

- 面向人的阶段报告、方案说明和验收结论默认使用 Markdown（`.md`）；JSON、YAML、CSV 等格式用于程序读取或结构化数据。机器可读文件只有在存在明确的人类阅读场景时才配套 Markdown 说明，不因生成 JSON 就机械增加第二份报告；说明其用途、消费者、必要性及文件路径即可。
- 定期审视设计文档：每个开发阶段收尾，以及架构、目录或需求变更后，检查文档职责、重复内容、过期状态和实现差距；同一主题已经有文档时优先修改原文档，不为每个批次新增长期设计说明。
- 同一规则只保留一个主要维护位置：总体设计说明模块边界，模块文档说明具体规则，验收文档说明验证标准；其他文档通过链接引用，不复制整段设计或多处维护能力状态。
- 合并或清理前列明旧文件、独有内容的承接位置、历史结论与原始证据的区别；在用户确认的范围内执行，先保存有效内容及证据引用，再删除被替代文件。存在未确认的语义冲突时记录差异，不借文档清理改变业务规则。
- 清理后同步检查 README.md、CODE_STRUCTURE.md 和所有现行文档的引用；历史验证结论明确日期和范围，不作为当前配置或生产资格。保留原始响应及验证证据，不因删除重复说明而删除证据文件。
- 文档更新与清理是日常维护，不另建定期审查服务、自动删除任务或多套文档版本；阶段报告简要列出保留、合并、删除项和仍待确认的问题。

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
