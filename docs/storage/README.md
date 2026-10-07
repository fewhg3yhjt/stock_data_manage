# 运行数据目录与归档

新增业务任务入口的暂存、重做和发布规则见 [任务流程](../pipeline/collection-tasks.md)。下文原 `collect-input` 候选布局继续适用；`collect-task` 对证券主数据和日线使用以下扩展：

```text
data/
├── raw/_tmp/<任务ID>/<单元哈希>/     # 本次尚未转正的完整响应
├── raw/<来源>/<端点>/scope-<哈希>/   # 每个请求范围只有一个当前结果
├── task_workspace/_tasks/<任务ID>/prepared.parquet
├── canonical/security_master/current/{data.parquet,manifest.json}
├── canonical/daily_bar/asset_type=<类型>/trade_date=<日期>/
├── task_archive/_raw_evidence/<清单哈希>/  # 正式记录的稳定证据引用
├── task_archive/_raw_audit/<标识>/         # 重做清空、旧当前响应的审计留存
└── metadata/metadata.duckdb               # 自动维护任务阶段、单元和提交记录
```

这里的“清空”是从活动 `_tmp` 中移走，保留原字节证据；审计归档不作为当前输入版本供业务选择。正式记录引用稳定证据，避免下一次更新当前 raw 后旧记录无法追溯。在同一磁盘内用硬链接留存相同文件，不重复复制响应字节；后续目录转正只重命名、不修改响应内容。归档暂无自动清理，修改过的新响应仍会增加审计占用。`_raw_audit` 也可保存被清空的本任务构建文件，属于执行证据。

业务任务完成后，采集报告及来源候选仍保留在工作区，业务任务记录已标记发布。它们当前不会自动迁入旧 `archive_published_task` 布局，避免改写报告中的历史引用；自动归档与保留期限暂未扩展。未成功来源的暂存保留以便重做。其他63种来源数据集没有套用日线的覆盖和发布规则。

来源报告中的 `_tmp` 路径表示采集当时的位置，不随转正改写。最终原始位置由 DuckDB 任务单元的 `raw_manifest`（稳定证据）与 `current_raw`（当前结果）记录；任务输出 JSON 也包含这组关联。追溯已发布记录时从正式清单的 `raw_refs` 进入，按源响应哈希关联原始请求、来源报告和处理代码版本。

正式运行产物统一由 [collection.yaml](../../config/collection.yaml) 的 `storage` 配置指定，默认根目录为 `data/`。配置读取代码位于 [现有配置模块](../../src/config/loader.py)，单项输入和周期采集共用 [现有采集流程](../../src/pipeline/inputs.py)。本次没有增加存储服务或独立配置系统。

## 目录及职责

```text
data/
├── raw/<来源>/<端点>/<UTC抓取日期>/<批次>/
│   ├── bodies/<SHA256>.bin
│   └── manifest.ndjson
├── task_workspace/<数据集>/scope-<请求范围哈希>/<任务ID>/
│   ├── started.json
│   ├── sources/<来源>/<输入ID>/
│   │   ├── raw_refs.json
│   │   ├── parsed/                 # 原列、解析及必要的排除行
│   │   ├── normalized.json
│   │   └── normalized.parquet
│   ├── report.json
│   ├── quality_report.json
│   └── manifest.json
├── task_workspace/_scheduler/      # 周期计划和执行摘要
├── canonical/<数据集>/<既有业务分区>/
├── task_archive/<数据集>/scope-<请求范围哈希>/<任务ID>/
├── metadata/metadata.duckdb
└── hot/minute_hot.db
```

目录按实际写入需要生成；未生成 `canonical/` 或 `task_archive/` 不代表路径没有定义。来源原始响应与来源标准化结果按来源区分，最终数据按数据集统一保存，来源通过原始证据、标准记录和发布清单追溯。

`scope-...` 根据脱敏后的绑定参数生成，代表请求范围，不是交易日期。交易日、公告日、统计月和报告期需要各数据集自己的发布分区规则，抓取日期不能替代这些业务日期。运行任务ID使用短随机批次，输入ID保存在任务清单和来源目录中，避免 Windows 深目录重复标识造成长路径失败；显式验证输出继续保留原输入ID前缀。重复请求不会覆盖旧任务。

各层职责如下：

| 层级 | 保存内容与当前行为 |
|---|---|
| 原始层 `raw/` | 解析前保存精确应用响应字节及请求/响应清单；原有脱敏、缓存、重试和 SDK 会话行为保留。SDK 不暴露底层传输字节时继续明确标注可见证据边界 |
| 中间层 `task_workspace/` | 原列、解析结果、来源字段映射、精确类型的 Parquet、质量报告、原始响应引用和任务清单。当前64项输入成功后为候选完成，不等于正式发布 |
| 最终层 `canonical/` | 保留已有标准 Bar 的发布和查询格式。本轮没有为63种输入数据集另造合并、覆盖或业务完成规则，也没有把候选文件直接复制为最终数据 |
| 归档 `task_archive/` | 只有正式发布完成的任务可以转入。保留整个任务及其原始响应引用；不重复搬运 `raw/`。候选、失败、中断任务留在工作区 |
| 元数据 `metadata/` | 复用已有 DuckDB，索引采集状态、原始证据路径及哈希、调度幂等占用、质量与既有分区发布状态。它不存储市场行情主体数据 |
| 分钟热数据 `hot/` | 保留现有实时分钟 SQLite 数据及查询用途，不替代最终层 |

多来源仲裁后的 `candidate/` 文件由后续正式数据构建流程生成，本轮单来源采集不创建空候选合并结果。

## 输入、输出与验证位置

输入参数来源仍由 [providers.yaml](../../config/providers.yaml) 定义；频率由 `collection.yaml` 定义；字段模板、必填字段及主键来自 `config/datasets/`，字段映射来自 `config/normalization/`。命令参数或 JSON 上下文提供实际请求范围，不会改变字段含义。

默认 `collect-input` 和 `collect-due-inputs` 使用上述运行目录。`--data-root` 可整体切换运行根目录，各层相对位置一起切换；同时指定 `--data-root` 和 `--output-root` 会拒绝执行。各存储层必须位于根目录内，不能相互嵌套或通过路径链接逃逸。

显式 `--output-root` 继续表示隔离验证模式，保留已验证脚本依赖的旧 `输入ID-批次/_raw/` 布局。这是已有脚本及历史报告的外部路径契约，同一个采集与映射流程只调整落盘位置，没有第二套 Provider。验证输出不能放进配置指定的正式 `data/`。

```powershell
# 默认运行目录中的离线回放：不会获得生产路由资格或发布最终数据。
python -m stock_data_manage.cli collect-input --input ASTOCK-002-daily `
  --symbol 600519 --start-date 2026-09-01 --end-date 2026-09-18 `
  --replay-manifest provider_validation/results/raw/2026-10-01-v39-live-escalated/manifest.ndjson

# 使用同一分层布局在隔离目录验收。
python -m stock_data_manage.cli collect-input --input ASTOCK-002-daily `
  --symbol 600519 --start-date 2026-09-01 --end-date 2026-09-18 `
  --data-root tmp/storage-check/data `
  --replay-manifest provider_validation/results/raw/2026-10-01-v39-live-escalated/manifest.ndjson
```

运行中的 `report.json`、`raw_refs.json` 和任务清单所用文件路径以任务根目录为基准；原始响应引用允许指向同一 `data/` 下的 `raw/`。响应清单中的 `body_storage` 则始终以响应清单所在目录为基准。默认工作区与归档区深度相同，移入归档后引用继续有效；自定义布局若导致引用失效，归档会拒绝。

历史来源可行性证据仍位于 `provider_validation/results/`，覆盖表仍位于 `provider_validation/coverage/`。正式接口说明仍位于 [docs/providers/](../providers/README.md)。这些历史材料没有搬进运行数据区，表内的历史证据路径也没有改写为当前输出路径。

正式运行时的采集与质量记录位于任务工作区，发布后随任务进入归档；本轮64项验证产物特意保存于 [隔离验收目录](../../provider_validation/results/storage-layout-20261005/)，属于开发验收证据，不是实际生产数据。

## 发布与归档门禁

任务执行复用已有采集尝试状态（Collection Attempt）：请求前占用并保存 `fetching`，保存原始响应后记录 `raw_committed`，成功生成来源标准文件后转为 `normalized`，候选检查完成后记录 `validated`。来源或转换失败保存失败证据，进入既有失败或隔离状态；中断不会自动视为发布成功。

当前64项输入不授予生产路由资格、不自动开启调度、不扩大市场覆盖；未独立核准的单位继续使标准字段置空，原值留在原始层与解析结果中。质量报告显式记录 `publication_permitted: false`。

归档实现位于 [现有恢复模块](../../src/worker/recovery.py)，可由主流程调用 `archive_published_task`，也可以通过现有恢复命令显式选择任务：

```powershell
python -m stock_data_manage.cli recover --archive-task data/task_workspace/<数据集>/<范围>/<任务ID>
```

该命令先执行既有分区恢复，再核对所选任务。任务清单和 DuckDB 状态必须均为 `published`；原始清单、响应体、来源输出、质量报告、采集报告、最终分区清单及数据文件必须完整且哈希匹配。归档拒绝候选任务、损坏文件、路径越界、已有目标目录及会破坏原始引用的布局。移动使用同一文件系统的原子目录重命名，跨文件系统失败时保留工作区，不进行复制后删除。

本轮未对已有用户数据执行搬迁、覆盖或清理。原配置中的保存天数不新增自动清理动作。`metadata/metadata.duckdb` 的默认位置已统一为 `data/metadata/metadata.duckdb`；验收前未发现旧数据库，需要处理已有数据库的部署应另做明确迁移，不自动覆盖目标。

仍需后续接通的工作是各数据集的多来源候选构建、业务完成标准与正式发布，再在发布成功后调用归档。已有日线和分钟发布行为不因这次目录统一而扩大适用范围。
