# Stock Data Manage

证券数据采集与管理项目。当前源头适配和基础流程已有实现，正式全量证券名单与在线发布仍待验收；状态见 [项目进度](PROJECT_PROGRESS.md)。

| 需要了解什么 | 文档入口 |
|---|---|
| 总体架构、术语和模块边界 | [总体设计](STOCK_ANALYSIS_V2_MARKET_DATA_CENTER_ARCHITECTURE_V1.md) |
| 现有目录和代码负责什么 | [代码结构](CODE_STRUCTURE.md) |
| 输入参数、采集字段、映射及范围 | [源头接口说明](docs/providers/README.md)、[股票与ETF来源](docs/providers/security-catalog.md) |
| 原始暂存、检查、发布、重做和证券清单调度 | [任务流程](docs/pipeline/collection-tasks.md) |
| 数据在哪里、日期如何区分和归档 | [存储说明](docs/storage/README.md) |
| 来源资格、证据与归一化设计 | [来源专项设计](provider-capability-verification-and-normalization.md) |
| 前复权变化后如何重建历史 | [历史重建设计](stock-data-design-qfq-history-rebuild.md) |
| 怎么判断功能完成和数据可用 | [验收基线](stock-data-test-design-acceptance-baseline.md) |
| 开发协作与文档维护要求 | [AGENTS.md](AGENTS.md) |

[2026-10-03历史能力矩阵](PROVIDER_CAPABILITY_MATRIX.md)只用于追溯旧样本和证据，不作为当前能力入口。

## 目录与文档导航

目录和文件的详细职责见 [代码目录与文件职责](CODE_STRUCTURE.md)，文档归属规则见 [项目协作要求中的文档管理](AGENTS.md#文档管理)。

| 目录或文件 | 用途 |
|---|---|
| `src/` | 按 providers、routing、quality、pipeline、storage、service、worker 等职责组织的正式实现 |
| `config/` | 来源参数、采集频率、路由、数据集字段模板及归一化映射配置 |
| `tests/` | 实现的自动化测试 |
| `docs/` | 按模块职责归档的正式说明；各子目录随文档产生建立 |
| `provider_validation/` | 正式实现前的来源可行性验证脚本、原始返回、覆盖报告及结论；业务开发后只读引用已有证据，入口见 [Provider 验证目录说明](provider_validation/README.md) |
| `AGENTS.md` | 协作、验证及文档管理约定 |
| `data/` | 统一运行根目录：`raw/`、`task_workspace/`、`canonical/`、`task_archive/`、`metadata/` 和 `hot/`；按需生成，不提交 Git，详见 [存储与归档说明](docs/storage/README.md) |
| `tmp/` | 本地临时工作、正式实现的隔离回放、联调与故障验收；按独立运行根目录分层输出，不能冒充真实生产数据 |
| `tmp_test/` | 历史研究脚本及输出，保留历史引用；新来源验证统一进入 `provider_validation/` |
| `CODE_STRUCTURE.md` | 当前代码和文档目录的职责索引 |

正式文档的归属如下。当前已建立源头接口、任务流程和存储文档；证券清单调度并入任务流程。其他模块目录按需要建立，现有总体及专项设计保持原位，入口见本文开头。

| 文档目录 | 内容 |
|---|---|
| `docs/providers/` | 源头采集接口、调用参数、来源字段、采集限制及接入状态 |
| `docs/routing/` | 来源选择、补缺与回退 |
| `docs/quality/` | 标准字段、归一化与质量检查 |
| `docs/pipeline/` | 数据构建、合并、重做与发布 |
| `docs/storage/` | 数据存储与归档 |
| `docs/service/` | 面向业务的数据查询接口 |
| `docs/worker/` | 任务调度、执行与恢复 |

正式说明表引用当前代码、YAML 和验证证据；修改说明表不会修改运行配置。能力已实现、已验证的范围、生产路由资格与调度启用状态分别记录。

正式代码直接放在 `src/` 下，模块按职责分目录，不再套一层项目同名目录。Python 导入名仍为 `stock_data_manage`，由 `pyproject.toml` 将该包映射到 `src/`；`stock_data_manage.providers` 对应 `src/providers/`。根目录 `config/` 保存 YAML，`src/config/` 保存读取和校验配置的代码。

来源可行性证据位于 `provider_validation/results/`。通用输入入口 `collect-input` 默认将原响应保存到 `data/raw/`，来源解析和标准化结果保存到 `data/task_workspace/`，元数据库保存到 `data/metadata/metadata.duckdb`。已实现的来源输入仍为候选数据；新增 `collect-task` 先对证券主数据和单交易日日线接通 `raw/_tmp`、检查、重做及发布收尾，正式执行默认使用配置中的 `data/`，目前仍受来源资格检查阻断；离线回放须显式指定独立的 `--data-root`，业务任务输出禁止放入 `provider_validation/`。尚未在线启用。详见 [任务流程](docs/pipeline/collection-tasks.md) 和 [存储与归档说明](docs/storage/README.md)。显式 `--output-root` 保留隔离验证布局；历史报告中的旧路径及哈希不改写。

2026-10-09确认的开发边界进一步限定：`provider_validation/` 仅服务正式实现前的接口可行性确认；进入业务开发后，业务原始响应、Provider联调、来源复核及端到端验收都使用 `data/` 或 `tmp/` 下独立运行布局，不向该验证区新增产物。历史证据只读保留，业务代码位于 `src/`、实现测试位于 `tests/`。

raw按“来源＋接口＋数据所属日期＋请求范围”每天唯一：不同日期保留，同日重做只更新目标范围。当前代码仍有独立采集按UTC抓取日生成多批次、业务任务转正路径缺日期、显式输出可回写验证区等差距，尚未修改。现有生产元数据库业务任务为0、未发布正式全量名单；10月9日手工来源候选不算真实任务完成。本次仅对齐设计、目录边界和验收文档，下一阶段需修改现有路径及任务摘要，再按正式入口验收。进度与代码差距见 [项目进度](PROJECT_PROGRESS.md)。

## 当前开发状态

第一批基础能力已实现：

- Provider Capability 的范围、有效期、角色和吞吐量筛选；
- 日线/分钟统一领域模型与 Canonical 主键；
- 基于明确状态的 Missing Set；
- Provider 维度的单位和分钟时间戳标准化；
- OHLC、成交量、时间键校验；
- final/provisional、字段完整度和来源优先级仲裁；
- soft/hard conflict 识别及单条隔离；
- Collection Attempt 状态机、租约过期和 Raw 复用判定；
- 确定性 row hash、文件 manifest 生成和篡改校验；
- 按 A 股 Session 边界从完整 1m 派生 5m；
- SQLite WAL Hot Minute UPSERT、质量降级保护和即时查询。
- DuckDB Partition/Item/Attempt/Conflict 元数据；
- 不覆盖的 Raw Object Store；
- Canonical Parquet 合并、文件锁、Manifest 与原子替换；
- 中断后的临时文件隔离、发布续接和元数据恢复；
- Fixture Provider 驱动的日线端到端 Missing Set/Fallback 链路。
- Provider Contract 对 HTML 200、403/429、5xx、空返回、Schema 漂移和静默截断分类；
- Capability Probe 证据与有效期持久化；
- Endpoint/市场/资产粒度的失败计数、冷却及 Probe 恢复门槛；
- 按天数和最大行数约束拆分历史请求窗口；
- Sina、Tencent 日线 HTTP 请求与响应解析适配器。
- A 股连续竞价 Session 配置与 240 根 Expected Minute 计算；
- Watchlist 实时分钟吞吐预算、闭合延迟过滤和 Hot Store 写入；
- 累计快照差分生成 provisional 1m，并标记采样不足/跨日重置；
- Canonical + Hot 统一分钟查询；
- 盘后 Minute Reconciliation、final 提升、冲突隔离和分钟完整性统计。
- Phase 1 配置化任务时间表与按日幂等的 due-job 调度器。
- Security Master 与 Trading Calendar 更新服务，支持来源缺失保留、历史变更审计和官方优先级。
- Tencent 原生 1m、Sina 原生 5m 与可注入 TDX 延迟 1m 适配器；TDX 因公共主站不稳定默认禁用。
- Tencent/Sina 批量快照文本响应适配器，保留原始成交量/成交额单位标记供后续 Normalization Rule 使用。
- 收盘快照到 provisional 日线的构建服务，单位换算和来源语义由 Normalization Rule 控制。
- 日线 Reconciliation：按主键合并 provisional/final、提升质量状态并记录缺失与冲突。
- 独立的 `config/schedules.yaml` 调度配置；已禁用的 TDX 延迟分钟任务不会进入运行计划。

默认测试只使用离线 Fixture，不会访问第三方行情接口；显式执行 `probe-*` 命令才会访问对应来源，探针证据不写入生产数据目录。

显式执行一次小流量能力探针（会访问对应行情来源）：

```powershell
python -m stock_data_manage.cli probe-daily --provider sina --symbol sh600519 --trade-date 2026-09-11
```

分钟来源探针：

```powershell
python -m stock_data_manage.cli probe-minute --provider tencent --symbol sh600519 --frequency 1 --as-of 2026-09-11T13:02:00+08:00
```

进程中断后的 Canonical 与元数据恢复：

```powershell
python -m stock_data_manage.cli recover --canonical-root data/canonical --metadata data/metadata/metadata.duckdb
```

运行可重复的离线 M1 验收证据：

```powershell
python -m stock_data_manage.cli acceptance-offline --root tmp/acceptance-m1 --output tmp/acceptance-m1/report.json
```

该报告覆盖 20 个工作日回放、Canonical 幂等、断电恢复和 200 只 Watchlist 容量；旧 Provider Probe 样本见 [历史能力矩阵](PROVIDER_CAPABILITY_MATRIX.md)，当前资格不能由旧矩阵推导，连续交易日试运行仍需在目标运行环境执行。

## 本地验证

首次使用先安装当前项目：

```powershell
python -m pip install -e .
```

安装登记现有源码及 `stock-data` 命令入口。执行命令、测试和来源验证脚本时使用同一个 Python 环境；不再依靠将 `src/` 添加到 `PYTHONPATH` 启动。目录迁移后，已有开发环境也需重新执行上述安装命令。需要安装测试依赖时使用 `python -m pip install -e ".[test]"`。

```powershell
python -m pytest
```

当前实现已覆盖 Phase 1 的离线核心链路与 Provider 契约适配；Tencent、Sina、BaoStock、THS、EastMoney 分红事件/证券列表/实时行情、AkShare 日线适配器已按来源拆分。数据集标准定义位于 `config/datasets/`，数据集专用归一化规则位于 `config/normalization/`，Endpoint 静态配置位于 `config/providers.yaml`，能力路线位于 `config/capabilities.yaml`。AkShare 是可选依赖，建议使用 `python -m pip install -e .[akshare]` 安装；当前股票、ETF、LOF、指数代表样本和 THS 行业/概念代表板块已完成真实探针，但仍保持 `validation_only`。EastMoney 实时行情和证券列表本次正式探针为 `RemoteDisconnected`，前复权完整历史来源、TDX 真实客户端、EastMoney 分红事件/证券列表/实时行情完整覆盖仍未完成。未验证或未实现的来源不会自动进入生产候选。
