# 代码目录与文件职责

本文维护当前目录、模块和文件职责，不记录逐批开发过程；当前完成状态见 [项目进度](PROJECT_PROGRESS.md)，历史迁移和验证解释见既有证据。下文代码文件路径均相对于 `src/`。

## 当前目录约定

- 正式代码直接位于 `src/`，不再建立 `src/stock_data_manage/` 或 `src/stockdata/`，各模块职责不变。
- `src/__init__.py` 是逻辑包 `stock_data_manage` 的初始化文件，`src/cli.py` 是命令入口。`pyproject.toml` 显式将逻辑包映射到 `src/` 并列出Python包；新增含 `__init__.py` 的子包时同步更新该清单。
- 根目录 `config/` 保存 YAML 参数、调度意图、字段模板和字段映射；`src/config/` 保存配置读取与校验代码。
- `tests/` 保存实现测试；`provider_validation/` 保存正式实现前的来源可行性验证、原返回与派生证据；`docs/` 保存按职责归档的正式说明。业务开发后不向验证区写入新代码、数据或验收结果。
- `data/` 统一保存 `raw/`、`task_workspace/`、`canonical/`、`task_archive/`、`metadata/` 和 `hot/`，按需生成；实现回放与故障测试使用 `tmp/` 下的隔离运行根目录。raw暂存及正式结果按数据日期分层，旧数据保留原位，来源候选成功不自动发布或归档，详见 [运行数据与归档](docs/storage/README.md)。
- `tmp/` 用于本地临时工作与正式实现的隔离验收。原根目录 `tmp_test/` 已撤掉，历史研究材料完整归档到 `provider_validation/results/legacy/tmp_test/`；迁移清单保留原路径、相对目录、文件哈希及旧绝对路径说明。新来源验证归入 `provider_validation/`，正式开发产物按既有业务运行布局保存。
- 测试、命令及验证脚本在安装当前项目的同一环境中运行，不再通过 `PYTHONPATH=src` 暴露顶层 `providers`、`config` 等包。历史证据中的旧路径及哈希保留原样。

## 设计模块映射

| 设计模块 | 当前目录 | 职责边界 |
|---|---|---|
| 领域模型 | `src/domain/` | 数据集、证券类型、状态、标准 Bar 记录和交易时段模型 |
| 数据源适配器 | `src/providers/` | Provider 接口、Fixture/HTTP 适配器、响应契约和能力探针 |
| 能力路由器 | `src/routing/` | 来源能力注册、缺失集合、实时采集计划和重试策略 |
| 数据质量 | `src/quality/` | 标准化、基础校验、冲突解决和发布门槛 |
| 数据生产与发布 | `src/pipeline/` | 日线、分钟、快照、重采样和盘后校准流程 |
| 数据存储 | `src/storage/` | Raw、Hot、Canonical、Metadata 和完整性证据 |
| 数据访问服务 | `src/service/` | Security Master、Trading Calendar 和统一分钟查询 |
| Worker | `src/worker/` | 调度、执行记录、恢复和离线验收 |


## 配置与文档职责

根目录 `config/`：`providers.yaml` 保存来源和输入契约，`collection.yaml` 保存参数来源、范围与刷新频率，`capabilities.yaml` 保存路由策略，`datasets/` 保存字段模板，`normalization/` 保存来源映射，`schedules.yaml` 引用任务时间表。运行状态由现有存储维护，不回写YAML。

| 文档 | 主要职责 |
|---|---|
| `README.md` | 项目安装、运行与文档导航 |
| `AGENTS.md` | 协作、验证、权限及定期文档整理要求 |
| `PROJECT_PROGRESS.md` | 当前实现、差距和下一步，不重复历史批次流水账 |
| `STOCK_ANALYSIS_V2_MARKET_DATA_CENTER_ARCHITECTURE_V1.md` | 总体架构、术语、模块边界及合并保留的分钟规则 |
| `stock-data-design-qfq-history-rebuild.md` | 前复权历史变化与人工重建规则 |
| `stock-data-test-design-acceptance-baseline.md` | 测试层次、故障场景和验收门槛 |
| `provider-capability-verification-and-normalization.md` | 来源证据、资格及YAML归一化规则 |
| `docs/providers/README.md` | 正式源头接口说明、各数据域边界及说明表依据 |
| `docs/providers/security-catalog.md` | 当前股票和ETF来源范围、日期、分类与差异 |
| `docs/pipeline/collection-tasks.md` | 获取、检查、发布、重做、证券清单调度和恢复 |
| `docs/storage/README.md` | 数据分层、日期唯一、原始引用和归档 |
| `PROVIDER_CAPABILITY_MATRIX.md` | 2026-10-03历史抽样及证据索引，不是当前能力清单 |

源头接口阅读版与同名JSON位于 `docs/providers/`，是代码、YAML和证据的派生快照，不是运行配置；`docs/pipeline/examples/` 是离线任务参数示例，不保存运行数据。合并删除的5份旧文档内容已由上述文件承接，不建立兼容副本。

目前正式模块文档目录为 `docs/providers/`、`docs/pipeline/`、`docs/storage/`。后续路由、质量、查询或Worker文档按实际需要创建，不预建空目录；当前证券清单调度在任务流程中维护，避免单独重复说明。

验证区保存正式实现前的原响应、脚本和历史结论；已有业务联调或说明生成工具错放位置作为待整改项，不据此新增业务产物。当前正式说明生成器仍为 `provider_validation/tests/build_interface_coverage.py` 和 `build_interface_coverage_workbook.mjs` 的 `--formal-spec` 模式，本次不移动工具或刷新说明表。详细阶段边界见 [存储说明](docs/storage/README.md) 和 [验证目录](provider_validation/README.md)。

## 领域模型

| 文件 | 职责 |
|---|---|
| `domain/models.py` | `Dataset`、`AssetType`、`BarRecord`、任务状态、记录状态和复权口径等核心模型 |
| `domain/sessions.py` | A 股交易时段、市场日程和应有分钟点计算 |
| `domain/__init__.py` | 对外导出稳定的领域模型入口 |

## 数据源适配器

| 文件 | 职责 |
|---|---|
| `providers/base.py` | 日线、分钟等 Provider 协议以及 Fixture Provider 基础实现 |
| `providers/contracts.py` | HTTP 响应、失败分类、返回窗口和 Provider Contract |
| `providers/transport.py` | 公共 HTTP 传输、共享请求组限速、单项输入会话捕获与严格回放、快照和 TDX 行解析；保留来源会话/代理并复用原验证重试策略 |
| `providers/tencent/daily.py` | Tencent 既有日线采集与明确日期窗口的前复权候选输入；共享源码合同的主机回退 |
| `providers/tencent/snapshot.py` | Tencent 指定证券分批快照、原脚本来源字段、代码/格式/重复验证及请求范围覆盖；GBK解码和旧行情行保留，全市场在线验证待完成 |
| `providers/tencent/minute.py` | Tencent 原生 1m 分钟线与最近 5m 候选输入 |
| `providers/sina/calendar.py` | 原 SDK 新浪交易日历来源适配，返回明确的正向日期并复用现有日历协议 |
| `providers/eastmoney/limit_pool.py` | 原 SDK 东财涨停池日期快照，保留中文来源字段 |
| `providers/sina/daily.py` | Sina 普通历史日线 |
| `providers/sina/snapshot.py` | Sina 批量收盘快照 |
| `providers/sina/minute.py` | Sina 原生 5m 分钟线 |
| `providers/tdx/minute.py` | 注入式 TDX 延迟 1m 分钟线 |
| `providers/baostock/session.py` | BaoStock SDK 登录至登出的会话串行、代码映射和结果集读取；两查询解码载荷留证、严格回放、缓存及查询边界限速 |
| `providers/baostock/daily.py` | BaoStock 历史日线，初始角色为校验来源 |
| `providers/baostock/minute.py` | BaoStock 原生 5m，初始角色为校验来源 |
| `providers/baostock/industry.py` | 原 BaoStock 两查询、沪深A股筛选与行业合并；分别提供证券快照/行业归属的 YAML 映射候选，保留旧默认返回与覆盖缺失；TCP帧不可见 |
| `providers/baostock/__init__.py` | BaoStock 适配器导出 |
| `providers/eastmoney/dividend.py` | 原批量分红事件及单证券 SDK 历史候选输入；已实施选择、预披露留证 |
| `providers/eastmoney/security_list.py` | EastMoney 分页证券列表、市场和资产类型映射 |
| `providers/exchanges/security.py` | 北交所证券目录与上交所上市ETF目录，保留原Session、代理及请求合同；检查完整返回与唯一证券，当前目录不伪装历史名单 |
| `providers/eastmoney/realtime.py` | EastMoney 原行情/分时；三个历史输入复用的证券身份、来源计数校验及回放时钟恢复函数 |
| `providers/eastmoney/fund_flow.py` | 原日级资金流解析及原 SDK 历史候选输入，修正大小单列对应 |
| `providers/eastmoney/__init__.py` | EastMoney 适配器导出 |
| `providers/akshare/session.py` | AkShare 可选依赖加载和代码转换 |
| `providers/akshare/daily.py` | AkShare 股票、ETF、LOF、指数函数级历史日线 |
| `providers/akshare/boards.py` | 原 AkShare 同花顺行业目录、行业指数日线和行业/概念资金流快照；YAML 名称映射、保留旧返回并提供 SDK 原列与身份上下文 |
| `providers/akshare/__init__.py` | AkShare 适配器导出 |
| `providers/ths/boards.py` | 同花顺行业/概念板块列表、分页和成分页面适配器；输出可写入 Raw 的关系快照，不伪装成行情 Bar |
| `providers/ths/__init__.py` | THS 适配器导出 |
| `providers/tencent/__init__.py` | Tencent 适配器导出 |
| `providers/sina/__init__.py` | Sina 适配器导出 |
| `providers/tdx/__init__.py` | TDX 适配器导出 |
| `providers/probes.py` | 日线/分钟能力探针、证据模型和探针结果生成 |
| `providers/__init__.py` | Provider 包说明，不承载业务实现 |

## 配置加载

| 文件 | 职责 |
|---|---|
| `config/loader.py` | 加载来源、输入契约、参数来源、采集意图、字段模板、路由与映射/状态/范围配置；参数绑定与类型/范围校验、统一运行路径及隔离层校验；配置不生成验证有效期 |
| `routing/factory.py` | 按现有端点创建 Provider，保留来源传输并共享限速；按实际范围、有效期及四层证据检查资格和冷却。原响应与SDK派生载荷分别核验，派生载荷不计为额外源响应 |

## 能力路由器

| 文件 | 职责 |
|---|---|
| `routing/capabilities.py` | Provider Capability 模型、有效期/样例范围筛选、按请求形态计算间隔预算及证据可用性回调；不承载独立能力管理服务 |
| `routing/router.py` | Missing Set 计算和实时分钟采集计划 |
| `routing/retry.py` | 重试、Fallback、冷却相关的执行策略 |
| `routing/__init__.py` | 路由包说明，不承载业务实现 |

## 数据质量

| 文件 | 职责 |
|---|---|
| `quality/normalization.py` | 执行 YAML 来源字段映射、字段类型/倍率/时区/空值、规则状态与来源范围；支持各数据域的候选字段合同 |
| `quality/validation.py` | OHLC、日期、时间键和基础记录质量校验 |
| `quality/resolution.py` | 来源优先级、字段完整度、冲突识别和记录仲裁 |
| `quality/publication.py` | 日线发布门槛和数据集发布策略加载 |
| `quality/__init__.py` | 质量包说明，不承载业务实现 |

## 数据生产与发布

| 文件 | 职责 |
|---|---|
| `pipeline/daily.py` | 日线按来源补缺、标准化、校验、候选生成和发布编排 |
| `pipeline/inputs.py` | 单项候选采集/回放、持久化业务任务及证券清单调度检查；统一按日原始暂存和正式输出边界、来源解析、YAML映射、质量与覆盖、重做和发布恢复；自动生成任务summary.json并将路径/哈希记入原元数据，名单在线请求受路由资格约束 |
| `pipeline/daily_reconciliation.py` | 日线 provisional/final 合并、缺失统计和盘后校准 |
| `pipeline/minute.py` | Watchlist 实时分钟采集和 Hot Store 写入 |
| `pipeline/minute_reconciliation.py` | 分钟盘后校准、final 提升、冲突隔离和完整性处理 |
| `pipeline/snapshot_daily.py` | 收盘快照构建 provisional 日线 |
| `pipeline/snapshot_aggregator.py` | 累计快照差分为 provisional 1m 的聚合逻辑 |
| `pipeline/resample.py` | 完整 1m 按交易时段派生 5m |
| `pipeline/history.py` | 历史请求按天数和最大行数拆分；后续历史重建继续在此流程边界内扩展 |
| `pipeline/__init__.py` | 流程包说明，不承载业务实现 |

## 数据存储

| 文件 | 职责 |
|---|---|
| `storage/raw.py` | 不覆盖的 Raw Object Store、按日暂存和来源范围路径、转正日期校验、解析前响应字节/清单、哈希复查、脱敏和匹配有效响应复用 |
| `storage/hot.py` | SQLite WAL Hot Minute Store 和即时查询数据 |
| `storage/parquet.py` | 来源字段合同的候选 Parquet 精确类型写入；既有 Canonical Bar 分区、Manifest、文件锁和原子发布 |
| `storage/metadata.py` | DuckDB 元数据、Attempt 及调度周期的原子占用、Provider 健康、Probe 验证记录、既有能力证据查询和冲突记录；不新增能力管理数据库 |
| `storage/integrity.py` | 确定性 row hash、Manifest 生成和完整性校验 |
| `storage/__init__.py` | 存储包说明，不承载业务实现 |

## 数据访问服务

| 文件 | 职责 |
|---|---|
| `service/instruments.py` | Security Master 本地存储、读取、合并和稳定 instrument ID |
| `service/instruments_update.py` | Security Master 来源更新、缺失保留和变更审计 |
| `service/calendar.py` | Trading Calendar 存储、读取和来源合并 |
| `service/calendar_update.py` | 交易日历来源更新、官方优先级，以及校验原在线证据后的正向日期导入 |
| `service/market_data.py` | Canonical + Hot 的统一分钟查询服务 |
| `service/__init__.py` | 服务包说明，不承载业务实现 |

## Worker 与命令行

| 文件 | 职责 |
|---|---|
| `worker/attempts.py` | Collection Attempt 状态机和租约状态 |
| `worker/recovery.py` | 中断后的临时文件、Canonical 和元数据恢复扫描；已发布任务的证据复查和原子归档 |
| `worker/scheduler.py` | 既有任务时间表；输入配置的天/分钟周期、交易日/交易时段槽位、证券范围绑定及容量门禁；从同一频率配置生成证券清单每日任务 |
| `worker/acceptance.py` | 离线验收回放和容量/恢复验收证据 |
| `worker/__init__.py` | Worker 包说明，不承载业务实现 |
| `cli.py` | `collect-due-inputs`（默认保存计划）、`collect-input`（默认分层落盘，支持 JSON 参数上下文及隔离验证输出）、`probe-*`、`recover`、`acceptance-offline` 等入口，负责参数解析和现有流程组装 |
| `__init__.py` | 包级公共领域模型导出 |

## 维护约束

- 新功能继续修改对应现有模块，不保留根级兼容转发或平行入口。
- 前复权、事件发现、历史重建、范围、重做及回退在现有模块内扩展，不另建任务管理层。
- 原始证据与历史哈希保持原样；文档合并不扩大验证范围或启用生产来源。
- 目录职责变动同步维护本文与README；阶段收尾检查重复设计、过期状态和失效引用。
- `provider_validation/results/legacy/tmp_test/` 只保存历史研究脚本、合同和结果；原11个跟踪文件继续纳入Git，原本忽略的大体积结果保持本地保存。`archive-manifest.json` 是逐文件迁移索引，旧脚本与响应字节不改写，不作为正式运行入口。
