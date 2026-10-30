# 代码目录与文件职责

本文维护当前目录、模块和文件职责，不记录逐批开发过程；当前完成状态见 [项目进度](PROJECT_PROGRESS.md)，历史迁移和验证解释见既有证据。下文代码文件路径均相对于 `src/`。

业务验收证据由既有 `data/task_workspace/_checks/` 保存；本次三只股票v7验收包中的 `run_acceptance.py` 与 `audit_publication.py` 只复现已确认样本的分阶段验收和独立回读，不是正式采集入口。它们与机器核验、JUnit、来源证据的索引见[验收报告](data/task_workspace/_checks/tencent-v7-formal-three-stock-2026-10-28/README.md)；正式执行仍由 `src/pipeline/inputs.py` 的任务入口承接，没有新增业务模块。

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
| 领域模型 | `src/domain/` | 数据集、证券类型、状态、三口径日线聚合记录、标准 Bar 记录和交易时段模型 |
| 数据源适配器 | `src/providers/` | Provider 接口、Fixture/HTTP 适配器、响应契约和能力探针 |
| 能力路由器 | `src/routing/` | 来源能力注册、缺失集合、实时采集计划和重试策略 |
| 数据质量 | `src/quality/` | 标准化、基础校验、冲突解决和发布门槛 |
| 数据生产与发布 | `src/pipeline/` | 日线、分钟、快照、重采样和盘后校准流程 |
| 数据存储 | `src/storage/` | Raw、Hot、Canonical、Metadata 和完整性证据 |
| 数据访问服务 | `src/service/` | Security Master、Trading Calendar、统一分钟查询，以及任务与证据的只读本机页面 |
| Worker | `src/worker/` | 调度、执行记录、恢复和离线验收 |


## 配置与文档职责

根目录 `config/`：`providers.yaml` 保存来源和输入契约，`collection.yaml` 保存参数来源、范围与刷新频率，`capabilities.yaml` 保存路由策略，`datasets/` 保存字段模板，`normalization/` 保存来源映射，`schedules.yaml` 引用任务时间表。运行状态由现有存储维护，不回写YAML。

| 文档 | 主要职责 |
|---|---|
| `README.md` | 项目安装、运行与文档导航 |
| `AGENTS.md` | 协作、验证、权限及定期文档整理要求 |
| `PROJECT_PROGRESS.md` | 当前交付、开发任务依赖、状态、验收证据及未关闭问题；历史决策另见docs/engineering-decisions.md |
| `docs/engineering-decisions.md` | 重要决策与被替代合同、原证据索引及本次文档承接关系，不维护运行状态 |
| `docs/pipeline/daily-history-rebuild.md` | 历史事件、换版确认、人工三口径重建及多日期统一切换的待实现合同 |
| `STOCK_ANALYSIS_V2_MARKET_DATA_CENTER_ARCHITECTURE_V1.md` | 总体架构、术语、模块边界、对象关联、阶段合同与配置职责；具体分钟规则见任务流程，当前交付见进度，历史取舍见工程决策 |
| `stock-data-design-qfq-history-rebuild.md` | 多口径日线聚合记录、证券类型与价格/点位语义、口径适用状态及默认消费；历史重建另见docs/pipeline/daily-history-rebuild.md，原路径保留 |
| `stock-data-test-design-acceptance-baseline.md` | 测试层次、故障场景和验收门槛 |
| `provider-capability-verification-and-normalization.md` | 来源证据、资格及YAML归一化规则 |
| `docs/providers/README.md` | 正式源头接口说明、各数据域边界及说明表依据 |
| `docs/providers/源头采集接口说明.xlsx.inspect.ndjson` | 既有接口说明表的逐行 JSON 检查快照，记录工作表、单元格样式与公式，不作为当前配置或生产资格依据 |
| `docs/providers/security-catalog.md` | 当前股票和ETF来源范围、日期、分类与差异 |
| `docs/pipeline/collection-tasks.md` | 获取、检查、发布、重做、证券清单调度和恢复；分别维护设计稿的阶段时序、故障决策表与当前入口、名单、日线、分钟及恢复流程框图 |
| `docs/storage/README.md` | 数据分层、日期唯一、实际文件名与内容示例、字段解读、生成/读取/保留规则、原始引用与归档；精简方向与当前实现分开标注 |
| `docs/service/read-only-web.md` | 本机只读运行台的用途、读取来源、启动方式、状态语义、安全边界与云端部署前置条件 |
| `docs/service/container-deployment.md` | 只读运行台的 Docker/Compose 部署、代码挂载更新、持久数据只读挂载及网络安全边界 |
| `PROVIDER_CAPABILITY_MATRIX.md` | 2026-10-03历史抽样及证据索引，不是当前能力清单 |

源头接口阅读版与同名JSON位于 `docs/providers/`，是代码、YAML和证据的派生快照，不是运行配置；`docs/pipeline/examples/` 是离线任务参数示例，不保存运行数据。合并删除的5份旧文档内容已由上述文件承接，不建立兼容副本。

目前正式模块文档目录为 `docs/providers/`、`docs/pipeline/`、`docs/storage/`、`docs/service/`。后续路由、质量或Worker文档按实际需要创建，不预建空目录；当前证券清单调度在任务流程中维护，避免单独重复说明。

验证区保存正式实现前的原响应、脚本和历史结论；已有业务联调或说明生成工具错放位置作为待整改项，不据此新增业务产物。当前正式说明生成器仍为 `provider_validation/tests/build_interface_coverage.py` 和 `build_interface_coverage_workbook.mjs` 的 `--formal-spec` 模式，本次不移动工具或刷新说明表。详细阶段边界见 [存储说明](docs/storage/README.md) 和 [验证目录](provider_validation/README.md)。

## 领域模型

| 文件 | 职责 |
|---|---|
| `domain/models.py` | `Dataset`、`AssetType`、`DailyRecord` 三口径聚合对象、`BarRecord` 分钟/历史对象、任务状态、记录状态和复权口径 |
| `domain/sessions.py` | A 股交易时段、市场日程和应有分钟点计算 |
| `domain/__init__.py` | 对外导出稳定的领域模型入口 |

## 数据源适配器

| 文件 | 职责 |
|---|---|
| `providers/base.py` | 日线、分钟等 Provider 协议以及 Fixture Provider 基础实现 |
| `providers/contracts.py` | HTTP 响应、失败分类、返回窗口和 Provider Contract |
| `providers/transport.py` | 公共 HTTP 传输、共享请求组限速、单项输入会话捕获与严格回放、快照和 TDX 行解析；保留来源会话/代理并复用原验证重试策略 |
| `providers/tencent/daily.py` | 腾讯日线响应采集与纯解析分离；保留来源窗口原行和失败合同，默认复权探测使用`newfqkline`；窗口采集只因明确的连接、超时或5xx故障按接口对应主机顺序切换，不因业务内容问题换源 |
| `providers/tencent/snapshot.py` | Tencent 指定证券分批快照、原脚本来源字段、代码/格式/重复验证及请求范围覆盖；GBK解码和旧行情行保留，全市场在线验证待完成 |
| `providers/tencent/minute.py` | Tencent 原生 1m 分钟线与最近 5m 候选输入 |
| `providers/sina/calendar.py` | 原 SDK 新浪交易日历来源适配，返回明确的正向日期并复用现有日历协议 |
| `providers/eastmoney/limit_pool.py` | 原 SDK 东财涨停池日期快照，保留中文来源字段 |
| `providers/sina/daily.py` | Sina 普通历史日线 |
| `providers/sina/snapshot.py` | Sina 批量收盘快照 |
| `providers/sina/minute.py` | Sina 原生 5m 分钟线 |
| `providers/tdx/minute.py` | 注入式 TDX 延迟 1m 分钟线 |
| `providers/baostock/session.py` | BaoStock SDK 登录至登出的会话串行、代码映射和结果集读取；已验证查询的解码载荷留证、严格回放、缓存及查询边界限速；日线验证路径还捕获 socket `recv` 字节并与解码结果关联 |
| `providers/baostock/daily.py` | BaoStock 历史日线；按完整日线字段策略请求，捕获/回放原始接收字节与完整SDK解码来源行后再生成标准候选，初始角色为校验来源 |
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
| `providers/sse/market.py` | 上交所既有风险指标和ETF份额；复用原SDK接入指定统计日普通ETF身份，复用已验证官网请求接入交易型货币ETF身份；原份额接口不变 |
| `providers/exchanges/daily.py` | 交易所期货、期权和排名日数据；复用深交所原SDK接入指定统计日ETF身份，逐项核对原XLSX以防SDK删行掩盖问题 |
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
| `config/loader.py` | 加载来源、输入契约、参数来源、字段选择策略、采集意图、字段模板、路由与映射/状态/范围配置；参数绑定与类型/范围校验、统一运行路径及隔离层校验；配置不生成验证有效期 |
| `routing/factory.py` | 按现有端点创建 Provider，保留来源传输并共享限速；按实际范围、有效期及四层证据检查资格和冷却。日期工作区的来源报告按工作区基准解析原响应引用，缺失或哈希不符明确拒绝；原响应与SDK派生载荷分别核验 |

日线留存资格在同一文件按证券、资产类别和完整参数精确核验，保存真实来源、当前版本复核、语义及端到端证据，不推广样本资格。

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
| `quality/normalization.py` | 执行 YAML 来源字段映射、字段类型/倍率/时区/空值、规则状态与来源范围；支持各数据域的候选字段合同；unit_contracts统一校验并匹配来源、市场、显式证券类型、代码段和实际序列，按字段换算并拒绝重叠/无效倍率；未覆盖范围保持空值 |
| `quality/validation.py` | OHLC、日期、时间键和基础记录质量校验 |
| `quality/resolution.py` | 来源优先级、字段完整度、冲突识别和记录仲裁 |
| `quality/publication.py` | 日线发布门槛和数据集发布策略加载 |
| `quality/__init__.py` | 质量包说明，不承载业务实现 |

## 数据生产与发布

| 文件 | 职责 |
|---|---|
| `pipeline/daily.py` | 日线策略冻结、来源口径单元展开、完整记录聚合及发布覆盖检查；承接来源报告的单位精度标记，不重复匹配单位规则或进行网络请求 |
| `pipeline/inputs.py` | 现有来源候选、数据集任务、调度触发和统一运行恢复入口；日线负责按 YAML 任务定义展开三口径来源单元、先留原响应再解析、按完整聚合记录发布；分钟和证券主数据继续原有合同；任务范围及采集策略进入运行冻结定义，调度默认关闭 |
| `pipeline/daily_reconciliation.py` | 只接收范围完整的三口径日线记录，并复用规范分区发布；快照候选不在此提升为正式日线 |
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
| `storage/raw.py` | 原响应字节、按日期/来源/接口暂存及转正；请求级清单和接口索引，按请求替换、审计及共享字节保护；当前目录在解析前按原捕获日落盘；哈希复查、脱敏与缓存 |
| `storage/hot.py` | SQLite WAL Hot Minute Store 和即时查询数据 |
| `storage/parquet.py` | 来源字段合同的候选精确类型写入；Canonical Bar按日期/数据集/资产类型分区，复权保留记录主键；清单、文件锁和原子发布 |
| `storage/metadata.py` | 既有DuckDB任务、分区、Attempt、调度占用、来源健康、证据和冲突；分区及证券状态同事务批量登记，保留未更新证券；查询不晚于目标日期的最新合格分区；不新增数据库 |
| `storage/integrity.py` | 确定性 row hash、Manifest 生成和完整性校验 |
| `storage/__init__.py` | 存储包说明，不承载业务实现 |

## 数据访问服务

| 文件 | 职责 |
|---|---|
| `service/instruments.py` | Security Master 本地存储、读取、合并和稳定 instrument ID |
| `service/instruments_update.py` | Security Master 来源更新、缺失保留和变更审计 |
| `service/calendar.py` | Trading Calendar 存储、读取和来源合并 |
| `service/calendar_update.py` | 交易日历来源更新、单日开市/休市轻量采集及证据引用、官方优先级，以及校验原在线证据后的正向日期导入 |
| `service/market_data.py` | Canonical + Hot 的统一分钟查询服务 |
| `service/web.py` | 只读运行台入口；从现有任务摘要、Raw manifest/body 和 Canonical manifest 生成展示接口；按类型、证券及业务日期查询已核验来源行，复用 YAML 字段说明和已发布证券名称辅助检索，不读写任务数据库 |
| `service/static/` | 只读运行台的原生 HTML、CSS 和 JavaScript 页面资源，无独立前端构建或持久化状态 |
| `service/__init__.py` | 服务包说明，不承载业务实现 |

## Worker 与命令行

| 文件 | 职责 |
|---|---|
| `worker/attempts.py` | Collection Attempt 状态机和租约状态 |
| `worker/progress.py` | 三阶段统一采集单元计数，发布整批完成后计入；内部动作和旧快照恢复、失败与复用、耗时及节流；CLI stderr展示，不管理调度或业务数据 |
| `worker/recovery.py` | 既有提交、临时文件和元数据恢复；日期工作区归档前复核候选、原响应及发布，归档后更新元数据和摘要；保留旧材料归档校验 |
| `worker/scheduler.py` | 既有任务时间表及频率计算；构建证券任务时按目标日期选择当前或历史来源，绑定请求日期、日历和检查范围；定时名单继续使用当前目录 |
| `worker/acceptance.py` | 合成组件的存储重放、恢复和容量估算自检；逐项预期/实际值、范围和证据，不能签署项目M1/M2 |
| `worker/__init__.py` | Worker 包说明，不承载业务实现 |

任务入口 `pipeline/inputs.py` 支持正式留存响应模式及持久化进度；`cli.py` 提供 `--mode retained`、`--progress/--no-progress`。实现测试新增 `tests/test_task_progress.py`（进度、未知总量、失败和恢复计数）与 `tests/test_retained_daily.py`（合成四层资格、范围及哈希拒绝、零网络发布和恢复）；测试只写tmp，不作为真实来源资格。

腾讯窗口输入沿用同一输入ID：`pipeline/inputs.py`保留完整来源行、只映射请求日期，并将动态日线任务顶层config传到冻结的各证券单元；`routing/factory.py`按显式api识别北交所及精确参数资格，在线日线按精确范围或显式市场/资产/窗口授权放行，四层样本证据仍核验，并在创建任务前及每个单元执行前检查，未认证范围保持阻断。相关合同和配置传递测试补入现有`tests/test_provider_contract_adapters.py`、`tests/test_collection_tasks.py`。

证券任务省略units时，`pipeline/inputs.py` 查询既有交易日历并调用 `worker/scheduler.py` 按collection.yaml构建来源；`routing/factory.py` 对历史ETF按完整日期参数及互补发布数量检查资格。新增行为在既有 `tests/test_scheduler.py`、`tests/test_collection_tasks.py` 验证，不新增任务管理模块。
| `cli.py` | `collect-due-inputs`（默认返回单次计划）、`collect-input`（默认分层落盘，支持 JSON 参数上下文及隔离验证输出）、`probe-*`、`recover`、`acceptance-offline` 等入口，负责参数解析、UTF-8结果与进度输出和现有流程组装 |
| `__init__.py` | 包级公共领域模型导出 |

## 维护约束

用户证券脚本能力补齐仍修改现有模块：`providers/baostock/industry.py`保存交易日历、证券类型及区间筛选；`providers/exchanges/security.py`保存六类上市/退出事件候选及北交所原生分页核验；`providers/exchanges/daily.py`复用原生沪深统计。`pipeline/inputs.py`自动生成候选来源审查材料，`routing/factory.py`负责构建与资格门槛。新增`config/datasets/market_statistics.yaml`及`config/normalization/market_statistics.yaml`定义统计原列保留映射；新增`tests/test_universe_script.py`检查13项来源清单、参数、日期边界、证券类型、缓存与分页合同。

`storage/raw.py`的既有工作材料归档也包含`universe_review.json`，重做及失败恢复保存旧审查结果后生成本次结论，原响应存储方式保持既有规则。

`providers/transport.py`在每跳响应落盘并释放限流名额后，使用requests原生机制继续重定向；`pipeline/inputs.py`对用户证券脚本迁移输入不注入额外SDK重试。既有`tests/test_security_catalog.py`覆盖同/跨主机跳转、Cookie、POST转换、错误留证及离线回放。

- 新功能继续修改对应现有模块，不保留根级兼容转发或平行入口。
- 前复权、事件发现、历史重建、范围、重做及回退在现有模块内扩展，不另建任务管理层。
- 原始证据与历史哈希保持原样；文档合并不扩大验证范围或启用生产来源。
- 目录职责变动同步维护本文与README；阶段收尾检查重复设计、过期状态和失效引用。
- `provider_validation/results/legacy/tmp_test/` 只保存历史研究脚本、合同和结果；原11个跟踪文件继续纳入Git，原本忽略的大体积结果保持本地保存。`archive-manifest.json` 是逐文件迁移索引，旧脚本与响应字节不改写，不作为正式运行入口。

请求捕获模块`providers/transport.py`接收输入契约的请求间隔，任务模块不再统一抬升到3秒；直接调用捕获工具的默认保守间隔仍为3秒。

`pipeline/inputs.py`将显示回调与持久化分离，每证券一次断点、5秒摘要及关键阶段立即保存；候选续跑按冻结任务、配置、原响应和候选完整性判断，不以源码指纹阻断，也不改写原始报告。`storage/metadata.py`只读取定义哈希作不可变检查，避免反复加载旧任务JSON。既有任务、进度和留存日线测试覆盖保存次数与恢复边界。
