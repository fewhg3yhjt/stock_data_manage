# 代码目录与文件职责

本文记录当前代码实现与市场数据中心设计文档的对应关系。目录按设计文档第 21 节的模块划分组织，不为新功能额外建立平行实现。

## 设计模块映射

| 设计模块 | 当前目录 | 职责边界 |
|---|---|---|
| 领域模型 | `src/stock_data_manage/domain/` | 数据集、证券类型、状态、标准 Bar 记录和交易时段模型 |
| 数据源适配器 | `src/stock_data_manage/providers/` | Provider 接口、Fixture/HTTP 适配器、响应契约和能力探针 |
| 能力路由器 | `src/stock_data_manage/routing/` | 来源能力注册、缺失集合、实时采集计划和重试策略 |
| 数据质量 | `src/stock_data_manage/quality/` | 标准化、基础校验、冲突解决和发布门槛 |
| 数据生产与发布 | `src/stock_data_manage/pipeline/` | 日线、分钟、快照、重采样和盘后校准流程 |
| 数据存储 | `src/stock_data_manage/storage/` | Raw、Hot、Canonical、Metadata 和完整性证据 |
| 数据访问服务 | `src/stock_data_manage/service/` | Security Master、Trading Calendar 和统一分钟查询 |
| Worker | `src/stock_data_manage/worker/` | 调度、执行记录、恢复和离线验收 |

配置文件仍位于仓库根目录的 `config/`：`providers.yaml` 保存来源配置和有证据引用的输入能力契约，`collection.yaml` 保存采集意图与刷新节奏，`capabilities.yaml` 保存路由顺序，`datasets/` 保存数据集标准定义，`normalization/` 保存按数据集拆分的来源归一化规则；命令行入口仍位于 `src/stock_data_manage/cli.py`。
EastMoney 各数据域的范围和实现状态见 `eastmoney-data-domain-coverage.md`，不以单个分红事件 Endpoint 代表整个来源。
项目当前实现、验证、待实现和明确不做范围见 `PROJECT_PROGRESS.md`。

Provider 的离线契约、实时探针和路由资格记录见仓库根目录的 `PROVIDER_CAPABILITY_MATRIX.md`。该文档是渠道验证事实记录，不是运行时配置。
能力采集、证据生命周期和 YAML 归一化规则见 `provider-capability-verification-and-normalization.md`；它是 Provider 接入和后续验证的专项设计。
Provider 接口验证资料统一位于 `provider_validation/`：`tests/` 保存探针、低频实时探针入口、BaoStock SDK低频探针、离线重放及逐接口覆盖报告生成脚本；`tests/source_snapshots/` 保存固定提交的上游源码/测试快照及哈希清单；`coverage/` 保存逐接口 CSV 和阅读版 XLSX；`docs/` 保存验证方法和报告；`results/` 保存原始响应、派生结果、逐接口 JSON 结论和摘要。原始 HTTP 清单及响应体位于 `provider_validation/results/raw/<run-id>/` 或对应探针批次的 `provider_validation/results/live-probes/<run-id>/_raw/`；SDK探针保存调用边界可见的解码字段和行，同时明确标记底层TCP帧不可见。历史探针摘要位于 `provider_validation/results/legacy/`。行业迁移证据可用 `provider_validation/tests/replay_sector_capability_archives.py` 离线重放。详细入口见 [Provider 验证目录说明](provider_validation/README.md) 和 [接口测试覆盖度说明](provider_validation/docs/interface-coverage-method.md)。

输入能力第一阶段的设计与实现边界见 `provider_validation/docs/2026-10-03-input-capability-framework.md`。现有 `provider_validation/tests/prepare_capability_results.py --input-catalog` 离线生成 `coverage/successful-input-capabilities.csv` 和同名 JSON，关联源记录、原响应和哈希。该阶段回归证据保存于 `results/2026-10-03-input-capability-framework-tests.xml` 与 `results/2026-10-03-input-capability-framework-verification.json`，不属于生产数据。

四项输入的 YAML 执行实现、使用方式与边界见 `provider_validation/docs/2026-10-03-yaml-input-collection.md`。`provider_validation/tests/replay_input_capabilities.py` 执行 `tests/test_input_collection.py` 中的离线合同与原实现对照，持久化比较结果；最终回放证据位于 `results/input-verified-20261003/`，在线证据位于 `results/input-live-20261003/`，回归结果位于 `results/2026-10-03-yaml-input-collection-tests.xml`。开发中间结果及存储失败记录位于同目录下其他 `input-*-20261003/` 批次，保留代码版本与失败类别，不混作最终验证结论。

`config/datasets/minute_bar_5m.yaml`、`limit_up_pool.yaml`、`trading_calendar.yaml` 分别定义三种输入的字段类型/单位/必填约束/主键；`config/normalization/` 下的同名文件定义来源映射与转换规则。`results/2026-10-03-yaml-input-collection-verification.json` 关联测试、最终回放、在线证据及当前代码/配置哈希。当前实现版能力清单位于 `coverage/successful-input-capabilities-20261003-implemented.csv/json`，保留第一阶段无日期版本。`results/sdk-snapshot-redaction-20261003.json` 记录本次生成的 SDK 源码快照脱敏，不改变 HTTP 响应与来源数据。

腾讯采集频率与全市场快照配置的历史实施见 `provider_validation/docs/2026-10-03-tencent-collection-scheduling.md`；独立四种策略写在既有 `collection.yaml`，输入契约引用具体策略。快照后续候选迁移已完成，独立1分钟输入仍未完成；周期采集不启用生产发布。历史离线证据位于 `results/input-scheduling-accepted-20261003/`，测试和总索引位于 `results/2026-10-03-tencent-scheduling-tests-final.xml` 与同前缀 `verification.json`，原能力清单保留独立的 `coverage/successful-input-capabilities-20261003-scheduling-release.csv/json` 版本。原 `schedules.yaml` 固定任务保留，未启动后台进程或管理台。

其他接口转换现状与建议下一批范围见 `provider_validation/docs/2026-10-04-input-transition-review.md`；离线调查代码和逐项引用哈希保存于 `results/input-transition-review-20261004/audit.py`、`review.json`。调查不改变现有Provider、运行时配置或正式路由资格。

同花顺四项既有接口的后续实施见 `provider_validation/docs/2026-10-04-ths-input-collection.md`。原 `providers/akshare/boards.py` 通过现有 YAML 映射保留旧返回，同时提供 SDK 原列给通用候选流程；`industry_directory.yaml` 为新增目录字段模板，行业日线/资金流复用原模板，三个同名归一化文件补来源映射。原 CLI 通过 `--context-file` 接收板块与依赖参数。修改前源码存于 `results/ths-original-20261004/`，最终对照、回归和索引存于 `results/ths-final-20261004/` 及同前缀测试/验证文件，均不属于生产数据。

BaoStock 后续两项转换见 `provider_validation/docs/2026-10-04-baostock-input-collection.md`。原 `industry.py` 的两查询方法分别输出证券状态快照和行业归属，保留旧默认返回；原 `session.py` 承担 SDK 解码证据回放、缓存复用、查询间隔和会话串行。新增 `datasets/security_snapshot.yaml` 及两项对应归一化文件，复用原行业模板。原源码、最终验证和 CLI 证据分别位于 `results/bao-original-20261004/`、`bao-final-20261004/` 和 `bao-cli-20261004/`；SDK 原始TCP帧不可见。

腾讯快照后续转换见 `provider_validation/docs/2026-10-04-tencent-snapshot-input-collection.md`。修改原 `snapshot.py`，保留旧行情行并提供原脚本字段给现有 `realtime_quote.yaml` 的YAML映射；原 `RequestsTransport` 增加可选择的复用Session，候选工厂保留原成功快照请求行为，K线默认方式不变。已有调度器仍以在线批量验证待完成门禁阻断自动快照。原源码/调查、最终对照及真实CLI证据分别位于 `results/tencent-original-20261004/`、`tencent-final-20261004-v2/` 和 `tencent-cli-20261004/`，完整测试和哈希索引为同前缀XML/verification.json；最终目录的 `verify.py` 独立核对来源、候选、代码配置及证据哈希。初版final证据保留，第二版只修正合成夹具字节长度。

`.gitattributes` 对本次新证据目录禁用 Git 换行转换，并固定新增实现/模板的 LF 格式，避免提交和检出改变证据字节及其 SHA-256 引用；不更改旧证据属性。

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
| `config/loader.py` | 加载来源、输入契约、参数来源、采集意图、字段模板、路由与映射/状态/范围配置；参数绑定与类型/范围校验；配置不生成验证有效期 |
| `routing/factory.py` | 按现有端点创建 Provider，保留来源传输并共享限速；读取已有元数据证据，按实际验证范围和原有效期注册能力，并在选择时复查证据与冷却状态 |

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
| `pipeline/inputs.py` | 单项输入及按周期触发的候选采集/回放；共享限速、独立候选 Attempt 幂等状态、响应留证、分红选择/排除记录及 YAML 映射；不注册正式路由 |
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
| `storage/raw.py` | 不覆盖的 Raw Object Store、解析前响应字节/清单、哈希复查、脱敏和匹配有效响应复用 |
| `storage/hot.py` | SQLite WAL Hot Minute Store 和即时查询数据 |
| `storage/parquet.py` | Canonical Parquet 分区、Manifest、文件锁和原子发布 |
| `storage/metadata.py` | DuckDB 元数据、Attempt 及调度周期的原子占用、Provider 健康、Probe 验证记录、既有能力证据查询和冲突记录；不新增能力管理数据库 |
| `storage/integrity.py` | 确定性 row hash、Manifest 生成和完整性校验 |
| `storage/__init__.py` | 存储包说明，不承载业务实现 |

## 数据访问服务

| 文件 | 职责 |
|---|---|
| `service/instruments.py` | Security Master 本地存储、读取、合并和稳定 instrument ID |
| `service/instruments_update.py` | Security Master 来源更新、缺失保留和变更审计 |
| `service/calendar.py` | Trading Calendar 存储、读取和来源合并 |
| `service/calendar_update.py` | Trading Calendar 来源更新和官方优先级处理 |
| `service/market_data.py` | Canonical + Hot 的统一分钟查询服务 |
| `service/__init__.py` | 服务包说明，不承载业务实现 |

## Worker 与命令行

| 文件 | 职责 |
|---|---|
| `worker/attempts.py` | Collection Attempt 状态机和租约状态 |
| `worker/recovery.py` | 中断后的临时文件、Canonical 和元数据恢复扫描 |
| `worker/scheduler.py` | 既有 Phase 1 固定任务时间表；输入配置的天/分钟周期、交易日/交易时段槽位、证券范围绑定及容量门禁 |
| `worker/acceptance.py` | 离线验收回放和容量/恢复验收证据 |
| `worker/__init__.py` | Worker 包说明，不承载业务实现 |
| `cli.py` | `collect-due-inputs`（默认保存计划）、`collect-input`（支持 JSON 参数上下文）、`probe-*`、`recover`、`acceptance-offline` 等入口，负责参数解析和现有流程组装 |
| `__init__.py` | 包级公共领域模型导出 |

## 迁移约束

- 旧的根级模块路径已经删除，不保留兼容转发模块。
- 新功能必须进入对应设计模块，不能在包根目录继续增加散落模块。
- 目录调整只改变模块归属和导入路径，不改变现有业务行为。
- 前复权、分红事件和历史重建应在现有 `pipeline`、`providers`、`routing`、`quality`、`storage` 边界内继续实现，不另起一套架构。
- `tmp_test/` 仅保留探针和研究脚本，不作为正式运行时模块入口。

东财后续三项已有接口转换见 `provider_validation/docs/2026-10-04-eastmoney-input-collection.md`。原 `shareholder.py`、`dividend.py`、`fund_flow.py` 提供原成功 SDK 的历史候选方法；复用三个现有模板和归一化文件。`providers/contracts.py` 的既有输入结果补来源行、映射上下文和排除行。原验证脚本增加 `--verify-em-inputs`，证据目录为 `results/em-original-20261004/`、`em-final-20261004/`、`em-cli-20261004/` 和同前缀测试/索引。没有新增运行时模块或自动调度策略。

当前输入进度的只读核对记录位于 `provider_validation/results/input-progress-review-20261004/review.json`：实际工厂绑定与配置一致，14项候选已实现、51项待转换或归并、7项阻断、2项别名。记录关联当前源码/配置及逐项来源证据哈希；未执行下一批转换，也不改变生产资格。前述各批最终索引仍代表各自提交时的版本。

自主转换首批见 `provider_validation/docs/2026-10-04-stock-pool-input-collection.md`：原 `limit_pool.py` 扩展四个池，保留原涨停池默认合同；候选路径执行原响应状态/qdate/tc/证券身份校验和回放时钟恢复。新增 `datasets` 与同名 `normalization` 的 `broken_limit_pool.yaml`、`limit_down_pool.yaml`、`previous_limit_pool.yaml`、`strong_stock_pool.yaml`；ASTOCK-023仅归并为既有行业资金流别名。原验证脚本增加 `--verify-stock-pools`，源码/调查、最终回放和CLI证据分别在 `results/pools-original-20261004/`、`pools-final-20261004/`、`pools-cli-20261004/`，完整回归和总索引为同前缀XML/verification.json。当前18项可执行候选、46项待转换、7项阻断、3项别名。原进度调查记录保持其生成时版本，不覆盖。

自主转换后续见 `provider_validation/docs/2026-10-04-eastmoney-events-input-collection.md`：原 `providers/eastmoney/financial.py` 保留默认财务指标方法，并承接业绩预告与机构调研汇总的有界事件采集及原会话/严格分页行为。新增 `datasets` 及同名 `normalization` 的 `earnings_forecast.yaml`、`institution_survey.yaml`；现 `pipeline/inputs.py` 保留来源总数/上限、原采集时间、有效筛选空表。原验证脚本扩展 `--verify-em-events`；原始调查、最终回放和CLI证据分别在 `results/events-original-20261004/`、`events-final-20261004/`、`events-cli-20261004/`，完整回归及索引为同前缀XML/verification.json。当前20项候选、44项待转换，不新增运行时模块。

四项金融事件转换见 `provider_validation/docs/2026-10-04-eastmoney-actions-input-collection.md`：原 `financial.py` 增加 `fetch_action_list` 并复用原会话/严格分页；候选工厂绑定ASTOCK-080至083，原采集报告按各来源公告/统计/申购日记录窗口。新增 `datasets` 和同名 `normalization` 的 `holder_trades.yaml`、`buyback.yaml`、`pledge.yaml`、`ipo_calendar.yaml`。原 `quality/normalization.py` 补必要YAML值映射、备用来源字段和零值空置，保持来源进度代码和未来排期语义，不新增归一化体系。原验证入口扩展 `--verify-em-actions`；原始调查、最终回放和CLI证据在 `results/actions-original-20261004/`、`actions-final-v2-20261004/`、`actions-cli-v2-20261004/`，同前缀XML与verification.json为完整回归/哈希索引。当前24项候选、40项待转换，生产未启用。
