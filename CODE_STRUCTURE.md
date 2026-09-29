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

配置文件仍位于仓库根目录的 `config/`，包括 `providers.yaml`、`capabilities.yaml`、`normalization.yaml` 和数据集配置；命令行入口仍位于 `src/stock_data_manage/cli.py`。

Provider 的离线契约、实时探针和路由资格记录见仓库根目录的 `PROVIDER_CAPABILITY_MATRIX.md`。该文档是渠道验证事实记录，不是运行时配置。
能力采集、证据生命周期和 YAML 归一化规则见 `provider-capability-verification-and-normalization.md`；它是 Provider 接入和后续验证的专项设计。
探针摘要证据位于 `docs/provider-probes/`；临时完整探针输出位于 `/tmp/opencode/provider-probes/`，不作为生产数据目录。

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
| `providers/transport.py` | 公共 HTTP 传输、快照返回模型、来源时间和 TDX 行记录解析工具 |
| `providers/tencent/daily.py` | Tencent 普通历史日线和前复权历史日线 |
| `providers/tencent/snapshot.py` | Tencent 批量收盘快照 |
| `providers/tencent/minute.py` | Tencent 原生 1m 分钟线 |
| `providers/sina/daily.py` | Sina 普通历史日线 |
| `providers/sina/snapshot.py` | Sina 批量收盘快照 |
| `providers/sina/minute.py` | Sina 原生 5m 分钟线 |
| `providers/tdx/minute.py` | 注入式 TDX 延迟 1m 分钟线 |
| `providers/tencent/__init__.py` | Tencent 适配器导出 |
| `providers/sina/__init__.py` | Sina 适配器导出 |
| `providers/tdx/__init__.py` | TDX 适配器导出 |
| `providers/probes.py` | 日线/分钟能力探针、证据模型和探针结果生成 |
| `providers/__init__.py` | Provider 包说明，不承载业务实现 |

## 配置加载

| 文件 | 职责 |
|---|---|
| `config/loader.py` | 加载 `providers.yaml`、`capabilities.yaml` 和 `normalization.yaml` 的静态配置 |
| `routing/factory.py` | 根据来源 Endpoint 配置创建具体 Provider，并注册静态 Capability |

## 能力路由器

| 文件 | 职责 |
|---|---|
| `routing/capabilities.py` | Provider Capability 模型和有效期/范围筛选；不承载独立能力管理服务 |
| `routing/router.py` | Missing Set 计算和实时分钟采集计划 |
| `routing/retry.py` | 重试、Fallback、冷却相关的执行策略 |
| `routing/__init__.py` | 路由包说明，不承载业务实现 |

## 数据质量

| 文件 | 职责 |
|---|---|
| `quality/normalization.py` | Provider 专属字段、单位、时间和来源信息标准化 |
| `quality/validation.py` | OHLC、日期、时间键和基础记录质量校验 |
| `quality/resolution.py` | 来源优先级、字段完整度、冲突识别和记录仲裁 |
| `quality/publication.py` | 日线发布门槛和数据集发布策略加载 |
| `quality/__init__.py` | 质量包说明，不承载业务实现 |

## 数据生产与发布

| 文件 | 职责 |
|---|---|
| `pipeline/daily.py` | 日线按来源补缺、标准化、校验、候选生成和发布编排 |
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
| `storage/raw.py` | 不覆盖 Raw Object Store 和原始响应引用 |
| `storage/hot.py` | SQLite WAL Hot Minute Store 和即时查询数据 |
| `storage/parquet.py` | Canonical Parquet 分区、Manifest、文件锁和原子发布 |
| `storage/metadata.py` | DuckDB 元数据、Attempt、Provider 健康、Probe 验证记录和冲突记录；不新增能力管理数据库 |
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
| `worker/scheduler.py` | Phase 1 配置化任务时间表和按日幂等调度 |
| `worker/acceptance.py` | 离线验收回放和容量/恢复验收证据 |
| `worker/__init__.py` | Worker 包说明，不承载业务实现 |
| `cli.py` | `probe-*`、`recover`、`acceptance-offline` 等命令行入口，只负责参数解析和服务组装 |
| `__init__.py` | 包级公共领域模型导出 |

## 迁移约束

- 旧的根级模块路径已经删除，不保留兼容转发模块。
- 新功能必须进入对应设计模块，不能在包根目录继续增加散落模块。
- 目录调整只改变模块归属和导入路径，不改变现有业务行为。
- 前复权、公司行动和历史重建应在现有 `pipeline`、`providers`、`routing`、`quality`、`storage` 边界内继续实现，不另起一套架构。
- `tmp_test/` 仅保留探针和研究脚本，不作为正式运行时模块入口。
