# Stock Data Manage

证券数据采集与管理平台 Phase 1 的实现仓库。设计目标和完整验收标准分别见：

- [市场数据中心术语与数据流程规范](stock-data-terminology-and-data-flow.md)
- [市场数据中心架构设计](STOCK_ANALYSIS_V2_MARKET_DATA_CENTER_ARCHITECTURE_V1.md)
- [前复权日线与历史重建设计](stock-data-design-qfq-history-rebuild.md)
- [实时采集设计](stock-data-design-realtime.md)
- [测试设计与验收基线](stock-data-test-design-acceptance-baseline.md)
- [Provider 能力验证矩阵](PROVIDER_CAPABILITY_MATRIX.md)
- [代码目录与文件职责](CODE_STRUCTURE.md)
- [Provider 能力验证与归一化设计](provider-capability-verification-and-normalization.md)
- [EastMoney 数据域覆盖设计](eastmoney-data-domain-coverage.md)
- [项目开发进度](PROJECT_PROGRESS.md)

## 目录与文档导航

目录和文件的详细职责见 [代码目录与文件职责](CODE_STRUCTURE.md)，文档归属规则见 [项目协作要求中的文档管理](AGENTS.md#文档管理)。

| 目录或文件 | 用途 |
|---|---|
| `src/` | 按 providers、routing、quality、pipeline、storage、service、worker 等职责组织的正式实现 |
| `config/` | 来源参数、采集频率、路由、数据集字段模板及归一化映射配置 |
| `tests/` | 实现的自动化测试 |
| `docs/` | 按模块职责归档的正式说明；各子目录随文档产生建立 |
| `provider_validation/` | 来源探针、覆盖报告、原始响应及验证证据；入口见 [Provider 验证目录说明](provider_validation/README.md) |
| `AGENTS.md` | 协作、验证及文档管理约定 |
| `data/` | 配置指定的实际运行数据目录，按需生成，不提交 Git；当前 Raw、Canonical 和 Hot 分别为 `data/raw/`、`data/canonical/`、`data/hot/` |
| `metadata/` | 当前配置指定的元数据库目录，按需生成；路径为 `metadata/metadata.duckdb` |
| `tmp/` | 本地临时工作和离线验收输出；历史内容保留，新输出不作为生产数据 |
| `tmp_test/` | 历史研究脚本及输出，保留历史引用；新来源验证统一进入 `provider_validation/` |
| `CODE_STRUCTURE.md` | 当前代码和文档目录的职责索引 |

正式文档的归属如下。当前已建立 [源头采集接口文档](docs/providers/README.md)，其他模块文档目录随实际文档建立；现有根目录设计文档保持原位，入口见本文开头。

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

来源可行性证据位于 `provider_validation/results/`。通用输入入口 `collect-input` 当前仍生成候选数据，输出到显式指定的 `--output-root`，尚未发布到生产数据目录。历史报告中的旧源码路径表示报告生成时的位置，原始证据和哈希不因目录调整而改写。

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
python -m stock_data_manage.cli recover --canonical-root data/canonical --metadata metadata/metadata.duckdb
```

运行可重复的离线 M1 验收证据：

```powershell
python -m stock_data_manage.cli acceptance-offline --root tmp/acceptance-m1 --output tmp/acceptance-m1/report.json
```

该报告覆盖 20 个工作日回放、Canonical 幂等、断电恢复和 200 只 Watchlist 容量；真实 Provider Probe 结果见 [Provider 能力验证矩阵](PROVIDER_CAPABILITY_MATRIX.md)，连续交易日试运行仍需在目标运行环境执行。

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
