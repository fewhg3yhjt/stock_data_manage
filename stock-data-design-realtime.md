# 证券全量数据采集与管理平台设计方案

> 版本：2026-09-13 能力验证修订版  
> 本版依据实际接口抽样结果补充具体采集组合、来源优先级、断电恢复、重复采集与冲突隔离规则。接口能力具有时效性，生产运行以现有 Metadata 中的能力验证记录和最近一次验证结果为准。这里的 Capability Registry 只是记录与筛选概念，不代表新增独立管理服务。
> 配套测试与完成标准见：[测试设计与验收基线](stock-data-test-design-acceptance-baseline.md)。
>
> 2026-09-28 设计收敛说明：股票、ETF 和 LOF 正式日线目标口径调整为前复权；公司行动发现、人工历史重建和多日期统一切换以 [前复权日线与历史重建设计](stock-data-design-qfq-history-rebuild.md) 为准。本文中以不复权日线为基础的旧描述仅保留为历史讨论和当前实现说明，不再作为目标口径。分钟数据继续使用独立流程，不复用日线历史重建。

## 1. 项目目标

建设一个轻量、可维护的证券数据采集底座，用于自动获取并管理：

- 股票
- ETF
- LOF
- 指数
- 可转债（后续）
- 日线
- Watchlist 分钟线
- 实时/收盘快照
- Security Master
- 估值、行业等扩展数据

系统重点解决以下问题：

1. 不同证券、不同来源的 `volume` / `amount` 单位不一致。
2. 底层文件混乱，存在半成品、重复文件、区间文件。
3. 单一 Provider 被限流或封禁后任务整体失败。
4. 不同 Provider 的日线、快照、分钟接口能力不同。
5. 同一天、同证券可能被不同 Provider 或同一 Provider 重复采集。
6. timeout、QPS、并发、重试等参数散落在代码中，不方便维护。
7. 后续需要能够通过管理台动态调整采集配置。
8. 分钟数据仅面向可配置 Watchlist 自动持续获取；沪深股票、ETF、LOF、指数以分钟闭合后数秒可查询为目标，北交所接受公共 TDX 行情约 15 分钟延迟。

## 1.1 第一阶段数据范围边界

第一阶段的数据范围必须显式区分：

```text
日线：股票 + ETF + LOF + 指数，可按全量 Universe 运行

分钟：仅 Watchlist / strategy_candidate / static symbols
      不承诺全市场分钟采集

行业板块 / 概念板块：来源专属扩展数据，不计入全量可交易证券 SLA

可转债：后续阶段
```

原因是当前已验证分钟接口主要为逐证券请求或低容量批量请求，无法在一分钟内稳定轮询数千只证券。Planner 必须拒绝不满足时效预算的 `all_stock`、`all_etf`、`all_lof` 分钟任务，而不能降级为超时运行。TDX 单次获取速度较快不等于已经具备全市场分钟 SLA；全市场分钟能力只有通过持续压测后才能单独开放。

第一阶段保持轻量：

```text
Python
+ SQLite WAL（盘中 Hot Minute）
+ Parquet（历史 Canonical）
+ DuckDB（Metadata / 分析查询）
+ YAML
+ APScheduler / systemd timer
```

暂不引入：

```text
Redis
Celery
Kafka
RabbitMQ
Kubernetes
复杂 DAG
分布式任务系统
```

---

# 2. 总体流程

第一阶段实际运行链路控制在：

```text
任务请求
   ↓
确定 Dataset / 日期 / Universe
   ↓
检查已有 Canonical
   ↓
计算 Missing Set
   ↓
选择 Provider + Endpoint
   ↓
采集
   ↓
Normalize + Validate
   ↓
Merge + Resolution
   ↓
写临时文件
   ↓
Atomic Replace
   ↓
更新 Partition State
```

其中：

- Planner：负责缺口计算和接口选择。
- Collector：负责请求、重试和 Provider Fallback。
- Normalizer：统一字段、单位、代码和时间。
- Storage：负责 Raw、Canonical、Merge、Atomic Publish。
- Metadata：记录证券主数据、分区状态、来源状态、冲突信息。

---

# 3. Dataset 与 Provider 解耦

业务层不调用：

```python
fetch_tencent_daily()
```

而统一使用：

```python
fetch(
    dataset="daily_bar",
    symbols=["600519", "000001"],
    start_date="2026-09-01",
    end_date="2026-09-10",
)
```

系统负责决定：

- 哪个 Provider 可用；
- 使用哪个 Endpoint；
- 是否支持指定 symbol；
- 是否支持历史；
- 是否支持分钟；
- 是否需要拆批；
- 失败后切换到哪个 Provider。

建议第一阶段 Dataset：

```text
security_master
daily_bar
minute_bar_1m
minute_bar_5m
snapshot
valuation
industry_daily
```

---

# 4. Security Master

Security Master 用于统一证券基础信息，并支撑：

- 资产类型判断；
- 单位转换；
- 上市/退市判断；
- 历史证券池计算；
- Universe 生成。

建议字段：

```text
instrument_id

symbol
exchange
asset_type
name

list_date
delist_date
status

lot_size

canonical_volume_unit
canonical_amount_unit

updated_at

publisher                 # 指数等非交易证券适用
publish_date
calculation_start_date
is_backfilled_history
classification_status
```

其中：

- `instrument_id` 是稳定证券身份，不随来源代码格式或代码迁移变化；
- `symbol` 是当前交易代码；
- 同一证券发生北交所旧代码迁移、代码变更或来源代码差异时，通过别名历史映射到同一个 `instrument_id`；
- ETF / LOF 分类冲突不能由单一来源静默决定，应保留分类证据并按规则仲裁。

补充表：

```text
security_symbol_history

instrument_id
provider
symbol
effective_from
effective_to
change_reason
evidence_source
```

## 4.1 更新机制

独立轻量任务，每日执行一次。

```text
获取最新证券列表
   ↓
与当前 security_master 比较
   ↓
新增 / 退市 / 更名 / 类型变化
   ↓
更新 security_master
   ↓
记录 security_master_history
```

历史表至少记录：

```text
symbol
field_name
old_value
new_value
effective_date
detected_at
source
```

证券全集不能由单一 Provider 的当次返回直接覆盖。更新时应使用：

```text
前一日可信 Security Master
+ 当日多来源证券清单并集
+ 上市 / 退市 / 更名 / 分类变更证据
↓
候选变更
↓
规则确认后生效
```

单一来源暂时缺失只能标记为来源缺失，不能直接判定证券退市。ETF 与 LOF 分类不一致时保留 `classification_conflict`，不得使用“任一来源认为是 ETF 就归为 ETF”之类的隐式规则。

## 4.2 历史任务必须使用历史证券池

不能使用今天的证券列表判断过去某一天的数据完整性。

应按：

```text
list_date <= trade_date
AND
(
    delist_date IS NULL
    OR delist_date >= trade_date
)
```

计算当日 Expected Set。

---

# 5. Universe

统一支持：

```text
all_stock
all_etf
all_lof
all_index
watchlist
CSI300
strategy_candidate
static symbols
```

例如：

```yaml
universes:
  watchlist:
    type: static
    symbols:
      - "600519"
      - "000001"
      - "510300"
```

全市场和固定证券列表使用同一套采集框架，但 Dataset 可以约束允许的 Universe：

```text
daily_bar       → 允许 all_stock / all_etf / all_lof / all_index / watchlist
minute_bar_*    → 第一阶段仅允许 watchlist / strategy_candidate / static symbols
```

---

# 6. Provider Capability

Provider 不等于某一种数据。

一个 Provider 可以拥有多个 Endpoint：

```text
Provider A
├── security_list
├── full_market_by_date
├── bulk_daily
├── daily_history
├── snapshot
└── minute_history
```

每个 Endpoint 描述自己的能力。

Capability 不能只描述 Dataset 和资产类型。结合已验证接口差异，至少细化到：

```text
provider + endpoint

markets / exchanges
asset_types
code_prefixes / excluded_code_prefixes

frequencies
adjustments

supports_symbol_filter
supports_date_range
supports_pagination

max_symbols_per_request
max_rows_per_request
max_days_per_request
history_retention_days
earliest_available_date

field_completeness
volume_mode / amount_mode
volume_unit / amount_unit

request_interval_seconds
maximum_requests_per_minute
estimated_full_universe_seconds
freshness_delay_seconds

provider_family / adapter
endpoint_version
data_finality             # realtime / delayed / provisional / final
auction_semantics
validated_at
validation_expires_at
validation_evidence
role                      # primary / fallback / validation_only / disabled
```

同一 Provider 的不同 Endpoint 必须分别声明。例如普通历史 K 线、ETF 专用全历史接口、快照接口不能合并成一个模糊的 `daily_history` 能力。北交所、科创板、ETF、LOF等存在差异时，也不能仅以 `stock` 或 `fund` 粗粒度表示。

示例：

```yaml
providers:

  provider_a:
    enabled: true
    priority: 100

    endpoints:

      snapshot:
        enabled: true
        datasets:
          - snapshot
          - daily_bar
        asset_types:
          - stock
          - etf
          - lof
          - index
        markets:
          - XSHG
          - XSHE
          - BSE
        historical: false
        supports_symbol_filter: true
        max_symbols_per_request: 100
        request_interval_seconds: 3
        field_completeness: full_snapshot

      daily_history:
        enabled: true
        datasets:
          - daily_bar
        asset_types:
          - stock
          - etf
          - lof
        historical: true
        supports_symbol_filter: true
        supports_date_range: true
        supports_pagination: false
        max_symbols_per_request: 1
        max_rows_per_request: 1024
        max_days_per_request: 500

      minute_history:
        enabled: true
        datasets:
          - minute_bar_1m
          - minute_bar_5m
        historical: true
        supports_symbol_filter: true
        frequencies:
          - 1m
          - 5m
        max_symbols_per_request: 1
        max_rows_per_request: 120
        max_days_per_request: 5
```

## 6.1 已验证的第一阶段采集组合

以下是 2026-09-13 在当前运行环境中的抽样验证结论。它用于生成首版配置，不代表第三方接口的永久承诺；每项能力必须保留验证时间、样本、首尾时间、行数和结果摘要。

| Dataset / 证券范围 | 主来源 | 次来源或校验来源 | 生产定位与已知边界 |
|---|---|---|---|
| 沪深股票 Security Master | 新浪证券列表 | 腾讯快照存在性校验；BaoStock 仅低优先级补充 | 单次来源缺失不得判定退市 |
| 北交所 Security Master | 北交所官网证券列表与新旧代码映射 | 新浪/腾讯行情存在性校验 | 官方映射写入 `security_symbol_history`；本次验证得到 343 只，以运行日官网为准 |
| ETF / LOF Security Master | 新浪列表 | 东财列表只做低频并集补充，腾讯行情校验 | 本次发现东财列表独有的 LOF 仍可由新浪历史接口获取，因此“列表来源”和“行情来源”不能绑定 |
| 指数 Security Master | 指数发布方/交易所目录 + 本地参考目录 | 新浪活跃指数行情列表 | 本次参考目录 732 个代码、活跃行情 562 个；需保存发布方、发布日期及是否回溯编制 |
| 沪深股票日线 | 新浪全历史 | 腾讯近期历史/收盘快照；BaoStock 仅校验或末级补采 | 腾讯常见接口存在近期窗口上限；不得据此确认更早历史完整 |
| 北交所日线 | 新浪全历史 | 腾讯收盘快照、TDX 日/分钟聚合交叉校验 | 新浪新代码可能带回北交所上市前的新三板历史，必须按 BSE 上市有效期过滤；新旧代码按身份合并 |
| ETF / LOF 日线 | 新浪全历史 | 腾讯近期历史；东财仅健康时末级补采 | LOF 样本的新浪全历史均成功；BaoStock 对该范围抽样为空，不进入常规候选 |
| 指数日线 | 新浪全历史 | 腾讯近期历史；BaoStock 仅长历史校验/末级补采 | 指数可能包含正式发布日前的回溯序列，需标记 `publish_date`、`calculation_start_date`、`is_backfilled_history` |
| 沪深股票/ETF/LOF/指数 1m | 腾讯原生分钟 | 公共 TDX 延迟分钟用于补采与盘后校准 | TDX 抽样每交易日 240 根，约 15 分钟延迟；盘中即时性与盘后最终性分开定义 |
| 北交所 1m | 公共 TDX 延迟分钟 | 新浪原生 5m 仅校验；腾讯快照仅在明确需要秒级 provisional 时启用 | 20 只北交所样本均取得 240 根/日；不再把快照聚合作为默认方案 |
| 5m | Canonical 1m 重采样 | 新浪/腾讯原生 5m 交叉校验或补缺 | 原生 5m 不得覆盖完整的 Canonical 1m 重采样结果 |
| 实时/收盘快照 | 腾讯批量快照 | 新浪快照 | 快照可生成 provisional 日线；生成分钟线只作为明确启用的降级路径 |

实测还确认了以下风险：

```text
EastMoney：当前环境多次出现 RemoteDisconnected，默认不进入日常热路径
BaoStock：部分资产无覆盖，且网络可用性不稳定，默认只做校验或末级补采
Sina / Tencent：作为第一阶段常规主来源，但仍必须受健康检查、限流和结果完整性校验约束
TDX：公共行情适合延迟分钟与盘后校准，不宣称交易所级实时或权威最终口径
```

抽样证据摘要：

| 验证项 | 结果 |
|---|---|
| 东财列表独有 LOF 的新浪历史能力 | 抽样 6/6 成功，共 10,629 行；证明东财可只参与低频列表并集，行情仍可走新浪 |
| 普通 LOF 历史 | `sh501018`、`sz166009` 新浪全历史成功；腾讯各返回近期 1,024 行；BaoStock 为空 |
| 北交所日线 | 新浪 `bj920000` 返回 1,397 行，且包含 BSE 上市前历史，确认必须按上市有效期过滤 |
| 北交所 1m | TDX 抽样 20/20 成功，每只 240 根；批量获取阶段很快，但服务启动、网络切点和持续运行仍需监控 |
| 跨市场 TDX 1m | 沪深股票、ETF、LOF、指数代表样本均取得 240 根/日 |
| 指数日线 | 新浪与 BaoStock 均能取得长历史，但 `sh000300` 起始日不同，确认需要回溯历史标记和冲突规则 |

已落盘的 LOF 验证摘要位于：

```text
tmp_test/etf_daily_probe/gap_lof_20260913/summary.json
tmp_test/etf_daily_probe/gap_lof_eastmoney_only_20260913/summary.json
```

探针脚本中“请求完成”不得直接映射为 `published`；零行结果必须按 `temporary_empty` 处理，这一规则同样适用于未来所有 Capability Probe。

## 6.2 Capability 必须是可执行配置

能力验证记录不能只是文档说明。Planner 只能选择现有 Metadata 中最近验证仍有效的具体记录；这里不新增独立能力管理层：

```text
provider_family + adapter + endpoint_version
+ dataset + market + asset_type + code_prefix
+ frequency + adjustment + field_level
+ history_window + freshness + finality
```

接口探针至少验证：HTTP/协议成功、非 HTML、非空语义、字段结构、首尾时间、返回条数上限、分页能力、单位、集合竞价口径和样本覆盖率。达到 `validation_expires_at` 后先降为 `unverified`，不得继续假定能力永久有效。

---

# 7. Endpoint 选择策略

请求：

```text
daily_bar
2026-09-11
all_stock
```

优先选择：

```text
1. full_market_by_date
2. bulk_daily
3. daily_history
4. snapshot
```

原则：

> 在数据质量相同的情况下，优先使用请求次数更少、批量能力更强的接口。

第一阶段不设置一个覆盖所有 Dataset 的“Provider 全局优先级”，而是按具体能力设置优先级。默认策略为：

```text
日常热路径：Sina / Tencent
延迟分钟与分钟校准：TDX
证券身份和代码迁移：交易所 / 指数发布方证据优先
低频补充或交叉校验：EastMoney / BaoStock
```

来源优先级只是同等质量候选的初始顺序。运行时 `provider_status`、结果完整度、时间范围、字段语义和数据 finality 可以使候选动态降级；不能为了坚持静态优先级而重复撞击已被屏蔽的接口。

首版 `selection_rules` 可直接按以下顺序生成；名称对应具体 Endpoint Capability，不是 Provider 全局开关：

```yaml
selection_rules:
  security_master:
    stock_xshg_xshe: [sina.security_list, tencent.snapshot_evidence, baostock.security_list]
    stock_bse: [bse_official.security_list, bse_official.code_mapping, sina.quote_evidence]
    etf_lof: [sina.fund_list, eastmoney.fund_list_union, tencent.snapshot_evidence]
    index: [official_publisher.index_catalog, local_reference.index_catalog, sina.index_quote_list]

  daily_bar:
    stock_xshg_xshe: [sina.full_history, tencent.recent_history, tencent.close_snapshot, baostock.daily_history]
    stock_bse: [sina.full_history, tencent.close_snapshot]
    etf_lof: [sina.full_history, tencent.recent_history, eastmoney.daily_history]
    index: [sina.full_history, tencent.recent_history, baostock.index_history]

  minute_bar_1m:
    xshg_xshe: [tencent.native_1m, tdx.delayed_1m]
    bse: [tdx.delayed_1m]

  snapshot:
    all_supported: [tencent.bulk_snapshot, sina.snapshot]
```

其中 `eastmoney.*` 默认 `enabled: false`，只有 Capability Probe 恢复后才可进入候选；`baostock.*` 默认 `role: validation_only`，主来源均不可用且该具体能力仍验证有效时，才临时提升为 fallback。数组中不存在的来源不得由 Collector 自行猜测。

Endpoint 进入候选集前必须同时通过：

```text
市场 / 资产 / 代码段支持检查
历史范围与返回行数检查
字段完整度检查
复权方式检查
分钟时间戳、集合竞价与成交量口径检查
来源健康检查
吞吐量与完成时限检查
Capability 验证是否仍在有效期内
```

对于实时分钟任务，Planner 必须估算：

```text
estimated_cycle_seconds
= ceil(symbol_count / max_symbols_per_request)
  / effective_concurrency
  × request_interval_seconds
```

如果预计单轮耗时超过采集周期或数据时效 SLA，则该 Endpoint 对当前 Universe 不可用。第一阶段分钟任务只允许 Watchlist，从调度入口禁止全市场分钟任务。

---

# 8. Snapshot 生成日线

收盘后快照如果包含：

```text
open
high
low
current
volume
amount
```

可以生成当天 provisional 日线：

```text
close = current
source_method = snapshot
quality_status = provisional
```

只能在收盘并留出安全缓冲后执行，例如：

```text
15:15 以后
```

Snapshot：

```text
可以：生成当前交易日 provisional daily_bar
不可以：补历史日期
```

历史数据必须使用：

```text
full_market_by_date
bulk_daily
daily_history
```

---

# 9. provisional → final

建议每日执行 reconciliation：

```text
T日收盘后
snapshot
↓
provisional

T+1早上
daily_history
↓
reconciliation
↓
final
```

每次回看最近 3 个交易日：

```yaml
reconciliation:
  enabled: true
  lookback_trading_days: 3
```

避免：

- 数据源延迟；
- 前一日同步失败；
- 节假日影响；
- Provider 后续修订。

---

# 10. 单位标准化

Canonical 层必须使用统一语义。

## 股票

```text
price  = 元
volume = 股
amount = 元
```

## ETF

```text
price  = 元
volume = 份
amount = 元
```

## LOF

```text
price  = 元
volume = 份
amount = 元
```

ETF 与 LOF 使用相同成交单位，但必须保留不同的 `asset_type`，不能仅因某一来源分类不同而互相覆盖。

## 可转债

```text
price  = 元
volume = 张
amount = 元
```

## 指数

指数成交量不能强行解释为“股”。

保留：

```text
volume
volume_semantics
```

例如：

```text
volume_semantics = provider_defined
```

---

# 11. Normalizer

所有转换集中在 Provider Normalizer 中。

例如：

```text
Provider A
股票 volume = 手
↓
Normalizer
↓
Canonical volume = 股
```

业务代码禁止自行出现：

```python
volume * 100
amount * 10000
```

等来源相关转换。

Normalizer 同时负责：

- symbol 格式；
- exchange；
- asset_type；
- 日期时间；
- timezone；
- OHLC 字段；
- volume；
- amount；
- source metadata。

单位规则必须至少按以下维度匹配，并记录规则版本和验证证据：

```text
provider
endpoint
market
asset_type
code_prefix
frequency
```

不能假设同一 Provider 的所有股票都使用相同成交量单位。例如特殊板块或代码段可能直接返回股，而其他证券返回手。无法确认的转换必须标记 `pending_validation`，不能进入 final。

---

# 12. 时间标准

A 股业务时区统一：

```text
Asia/Shanghai
```

所有 Provider 返回时间必须先明确解释其时区，再进入 Canonical。

Canonical 使用：

```text
trade_date
datetime
```

避免 UTC、本地时间、北京时间混存。

分钟数据还必须统一时间戳语义：

```text
bar_time_semantics = end_time
```

第一阶段 Canonical 统一使用分钟结束时间。例如 `09:31` 表示 `09:30:00 < t <= 09:31:00`。Normalizer 必须把使用开始时间标记的来源转换为结束时间后再比较和合并。

---

# 13. Raw Storage

Raw 用于保存原始采集事实。

第一阶段建议结构：

```text
data/
  raw/
    2026-09-11/
      tencent_153012.parquet
      eastmoney_153520.parquet
```

Raw 至少保留：

```text
provider
endpoint
fetch_time
task_id
symbol
raw payload / raw fields
```

原则：

> Raw 在保留期内不覆盖。

Raw 允许出现重复数据。

例如：

```text
600519 / 2026-09-10

Tencent 第一次
Tencent 第二次
EastMoney 第一次
EastMoney 第二次
```

这并不代表正式数据重复。

Raw 的职责只是：

> 记录“采集时实际发生过什么”。

---

# 14. Raw 生命周期

Raw 不要求永久无限保存。

建议配置：

```yaml
raw_storage:
  retention_days:
    daily_bar: 90
    snapshot: 30
    minute_bar_1m: 14
```

第一阶段可以先只保留配置，不实现自动清理。

等运行一段时间，根据真实磁盘增长情况再启用清理。

---

# 15. Canonical Storage

Canonical 是业务系统真正读取的数据。

日线建议：

```text
data/
  canonical/
    daily_bar/
      asset_type=stock/
        trade_date=2026-09-10/
          data.parquet
```

ETF：

```text
asset_type=etf
```

指数：

```text
asset_type=index
```

禁止生成：

```text
20260101_20260910.csv
```

任务可以跨日期，但最终必须按逻辑日期 Partition 落盘。

---

# 16. Canonical 主键

日线：

```text
(instrument_id, trade_date, adjustment)
```

分钟：

```text
(instrument_id, bar_time, interval_minutes, adjustment)
```

Snapshot：

```text
(instrument_id, snapshot_time, provider, endpoint)
```

正式 Canonical 数据不能出现主键重复。

---

# 17. Partition State

DuckDB 至少记录：

```text
dataset
asset_type
partition_key

expected_count
actual_count

status

manifest_path
content_hash
conflict_count
quarantined_count

updated_at
```

状态第一阶段控制为：

```text
missing
partial
complete_clean
complete_with_conflicts
invalid
```

例如：

```text
daily_bar
stock
2026-09-11

expected_count = 5367
actual_count   = 5201

status = partial
```

---

# 18. partition_item

用于精确记录某天哪些证券有效。

建议字段：

```text
dataset
partition_key
instrument_id

status

source_provider
updated_at
```

状态：

```text
success
no_trade
missing
invalid
conflict
quarantined
```

日线一天只有几千条 metadata，规模很小。

---

# 19. Missing Set

断点不能理解成：

```text
抓到证券列表第 2000 个
```

应该理解为：

```text
Expected Tradable Set
-
Canonical Valid Set
=
Missing Set
```

例如：

```text
Expected = 5300

Canonical Valid = 2000

Missing = 3300
```

下一 Provider 只处理这 3300。

---

# 20. 数据完成定义

必须明确：

```text
网络请求成功
≠
数据完成
```

只有：

```text
Fetch
↓
Normalize
↓
Validate
↓
Merge
↓
Canonical Publish
```

成功后，才能把：

```text
partition_item.status = success
```

Missing Set 只根据有效 Canonical 数据计算。

---

# 21. 停牌 / 无成交

不能因为 Security Master 中存在证券，但 Provider 没返回，就直接判定：

```text
missing
```

证券可能：

- 停牌；
- 当天无成交；
- 新上市特殊状态。

只有可靠依据确认后，才能标：

```text
no_trade
```

`no_trade` 的确认依据必须配置化，不能由任一快照中的零值或空值直接推断。建议：

```text
1. 交易所状态或含明确 trade_status 的权威历史接口
2. 两个独立 Provider 一致确认无成交，并通过上市/退市状态校验
3. 仍无法确认时保持 missing / unknown，等待盘后或次日 Reconciliation
```

Missing Set：

```text
Expected
-
(success + no_trade)
```

---

# 22. Provider Fallback

例如：

```text
Expected = 5300
```

Sina 主来源：

```text
成功 2000
```

则：

```text
Missing = 3300
```

Tencent 次来源：

```text
只请求 3300
成功 2500
```

则：

```text
Missing = 800
```

已验证且语义兼容的末级来源（例如该资产范围内的 BaoStock）：

```text
只补 800
```

目标不是“换一个 Provider 重新跑全部”，而是：

> 换 Provider 继续处理当前 Missing Set。

Fallback 只能在语义兼容的 Capability 之间发生。以下切换一律禁止静默执行：

```text
1m → 5m
不复权 → 前复权/后复权
final 历史 → provisional 快照
证券正式身份 → 未确认的同代码记录
完整字段级别 → 缺少核心字段且会造成字段退化的记录
```

若无兼容来源，保留 `missing` / `single_source` 并告警，不能伪造“已自动换源完成”。

---

# 23. 不支持指定 Symbol 的接口

某些 Endpoint 只能：

```text
一次返回整个市场
```

即使只缺 100 只，也只能完整请求。

这种情况下：

```text
Raw
可以保存完整返回
```

但实际进入补数流程的仅：

```text
Response ∩ Missing Set
```

---

# 24. Retry 与 Fallback

两者必须区分。

网络抖动：

```text
timeout
5xx
连接失败
```

先 Retry。

明显限流：

```text
403
429
连续拒绝
```

停止当前 Provider，切换到下一 Provider。

第一阶段不需要完整 Circuit Breaker 状态机。

可以简单：

```text
当前任务内连续失败 >= N 次
↓
冷却该 Provider 的具体 Endpoint + 市场 + 资产类型
↓
切换下一来源
```

冷却到期后由 Capability Probe 小流量试探；探针通过后，后续 Scheduler 才恢复该能力。冷却期内不因新任务重复撞击。

运行状态应细化到 `provider + endpoint + market + asset_type`，并增加：

```text
opened_at
open_until
cooldown_seconds
last_failure_class
```

对于连续返回 HTML、RemoteDisconnected、403/429 等稳定性问题，冷却期内不应由同一日的后续调度反复尝试。一个 Endpoint 失败不能错误禁用同 Provider 的其他 Endpoint。

采集尝试必须进入显式状态机：

```text
pending
  → leased
  → fetching
  → raw_committed
  → normalized
  → validated
  → published
```

异常状态：

```text
retryable_failed
terminal_failed
temporary_empty
confirmed_no_data
conflict
quarantined
```

只有 `published` 或有充分证据的 `confirmed_no_data` 才算完成。HTTP 200、请求未报错、返回空数组或 Raw 已写入都不等于完成。

---

# 25. 重复采集处理

Raw 允许重复。

Canonical 不允许。

例如：

```text
Tencent run1
Tencent run2
EastMoney run1
EastMoney run2
```

最终：

```text
(instrument_id, trade_date, adjustment)
```

只能保留一个最终结果。

---

# 26. Resolution Policy

建议第一阶段规则：

```text
0. 只在 adjustment、交易日期和证券身份一致时参与仲裁

1. valid final > valid provisional

2. 同质量状态下，字段完整度更高者优先

3. 同完整度时：daily_history > snapshot

4. 同等级时：
   capability_priority 高且当前健康的来源优先；默认 Sina / Tencent 高于 EastMoney / BaoStock

5. 同 Provider 同质量但数据不同：
   新记录必须通过完整性和异常回归检查后，才允许最新 fetch_time 优先

6. 明显差异：
   写 conflict_log
```

字段完整度建议分级：

```text
ohlc
ohlcv
full_bar       # OHLCV + amount + pre_close + trade_status
extended_bar   # full_bar + turnover / amplitude 等
```

`daily_history > snapshot` 不能无条件成立。例如缺少 `amount`、`pre_close`、`trade_status` 的历史行，不应静默覆盖字段更完整的快照行。业务核心 K 线和估值/扩展指标可以拆成不同 Dataset，避免为了补一个字段进行跨来源拼接。

---

# 27. 同 Provider 重复

例如：

```text
Tencent 第一次：
close = 10.21

Tencent 第二次：
close = 10.21
```

完全相同：

```text
直接视为重复
```

如果：

```text
第一次 = 10.21
第二次 = 10.22
```

且都是：

```text
daily_history / final
```

则：

```text
先检查新值非空、字段未退化且通过 Validator，再采用较新的 Provider 修订值
+
记录 conflict_log
```

---

# 28. 不同 Provider 冲突

例如：

```text
Tencent daily_history:
close = 10.21

Provider B daily_history:
close = 10.28
```

不能静默忽略。

建议配置阈值：

```yaml
conflict_policy:

  daily_bar:

    price_relative_diff: 0.001
    volume_relative_diff: 0.01
    amount_relative_diff: 0.01
```

超过阈值后先按冲突等级处理：

```text
soft_conflict：记录 conflict_log，按确定性 Resolution Policy 选值
hard_conflict：隔离该证券/日期记录，保留在 Missing Set 或人工复核队列
```

`hard_conflict` 不阻塞整个市场其他证券发布，但不得把冲突记录静默选成正式值。分区状态至少区分：

```text
complete_clean
complete_with_conflicts
partial
invalid
```

阈值不能全市场共用一组常量，应按 `dataset + provider_pair + market + asset_type + frequency + field` 配置。尤其北交所样本中已观察到 TDX/腾讯与新浪成交量、成交额明显不一致，必须先记录来源原值和口径证据，不能预设任一方恒为正确。

---

# 29. conflict_log

DuckDB 表建议：

```text
dataset
instrument_id
trade_date
adjustment

provider_a
provider_b

field

value_a
value_b

diff_ratio

selected_provider

resolution_rule
severity              # soft / hard
capability_version_a
capability_version_b
raw_object_path_a
raw_object_path_b

conflict_type

created_at
status
```

---

# 30. 禁止字段级跨来源拼接

第一阶段禁止：

```text
open   来自腾讯
close  来自腾讯
volume 来自东财
amount 来自新浪
```

一条 Canonical Record 必须整体选择一个 Provider 的完整记录。

这样能避免口径混乱。

如果某来源只能提供 OHLCV、另一来源提供成交额或估值，优先拆为：

```text
daily_bar_core
daily_bar_extended
valuation_snapshot
```

通过相同 `instrument_id + trade_date` 查询组合，而不是在同一 Canonical Record 内拼接字段。

---

# 31. Canonical 更新

后续补数不能简单覆盖旧文件。

例如原文件：

```text
3000条
```

新补：

```text
1800条
```

执行：

```text
existing
+
new
↓
按主键 merge
↓
Resolution
↓
Validate
↓
写 temp
↓
Atomic Replace
```

最终：

```text
4800条
```

不会生成：

```text
20260910_retry.csv
20260910_retry2.csv
```

---

# 32. Atomic Publish

禁止直接修改当前正式文件。

流程：

```text
读取现有 Partition + 新数据
↓
Merge + Validate
↓
写同目录唯一临时文件
2026-09-10.{run_id}.tmp.parquet
↓
写 manifest：row_count / key_range / schema / content_hash / raw_refs
↓
flush + fsync 文件与目录
↓
复读校验临时文件和 manifest
↓
os.replace 原子替换正式文件
↓
DuckDB 事务更新 partition_status / collection_attempt
```

同一文件系统内使用原子 rename，保证业务层不会读到半写文件。

启动时必须清理或审计过期临时文件。临时文件只有在校验完成并携带完整 manifest 时才能发布；无法确认来源的 `.tmp` 文件移入 quarantine，不直接覆盖正式 Partition。

断电可能发生在每一步之间，启动恢复规则必须确定且可重复执行：

```text
Raw 已完整写入且 hash 正确
→ 从 Normalize 继续，不重复请求来源

临时 Parquet 存在但 manifest 不完整或校验失败
→ quarantine，不发布

临时 Parquet + manifest 完整，但尚未 replace
→ 重新校验后完成 replace

正式文件已 replace，但 DuckDB 元数据未提交
→ 以正式文件 manifest 修复元数据，不重新下载

租约过期且没有完整 Raw
→ 释放 stale lease，重新进入 Missing Set
```

恢复完成后必须从 Canonical 重新计算 Missing Set。切换来源时仅请求缺失子集，不能因进程重启重抓整个 Universe。

---

# 33. 并发写控制

网络请求可以并发。

但同一个：

```text
dataset + trade_date + asset_type
```

一次只允许一个 Publish。

第一阶段可以使用：

```text
本地文件锁
或
进程内锁
```

避免两个任务同时修改同一个 Partition。

不需要为了这个引入 PostgreSQL。

---

# 34. Task 幂等

重复执行：

```text
daily_bar
2026-09-10
all_stock
```

Planner 必须重新计算 Missing Set。

如果：

```text
status IN (complete_clean, complete_with_conflicts)
```

直接：

```text
No-op
```

不会重新抓全市场。

## 34.1 长任务检查点与租约恢复

Missing Set 解决的是已经进入有效 Canonical 的缺口，不能替代采集过程中的检查点。逐证券、长日期范围任务必须按以下粒度记录：

```text
task_id
run_id
attempt_id

dataset
market
asset_type
item_key

provider
endpoint
capability_version

instrument_id
window_start
window_end
status

lease_owner
lease_acquired_at
lease_expires_at

raw_object_path
raw_content_hash
row_count
returned_first_key
returned_last_key
expected_count
accepted_count
rejected_count

attempt_count
failure_class
last_error
created_at
updated_at
```

任务中断后优先复用已经完整写入的 Raw，避免“数据已下载但未发布”时重复请求 Provider。`EMPTY` 不能直接作为完成状态，必须区分：

```text
confirmed_no_data
temporary_empty
request_failed
```

只有 `published` 或有权威依据的 `confirmed_no_data` 才能从 Missing Set 中排除。

执行语义为：

```text
采集 At-least-once
+
Canonical 发布幂等
```

Raw 保留每次尝试；Canonical 以业务主键唯一。若新记录 `row_hash` 与已发布记录相同则 No-op；同来源修订保留旧 Raw 并记录修订，跨来源差异进入 Resolution/Conflict 流程。

---

# 35. 日期区间任务

请求：

```text
2026-01-01 ~ 2026-09-10
```

只表示任务范围。

Planner 根据 Trading Calendar 拆成：

```text
2026-01-05
2026-01-06
2026-01-07
...
```

每一天单独检查：

```text
complete / partial / missing
```

只对真正缺失的日期执行采集。

---

# 36. Trading Calendar

必须维护统一交易日历。

避免：

```text
周末
法定节假日
```

被判断成数据缺失。

交易日历优先使用交易所公告/日历，新浪或腾讯交易数据用于交叉检查，BaoStock 只作为已通过探针时的低优先级补充，并落本地 DuckDB。任何单一行情来源当天为空都不能直接把交易日改成休市日。

---

# 37. 分钟数据

分钟数据独立 Dataset：

```text
minute_bar_1m
minute_bar_5m
```

第一阶段分钟 Universe 明确收缩为 Watchlist：

```text
允许：watchlist / strategy_candidate / static symbols
禁止：all_stock / all_etf / all_lof / all_index
```

Watchlist 可以包含股票、ETF、LOF和指数，但每次启动及运行中变更时都必须重新执行吞吐量预算。超过配置的 `max_watchlist_symbols` 或无法满足 `cycle_deadline_seconds` 时，任务拒绝扩容并告警，不静默降低分钟时效。

分钟数据分为两条采集链：

```text
                    minute_bar_1m
                         │
          ┌──────────────┴──────────────┐
          │                             │
   Realtime Collector             History Collector
       盘中实时                        历史补采
          │                             │
 realtime_minute / snapshot       minute_history
          │                             │
          └──────────────┬──────────────┘
                         ↓
                    Normalize
                         ↓
                  Hot / Canonical
```

Realtime 负责盘中可用性，History 负责历史回补和盘后校准。

---

# 38. 分钟 Provider Capability

Provider 需要明确声明是否支持：

```text
realtime_minute
realtime_quote
minute_history
```

例如：

```yaml
providers:

  provider_a:

    endpoints:

      realtime_minute:
        enabled: true
        datasets:
          - minute_bar_1m
        realtime: true
        native_minute_bar: true
        supports_symbol_filter: true
        max_symbols_per_request: 100
        request_interval_seconds: 1
        cycle_deadline_seconds: 50
        freshness_delay_seconds: 3

      realtime_quote:
        enabled: true
        datasets:
          - snapshot
          - minute_bar_1m
        realtime: true
        native_minute_bar: false
        supports_symbol_filter: true
        max_symbols_per_request: 100
        request_interval_seconds: 1
        volume_mode: cumulative
        amount_mode: cumulative

      minute_history:
        enabled: true
        datasets:
          - minute_bar_1m
          - minute_bar_5m
        historical: true
        supports_symbol_filter: true
        frequencies:
          - 1m
          - 5m
        max_symbols_per_request: 1
        max_rows_per_request: 120
        max_days_per_request: 5
```

结合 2026-09-13 当前接口验证，第一阶段采用以下边界：

```text
沪深 stock / ETF / LOF / index 1m：Tencent 原生分钟优先，公共 TDX 延迟分钟补采/校准
BSE 1m：公共 TDX 延迟分钟优先，约 15 分钟延迟；抽样 20/20 成功且每交易日 240 根
5m：Canonical 1m 重采样优先，Tencent / Sina 原生 5m 用于校验或补缺
BaoStock 分钟：只覆盖部分范围，不进入常规优先队列
EastMoney 分钟：当前网络环境多次 RemoteDisconnected，默认禁用，仅恢复验证后才能进入候选集
```

因此 Provider Fallback 的承诺仅限“Capability确认可覆盖的 Watchlist 子集”。如果某证券没有第二来源，状态必须标记 `single_source`，不能伪装成具备自动换源能力。

TDX 分钟数据还必须记录：

```text
freshness_class = delayed
expected_delay_seconds ≈ 900
source_server
server_connect_time
auction_semantics
```

抽样中首根 09:31 分钟与日线开盘价存在 0.01 的集合竞价口径差异，且个别北交所证券的成交量/成交额与新浪差异明显。1m 聚合结果不得无条件反推官方日线 open/volume/amount。

---

# 39. 分钟来源优先级

沪深 Watchlist 盘中分钟优先：

```text
1. Tencent 原生 realtime_minute
2. 公共 TDX delayed minute
3. Tick / 高频行情自行聚合（仅已验证来源）
4. Tencent / Sina realtime_quote 或 snapshot 聚合
```

北交所第一阶段接受约 15 分钟延迟，优先：

```text
1. 公共 TDX delayed minute
2. Tencent snapshot 聚合（仅当业务明确要求秒级 provisional 时启用）
3. Sina 原生 5m 仅作校验或 5m 缺口补采，不得冒充 1m
```

能直接获取原生 1 分钟 K 时，不优先使用普通快照自行聚合。

原因：

- 快照轮询可能丢失分钟内最高价；
- 可能丢失分钟内最低价；
- 请求延迟和限流会造成采样不完整；
- 累计成交量需要自行计算差值。

快照聚合出的分钟数据默认：

```text
quality_status = provisional
```

Snapshot聚合只允许用于规模经过吞吐校验的Watchlist，不得作为全市场分钟备用方案。北交所约 343 只按腾讯每批 50 只、批间 2～3 秒估算，一次全扫描约 18～21 秒，每只每分钟只有少量采样，无法保证分钟内真实最高价和最低价，因此不作为默认采集链。普通批量快照即使能够覆盖全市场，也可能无法在一分钟内完成一轮采集。

---

# 40. Snapshot 聚合一分钟 K

如果 Provider 只有：

```text
symbol
timestamp
price
cum_volume
cum_amount
```

可以生成一分钟 K。

例如：

```text
09:31:03  10.21
09:31:15  10.22
09:31:40  10.20
09:31:58  10.23
```

生成：

```text
09:31
open  = 10.21
high  = 10.23
low   = 10.20
close = 10.23
```

累计成交量转换为单分钟成交量：

```text
volume_09:31
=
cum_volume_09:31末
-
cum_volume_09:30末
```

成交额同理。

因此 Provider Capability 必须明确：

```yaml
volume_mode: cumulative
amount_mode: cumulative
```

或：

```yaml
volume_mode: interval
amount_mode: interval
```

单位换算仍由 Normalizer 完成。

聚合器还必须处理：

```text
首根分钟缺少前值
跨交易日累计值重置
午间是否重置
请求失败造成的采样缺口
乱序 / 重复 / 延迟快照
累计成交量或成交额出现负差值
分钟闭合后的迟到修订
```

任一窗口采样不足以证明最高价/最低价完整时，增加 `incomplete_sampling`，保持 provisional，等待盘后历史分钟校准。

---

# 41. 实时分钟 Hot Store

盘中实时分钟数据不能依赖“每 5~15 分钟写一次 Parquet”才能查询。

因此 Hot Minute 第一阶段采用：

```text
SQLite WAL
```

作为当天实时分钟存储。

建议文件：

```text
data/
  hot/
    minute_hot.db
```

SQLite 不需要独立数据库服务，只是一个本地文件，适合当前轻量单机架构。

## 41.1 写入流程

```text
Realtime / Delayed Minute Collector
↓
原生分钟 / TDX 延迟分钟 / Snapshot 聚合
↓
Normalize
↓
SQLite WAL
↓
立即可查询
```

完整的一分钟 K 在所选来源可见后立即写入 SQLite；存储层不再额外制造延迟。

例如当前时间：

```text
09:32:02
```

此时：

```text
09:31
```

若使用腾讯原生分钟，这一根完整分钟 K 应已经可以查询；若使用北交所 TDX 延迟分钟，则在来源约 15 分钟后放出该根 K 时立即可查。

不需要为了 Parquet Flush 再等待 5~15 分钟。

如果 Provider 提供秒级 Snapshot：

```text
snapshot
```

可以单独提供秒级行情查询；一分钟 K 则在分钟闭合后立即可查。

## 41.2 Hot Minute 表

建议表：

```text
hot_minute_bar
```

核心字段：

```text
instrument_id
source_symbol
bar_time
interval_minutes
adjustment

open
high
low
close

volume
amount

source_provider
source_method

quality_status
freshness_class
source_delay_seconds
as_of

updated_at
```

主键：

```text
(instrument_id, bar_time, interval_minutes, adjustment)
```

写入使用 UPSERT。

同一个分钟如果后续获得更完整的数据，可以根据 Resolution Policy 更新当前 Hot Record。

## 41.3 为什么使用 SQLite WAL

Hot 数据特点：

```text
高频写入
持续更新
盘中频繁查询
只保存当天或最近少量交易日
```

SQLite WAL 比反复重写 Parquet 更适合。

同时它仍然保持：

```text
无需 Redis
无需 PostgreSQL
无需额外数据库进程
```

查询线程读取 WAL 时，不阻塞 Realtime Collector 正常写入。

## 41.4 Parquet 的职责

Parquet 不承担盘中实时查询职责。

它负责：

```text
盘后历史数据
批量分析
长期存储
```

收盘后的分钟数据流程：

```text
SQLite Hot Minute
+
minute_history
↓
Reconciliation
↓
Validate
↓
Resolution
↓
Canonical Parquet
```

最终形成：

```text
data/canonical/minute_bar_1m/
  trade_date=2026-09-11/
    data.parquet
```

## 41.5 查询策略

统一查询接口不要求业务层判断数据存在哪里。

例如：

```text
GET /api/market/minute-bars
```

查询：

```text
2026-09-10 ~ 当前
```

内部自动执行：

```text
历史日期
→ Canonical Parquet

当前交易日
→ SQLite Hot Minute
```

然后按照：

```text
(instrument_id, bar_time, interval_minutes, adjustment)
```

合并、排序后统一返回。

因此上层：

- K 线页面；
- 技术指标；
- 盘中策略；
- 自选股页面；

都不需要区分“历史分钟”和“当天实时分钟”。

## 41.6 查询时效

目标：

```text
Snapshot：
取决于 Provider，可达到几秒级

沪深 Watchlist 完整 1m K：
Tencent 正常时，分钟闭合后数秒内可查询

北交所 Watchlist 完整 1m K：
公共 TDX 正常时，接受约 15 分钟延迟；实际延迟写入每条记录

历史 Canonical：
盘后 Reconciliation 后作为 final 数据查询
```

因此，原文所说的：

```text
5~15 分钟
```

不应来自 Parquet Flush，但可以来自所选 Provider 自身的数据延迟。查询 API 必须返回 `quality_status`、`freshness_class`、`source_delay_seconds` 和 `as_of`，避免把延迟行情展示成实时行情。

实时查询链路必须直接读取 SQLite Hot Store，禁止以 Parquet Flush 或 WAL checkpoint 是否完成作为可查询条件。

如果后续仍希望定期做额外持久化，可以每 5~15 分钟进行：

```text
SQLite checkpoint / backup
```

但它与实时查询无关。

---
# 42. 实时分钟 Missing 处理

盘中不把每个 `symbol + minute` 都写进 DuckDB Metadata。

当前采集窗口内，在内存或当前任务状态中维护 Missing Symbols。

例如：

```text
当前 Watchlist 监控 68 只证券

Provider A：
成功 65
失败 3
```

Provider B：

```text
只补失败的 3 只
```

如果某 Endpoint 不能指定 symbol，只能返回全市场：

```text
请求全市场
↓
只接收 Missing Symbols 对应记录
```

即使接口返回全市场，也必须先验证单次请求能在 `cycle_deadline_seconds` 内完成；否则它不能作为实时分钟 Endpoint，只能作为盘后补采候选。

盘后再统一检查整日分钟完整性。

---

# 43. 分钟历史任务拆分

请求：

```text
Watchlist中的68只证券
一年
1m
```

Planner 根据 Provider Capability 自动拆分。

例如 Provider：

```yaml
max_symbols_per_request: 1
max_rows_per_request: 120
max_days_per_request: 5
supports_pagination: false
```

则生成：

```text
股票A / 5天
股票A / 下5天
...
股票B / 5天
...
```

业务层不处理接口分页和时间范围限制。

每个子任务完成后必须验证返回范围，而不是只看 HTTP 成功：

```text
returned_first_time <= requested_first_time
returned_last_time  >= requested_last_time
returned_rows       >= expected_minimum_rows
```

如果达到 `max_rows_per_request` 且没有分页或游标能力，只能确认该接口的可见窗口，不能把更早历史标成完成。

---

# 44. 分钟盘后 Reconciliation

盘中数据用于快速可用，盘后历史分钟用于正式校准。

建议收盘后执行：

```text
Hot Minute
+
minute_history
↓
Normalize
↓
主键去重
↓
Resolution
↓
Validate
↓
Compact
↓
Canonical Minute
```

正式分钟主键：

```text
(instrument_id, bar_time, interval_minutes, adjustment)
```

默认优先级按“质量 + 语义 + 当前健康状态”确定，而不是只按来源名：

```text
通过完整性校验的 minute_history / TDX delayed minute final-candidate
>
native realtime_minute provisional
>
snapshot aggregated provisional
```

盘后只有在交易日 240 根、Session 边界、时间戳、单位、重复键和异常跳变均校验通过后，TDX delayed minute 才能从 `final-candidate` 转为 `final`。明显冲突写入：

```text
conflict_log
```

但不阻断整日发布。

## 44.1 5分钟生成策略

第一阶段 Canonical 5分钟优先由已经确认的 Canonical 1分钟重采样生成：

```text
open   = 第一根1m.open
high   = max(1m.high)
low    = min(1m.low)
close  = 最后一根1m.close
volume = sum(1m.volume)
amount = sum(1m.amount)
```

Provider原生5分钟用于历史缺口补采和交叉校验。只有1分钟数据不完整时，才允许原生5分钟成为正式记录，并标记 `derived_from=native_5m`。这样避免1分钟与5分钟长期使用不同来源和不同口径。

---

# 45. 分钟数据存储

分钟数据采用：

```text
盘中 SQLite Hot
+
盘后 Parquet Canonical
```

两层结构。

## 45.1 盘中

```text
data/hot/minute_hot.db
```

SQLite WAL 持续接收当天实时分钟数据。

特点：

```text
来源可见后立即写入
立即可查询
允许同分钟 UPSERT
```

## 45.2 盘后

Reconciliation 完成后写入：

```text
data/
  canonical/
    minute_bar_1m/
      trade_date=2026-09-11/
        data.parquet
```

Canonical 主键：

```text
(instrument_id, bar_time, interval_minutes, adjustment)
```

当天正式 Parquet 发布完成后：

```text
Hot 数据可清理
或保留最近 1~3 个交易日用于排错
```

第一阶段不做复杂 Bucket。由于分钟范围为 Watchlist，容量按配置上限估算：

```text
max_watchlist_symbols × expected_minutes_per_day

例如：
200 × 240 = 48,000 行/日
```

Parquet + DuckDB 足够处理历史分析。

只有实际出现：

- 单日文件明显过大；
- Merge 明显变慢；
- 查询明显变慢；

再引入 Bucket。

---
# 46. 分钟完整性

分钟 Metadata 不逐分钟记录。

按：

```text
instrument_id + trade_date + interval_minutes + adjustment
```

记录：

```text
expected_rows
actual_rows
coverage_ratio
first_timestamp
last_timestamp
status
```

不能永久简单写死：

```text
expected_rows = 240
```

因为需要考虑：

- 停牌；
- 临时停牌；
- 新股特殊情况；
- Provider 漏数据；
- 未来交易制度变化。

---

# 47. 市场交易时段配置

实时 Collector 不把交易时段写死在代码中。

建议：

```yaml
markets:

  CN_A:
    timezone: Asia/Shanghai

    sessions:
      - ["09:30", "11:30"]
      - ["13:00", "15:00"]

    bar_time_semantics: end_time
    expected_regular_minutes: 240
    include_opening_auction: false
    include_closing_auction: true

    realtime_start: "09:25"
    realtime_stop: "15:05"
```

Realtime Collector 根据：

```text
Trading Calendar
+
Market Session Config
```

决定当天是否运行以及何时采集。

必须明确09:30首根、11:30边界、13:00首根、15:00收盘分钟的归属。集合竞价、盘后交易与连续竞价分开配置，不能由不同Provider返回什么就被动决定Canonical分钟数量。

---

# 48. 分钟实时调度

建议：

```text
08:00
Security Master 更新

08:30
最近3个交易日日线 Reconciliation

09:25
启动 Realtime Collector

09:30 ~ 11:30
持续采集实时行情
↓
完整分钟形成后立即写 SQLite Hot Store

13:00 ~ 15:00
持续采集实时行情
↓
完整分钟形成后立即写 SQLite Hot Store

15:05
停止实时 Collector
并确认最后一分钟已落 Hot Store

15:20+
Minute Reconciliation
↓
生成 Canonical Parquet

16:20
Daily Bar 同步

20:00
缺失 Partition 重试
```

SQLite WAL 本身持续持久化，不依赖 5~15 分钟 Flush 才能查询。

可以额外配置低频：

```text
checkpoint / backup
```

但其目的仅是 WAL 控制和容灾，不影响实时读取。

具体时间全部配置化。

---
# 49. YAML 配置

Provider 的 timeout、QPS、并发、Retry 必须配置化，并细化到 Endpoint。

实时分钟 Hot Store 同样配置化：

```yaml
realtime_minute:

  enabled: true

  universe: watchlist
  max_watchlist_symbols: 200
  cycle_deadline_seconds: 50
  reject_full_market_universe: true

  hot_store:
    type: sqlite
    path: data/hot/minute_hot.db
    wal: true

  query:
    include_hot_today: true

  finalize:
    delay_seconds: 2

  retention:
    hot_trading_days: 3

  checkpoint:
    enabled: true
    interval_minutes: 10

  freshness_sla:
    XSHG: 10
    XSHE: 10
    BSE: 1200
```

其中：

```text
finalize.delay_seconds
```

表示一分钟结束后额外等待少量时间再确认该分钟 K，避免 Provider 最后一笔数据稍有延迟。

`checkpoint.interval_minutes` 只影响 WAL checkpoint，不影响查询时效。

例如：

```yaml
providers:

  source_policy:
    daily_hot_path:
      - sina
      - tencent
    delayed_minute:
      - tdx
    validation_or_last_resort:
      - eastmoney
      - baostock

  tencent:

    enabled: true
    priority: 100

    endpoints:

      daily_history:

        enabled: true

        timeout:
          connect: 3
          read: 15

        rate_limit:
          qps: 1
          concurrency: 1

        retry:
          max_attempts: 3
          retry_wait_seconds: 2

      snapshot:

        enabled: true

        timeout:
          connect: 3
          read: 5

        rate_limit:
          qps: 3
          concurrency: 2

        retry:
          max_attempts: 2
          retry_wait_seconds: 1

  eastmoney:
    enabled: false
    disabled_reason: unstable_in_current_network
    require_capability_probe_before_enable: true

  baostock:
    enabled: true
    default_role: validation_only
    promote_only_when:
      capability_validated: true
      primary_sources_unhealthy: true
```

同一个 Provider 不同 Endpoint 可以有不同限制。

---

# 50. 配置与运行状态分离

YAML 保存：

```text
默认启停
按 Dataset / Endpoint / Market / Asset Type 的优先级与角色
QPS
并发
timeout
retry
Capability
Capability 验证有效期
```

DuckDB 保存：

```text
最近成功时间
最近失败时间
连续失败次数
403次数
429次数
当前健康状态
```

第一阶段管理后台可以暂时不做。

后续：

```text
YAML Default
+
DuckDB Override
=
Runtime Config
```

即可支持页面动态维护。

---

# 51. DuckDB 元数据表

第一阶段建议只保留必要表：

```text
security_master

security_master_history

security_symbol_history

trading_calendar

partition_status

partition_item

provider_status

conflict_log

collection_task

collection_attempt
```

`collection_attempt` 即原先可选的 `collection_run_detail` 的正式化版本，第一阶段必须实现；它承载尝试状态、租约、Raw 引用、覆盖范围和断电恢复。可以不做 Task/Run/Batch 三层复杂模型，但不能省略逐尝试恢复信息。

不强制第一阶段实现：

```text
task / run / batch 三级状态
Schema Registry
多版本长期保存
```

---

# 52. provider_status

建议字段：

```text
provider
endpoint
market
asset_type
dataset
capability_version

enabled

last_success_at
last_failure_at

opened_at
open_until
cooldown_seconds

consecutive_failures

last_probe_at
probe_expires_at
last_success_coverage

http_403_count
http_429_count

last_error
```

足够支撑后续管理台。

---

# 53. 推荐目录结构

```text
stock-data/

├── config/
│   ├── datasets.yaml
│   ├── providers.yaml
│   ├── collection.yaml
│   └── schedules.yaml
│
├── src/
│
│   ├── providers/
│   │   ├── base.py
│   │   ├── sina.py
│   │   ├── tencent.py
│   │   ├── tdx.py
│   │   ├── eastmoney.py
│   │   ├── baostock.py
│   │   └── ...
│
│   ├── security_master.py
│
│   ├── planner.py
│
│   ├── collector.py
│
│   ├── realtime_collector.py
│
│   ├── minute_query.py
│
│   ├── normalizer.py
│
│   ├── validator.py
│
│   ├── storage.py
│
│   ├── metadata.py
│
│   └── scheduler.py
│
├── data/
│   ├── raw/
│   ├── hot/
│   │   └── minute_hot.db
│   └── canonical/
│
└── metadata/
    └── metadata.duckdb
```

---

# 54. Scheduler

第一阶段使用：

```text
APScheduler
```

或：

```text
systemd timer
```

即可。

Scheduler 只负责触发任务，不负责采集逻辑。

建议调度与第 48 节保持一致：

```text
08:00  Security Master 更新
08:30  最近3个交易日日线 Reconciliation
09:25  启动 Watchlist Realtime Collector
09:30~11:30 Watchlist实时写 SQLite Hot Minute
09:45~11:45 北交所按 freshness SLA 拉取 TDX 延迟分钟增量
13:00~15:00 Watchlist实时写 SQLite Hot Minute
13:15~15:15 北交所按 freshness SLA 拉取 TDX 延迟分钟增量
15:05  停止 Realtime Collector
15:20+ Minute Reconciliation
16:20  Daily Bar 同步
20:00  缺失 Partition 重试
```

盘中 09:30~11:30、13:00~15:00 的实时采集由 Realtime Collector 按 Market Session Config 控制，而不是每分钟创建 Scheduler Job。

所有时间放入 YAML。

---

# 55. Phase 1 实施范围

第一阶段必须完成：

```text
Security Master

Trading Calendar

daily_bar

minute_bar_1m

minute_bar_5m（优先从Canonical 1m重采样）

日线：stock / ETF / LOF / index 全量Universe

分钟：watchlist中的 stock / ETF / LOF / index

Sina / Tencent 常规主来源；TDX 分钟来源仅在公共主站 Probe 稳定通过时启用

EastMoney / BaoStock 低优先级校验或末级补采适配器

Provider Capability

现有 Metadata 能力验证记录 + 定期探针 + 验证过期机制

Capability吞吐量与历史窗口校验

RealtimeCollector

SQLite WAL HotMinuteStore

统一 Minute Query（Canonical + Hot）

MinuteReconciliation

Missing Set

Provider Fallback

语义兼容的 Fallback 与 Missing 子集切源

Normalizer

单位统一

Raw Storage

Canonical Partition

主键去重

Resolution Policy

Conflict Log

soft/hard conflict 分级与单条隔离

临时文件 + Atomic Publish

collection_attempt + lease + Raw hash + 断电恢复

故障注入测试：超时/HTML/429/空返回/静默截断/进程中止/重复任务/发布中断

YAML Provider Config

Partition State
```

---

# 56. Phase 1 暂缓

暂时不实现：

```text
复杂 DAG

Redis

Celery

Kafka

RabbitMQ

完整 Circuit Breaker

Task / Run / Batch 三级状态

动态 Bucket

Schema Registry Service

长期 Partition Version

复杂字段级数据仲裁

估值、行业、概念、财务等扩展指标的全量 SLA
```

这些属于运行规模扩大后的增强能力。尤其扩展指标不能因为东财存在某个接口就默认“已有能力”；必须先为每个 Dataset 找到至少一个稳定主来源，完成字段语义、历史窗口、更新频率和授权边界验证，再写入现有 Metadata 的能力验证记录。当前来源组合足以启动第一阶段的证券主数据、日线、分钟和快照，不等于所有扩展指标已经具备生产级来源。

---

# 57. 验收场景

## 场景 1：单位统一

Provider：

```text
股票 volume = 手
amount = 万元
```

Canonical：

```text
volume = 股
amount = 元
```

---

## 场景 2：部分采集

当日预计：

```text
5300只
```

Provider A：

```text
成功2000
```

结果：

```text
status = partial

Missing = 3300
```

不能误判为 complete。

---

## 场景 3：自动换源

Provider A 被限流。

Provider B：

```text
只处理剩余 Missing Set
```

不重新抓完整市场。

---

## 场景 4：重复采集

Tencent：

```text
第一次2000
第二次3000
```

Sina：

```text
第一次2000
第二次1000
```

Raw 允许保留。

Canonical：

```text
(instrument_id, trade_date, adjustment)
```

只能有一条。

---

## 场景 5：补数据

旧 Partition：

```text
3000
```

新数据：

```text
1800
```

执行：

```text
Merge
↓
4800
↓
Temp
↓
Atomic Replace
```

不是创建第二个日期文件。

---

## 场景 6：Snapshot

T 日：

```text
snapshot
↓
provisional
```

T+1：

```text
daily_history
↓
final
```

---

## 场景 7：历史回补

补：

```text
2024-01-05
```

Expected Set 使用：

```text
2024-01-05 当时已上市证券
```

而不是当前股票列表。

---

## 场景 8：停牌

Security Master 中存在证券。

可靠来源确认当天停牌：

```text
status = no_trade
```

不计入 Missing Set。

---

## 场景 9：不同 Provider 冲突

Provider A：

```text
close = 10.21
```

Provider B：

```text
close = 10.28
```

超过阈值：

```text
写 conflict_log
```

但按照 Resolution Policy 选择正式数据，不阻塞整个市场发布。

---

## 场景 10：并发写

两个任务同时处理：

```text
2026-09-10 daily_bar
```

必须通过 Partition Lock 保证：

```text
同一时间只能一个任务 Publish
```

---

## 场景 11：盘中实时分钟与即时查询

09:30 后 Realtime Collector 启动。

Provider A 提供原生 1 分钟 K。

例如：

```text
09:31:00 ~ 09:31:59
```

一分钟闭合后：

```text
09:32:00 + finalize_delay
```

系统完成：

```text
Normalize
↓
SQLite WAL Hot Store
```

此时业务接口必须能够立即查询到：

```text
09:31
```

这一根完整 K 线。

不得等待 5~15 分钟 Parquet Flush。

查询历史 + 当天时：

```text
Canonical Parquet
+
SQLite Hot Minute
↓
统一返回
```

因此业务层无需区分两种存储。

---

## 场景 12：快照生成分钟线

Provider B 没有原生分钟接口，仅有累计行情快照。

系统按照分钟窗口生成 OHLC，并通过累计值差分生成：

```text
volume
amount
```

结果标记为：

```text
provisional
```

不得直接作为长期 final 数据。

---

## 场景 13：盘中实时来源失败

当前监控 68 只证券。

Provider A：

```text
成功 65
失败 3
```

Provider B 只补：

```text
Missing Symbols = 3
```

不重新处理已成功的 65 只。

---

## 场景 14：盘后分钟校准

盘中 Hot Minute 已经存在。

收盘后正式 `minute_history` 到达：

```text
Hot Minute
+
minute_history
↓
Resolution
↓
Validate
↓
Canonical Minute final
```

同一 `(instrument_id, bar_time, interval_minutes, adjustment)` 最终只能保留一条正式记录。

---

---

## 场景 15：分钟统一查询

当前时间：

```text
2026-09-14 10:15
```

请求：

```text
600519
2026-09-11 ~ 2026-09-14
1m
```

系统内部：

```text
2026-09-11 ~ 2026-09-13
→ Canonical Parquet

2026-09-14
→ SQLite Hot Minute
```

统一按：

```text
(instrument_id, bar_time, interval_minutes, adjustment)
```

合并排序。

接口返回连续分钟序列，上层无需知道哪些记录来自 Hot、哪些来自 Canonical。

---

## 场景 16：Watchlist吞吐量保护

当前Provider按配置只能在50秒内处理200只证券，请求加入第201只时：

```text
重新计算 estimated_cycle_seconds
↓
超过 cycle_deadline_seconds
↓
拒绝扩容并告警
```

不能接受请求后把一分钟数据延迟成数分钟数据。

---

## 场景 17：历史接口静默截断

请求一年1分钟数据，接口HTTP成功但恰好返回 `max_rows_per_request`：

```text
检查首尾时间和Expected Window
↓
标记 partial / truncated
↓
能够分页则继续拆分，不能分页则切换来源
```

不能因为HTTP成功而把历史窗口标记 complete。

---

## 场景 18：证券代码迁移

同一证券从旧代码迁移到新代码：

```text
security_symbol_history
↓
旧代码与新代码映射到同一个 instrument_id
↓
历史数据按有效期拼接并检查重叠冲突
```

不能把代码字符串直接当作永久证券身份。

---

## 场景 19：空返回不等于完成

Provider暂时返回空数组：

```text
temporary_empty
```

只有交易状态或多源证据确认后才转为：

```text
confirmed_no_data / no_trade
```

`temporary_empty` 必须保留在 Missing Set 中等待重试或换源。

---

## 场景 20：Canonical 5分钟重采样

某证券已经存在完整、confirmed的五根1分钟K：

```text
Canonical 1m
↓
按市场Session边界重采样
↓
Canonical 5m
```

Provider原生5分钟只用于校验；如果重采样结果与原生5分钟超过阈值，写 `conflict_log`。

---

## 场景 21：Raw 写完后断电

某个采集尝试已经完成 Raw 写入并记录 hash，但尚未 Normalize：

```text
进程重启
↓
校验 Raw hash
↓
从 normalized 前继续
```

不得再次请求 Provider；最终重复执行仍只能发布一个 Canonical 主键。

---

## 场景 22：正式文件已替换但元数据未提交

断电发生在 `os.replace` 之后、DuckDB 事务提交之前：

```text
启动扫描正式文件 manifest
↓
复核 content_hash / row_count / key_range
↓
修复 partition_status 和 collection_attempt
```

不得用旧元数据覆盖已经完整发布的正式文件。

---

## 场景 23：来源被屏蔽与冷却

EastMoney 或 BaoStock 连续出现 HTML、RemoteDisconnected、403/429：

```text
仅冷却失败的 endpoint + market + asset_type
↓
Sina / Tencent 处理 Missing Set
↓
冷却期内不被后续任务反复撞击
```

来源恢复后必须先通过 Capability Probe，不能因一次 HTTP 200 立即恢复主路径。

---

## 场景 24：语义不兼容的切源

1m 主来源失败，但备用源只有 5m 或复权方式不同：

```text
不切换
↓
保留 missing / single_source
↓
告警并等待兼容来源
```

不得把 5m 拆成伪 1m，也不得把复权数据混入不复权 Canonical。

---

## 场景 25：北交所分钟成交量冲突

TDX 与新浪/腾讯校验结果在成交量或成交额上超过 hard threshold：

```text
保留双方 Raw 与 Capability 版本
↓
写 hard_conflict
↓
隔离该证券当日冲突记录
```

其他证券继续发布，分区状态为 `complete_with_conflicts` 或 `partial`，不能预设任一来源恒为正确。


# 58. 最终设计边界

系统只需要真正做好这些事情：

```text
Security Master 定义证券

Trading Calendar 定义交易日期

Provider Capability 描述接口能力

现有 Metadata 的能力验证记录保存实测能力、版本与有效期；不另建能力管理服务

Planner 计算缺口

Collector 采集并换源

CollectionAttempt 记录租约、Raw hash、尝试状态并支撑断电续跑

RealtimeCollector 仅负责Watchlist盘中持续获取行情，并执行吞吐量预算

HotMinuteStore 负责当天分钟实时写入和查询

Minute Query 统一合并 Hot 与 Canonical

Normalizer 统一单位和字段

Validator 判断数据是否有效

Resolution 解决重复和冲突

Storage 管理 Raw / Hot / Canonical 和 Atomic Publish

MinuteReconciliation 管理盘后分钟校准

Metadata 记录数据完整性和 Provider 状态
```

整个系统不追求成为完整量化平台。

第一阶段明确承诺：

```text
日线：股票、ETF、LOF、指数的全量Universe管理与缺口闭环
分钟：仅Watchlist，不承诺全市场分钟覆盖；沪深以秒级入 Hot 为目标，北交所接受约15分钟来源延迟
```

核心目标只有一个：

> 无论某次采集成功、超时、重复、换源还是补采，最终都能明确知道“当前已有多少有效数据、还缺什么、正式数据是什么”，而不是继续堆积无法判断完整性的文件。
