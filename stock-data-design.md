# 证券全量数据采集与管理平台设计方案

## 1. 项目目标

建设一个轻量、可维护的证券数据采集底座，用于自动获取并管理：

- 股票
- ETF
- 指数
- 可转债（后续）
- 日线
- 分钟线
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

第一阶段保持轻量：

```text
Python
+ Parquet
+ DuckDB
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

全市场和固定股票列表使用同一套采集框架。

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
          - index
        historical: false
        supports_symbol_filter: true
        max_symbols_per_request: 100

      daily_history:
        enabled: true
        datasets:
          - daily_bar
        asset_types:
          - stock
          - etf
        historical: true
        supports_symbol_filter: true
        max_symbols_per_request: 1
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
        max_days_per_request: 5
```

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
(symbol, trade_date)
```

分钟：

```text
(symbol, datetime)
```

Snapshot：

```text
(symbol, snapshot_time)
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

updated_at
```

状态第一阶段控制为：

```text
missing
partial
complete
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
symbol

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

Tencent：

```text
成功 2000
```

则：

```text
Missing = 3300
```

EastMoney：

```text
只请求 3300
成功 2500
```

则：

```text
Missing = 800
```

Provider C：

```text
只补 800
```

目标不是“换一个 Provider 重新跑全部”，而是：

> 换 Provider 继续处理当前 Missing Set。

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
禁用该 Provider
↓
切换下一来源
```

下一次 Scheduler 任务再次尝试。

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
(symbol, trade_date)
```

只能保留一个最终结果。

---

# 26. Resolution Policy

建议第一阶段规则：

```text
1. final > provisional

2. daily_history > snapshot

3. 同等级时：
   provider_priority 高的优先

4. 同 Provider 同质量但数据不同：
   最新 fetch_time 优先

5. 明显差异：
   写 conflict_log
```

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
采用较新的 Provider 修订值
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

超过阈值：

```text
记录 conflict_log
```

但不阻塞整个市场发布。

最终值仍按 Resolution Policy 选择。

---

# 29. conflict_log

DuckDB 表建议：

```text
dataset
symbol
trade_date

provider_a
provider_b

field

value_a
value_b

diff_ratio

selected_provider

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
读取现有 Partition
+
新数据
↓
Merge
↓
Validate
↓
写：

2026-09-10.tmp.parquet

↓
完成后 rename

2026-09-10.parquet
```

同一文件系统内使用原子 rename，保证业务层不会读到半写文件。

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
status = complete
```

直接：

```text
No-op
```

不会重新抓全市场。

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

交易日历可以从任一可靠 Provider 获取，并落本地 DuckDB。

---

# 37. 分钟数据

分钟数据独立 Dataset：

```text
minute_bar_1m
minute_bar_5m
```

Provider Capability 需要声明：

```text
frequency
supports_symbol_filter
max_symbols_per_request
max_days_per_request
max_rows
```

例如：

```yaml
minute_history:
  frequencies:
    - 1m
    - 5m

  max_symbols_per_request: 1
  max_days_per_request: 5
```

---

# 38. 分钟任务拆分

请求：

```text
68只股票
一年
1m
```

Planner 自动根据 Provider 限制拆成：

```text
股票A / 5天
股票A / 下5天
...

股票B / 5天
...
```

业务层不处理 API 限制。

---

# 39. 分钟数据存储

第一阶段不做复杂 Bucket。

直接：

```text
minute_bar_1m/
  trade_date=2026-09-11/
    data.parquet
```

全市场一天约：

```text
5000 × 240
≈ 120万行
```

Parquet + DuckDB 足够处理。

只有真实出现：

- Merge 太慢；
- 文件过大；
- 查询性能明显下降；

再引入 Bucket。

---

# 40. 分钟完整性

分钟数据不按每一根 K 线记录 metadata。

按：

```text
symbol + trade_date
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

不要简单永久写死：

```text
expected_rows = 240
```

因为还要考虑：

- 停牌；
- 临时停牌；
- 新股特殊情况；
- 未来交易制度变化。

---

# 41. YAML 配置

Provider 的 timeout、QPS、并发、Retry 必须配置化，并细化到 Endpoint。

例如：

```yaml
providers:

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
```

同一个 Provider 不同 Endpoint 可以有不同限制。

---

# 42. 配置与运行状态分离

YAML 保存：

```text
默认启停
优先级
QPS
并发
timeout
retry
Capability
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

# 43. DuckDB 元数据表

第一阶段建议只保留必要表：

```text
security_master

security_master_history

trading_calendar

partition_status

partition_item

provider_status

conflict_log
```

可选：

```text
collection_task
```

不强制第一阶段实现：

```text
task / run / batch 三级状态
复杂 Raw Recovery
Schema Registry
多版本长期保存
```

---

# 44. provider_status

建议字段：

```text
provider
endpoint

enabled

last_success_at
last_failure_at

consecutive_failures

http_403_count
http_429_count

last_error
```

足够支撑后续管理台。

---

# 45. 推荐目录结构

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
│   │   ├── tencent.py
│   │   ├── eastmoney.py
│   │   └── ...
│
│   ├── security_master.py
│
│   ├── planner.py
│
│   ├── collector.py
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
│   └── canonical/
│
└── metadata/
    └── metadata.duckdb
```

---

# 46. Scheduler

第一阶段使用：

```text
APScheduler
```

或：

```text
systemd timer
```

即可。

例如：

```text
08:00
Security Master 更新

08:30
最近3个交易日日线 reconciliation

16:20
当天日线同步

18:00
分钟数据同步

20:00
缺失 Partition 重试
```

所有时间后续放入 YAML。

---

# 47. Phase 1 实施范围

第一阶段必须完成：

```text
Security Master

Trading Calendar

daily_bar

stock / ETF / index

2~3个 Provider

Provider Capability

Missing Set

Provider Fallback

Normalizer

单位统一

Raw Storage

Canonical Partition

主键去重

Resolution Policy

Conflict Log

临时文件 + Atomic Publish

YAML Provider Config

Partition State
```

---

# 48. Phase 1 暂缓

暂时不实现：

```text
复杂 DAG

Redis

Celery

Kafka

RabbitMQ

完整 Circuit Breaker

Raw Recovery Worker

Task / Run / Batch 三级状态

动态 Bucket

Schema Registry Service

长期 Partition Version

复杂字段级数据仲裁
```

这些属于运行规模扩大后的增强能力。

---

# 49. 验收场景

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

EastMoney：

```text
第一次2000
第二次1000
```

Raw 允许保留。

Canonical：

```text
(symbol, trade_date)
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

# 50. 最终设计边界

系统只需要真正做好这些事情：

```text
Security Master 定义证券

Trading Calendar 定义交易日期

Provider Capability 描述接口能力

Planner 计算缺口

Collector 采集并换源

Normalizer 统一单位和字段

Validator 判断数据是否有效

Resolution 解决重复和冲突

Storage 管理 Partition 和 Atomic Publish

Metadata 记录数据完整性和 Provider 状态
```

整个系统不追求成为完整量化平台。

核心目标只有一个：

> 无论某次采集成功、超时、重复、换源还是补采，最终都能明确知道“当前已有多少有效数据、还缺什么、正式数据是什么”，而不是继续堆积无法判断完整性的文件。

