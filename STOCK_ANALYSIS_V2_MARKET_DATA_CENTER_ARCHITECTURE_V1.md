# 股票分析工具 V2：市场数据中心架构设计 V1.3

> 目标：把现有 `stock_data_manage` 收敛为一个简单、可靠、可维护的 A 股市场数据中心，为股票分析工具 V2 提供稳定的数据产品。
>
> V1.3 重点收敛任务与数据流：任务按最终数据分区定义；证券主数据直接读取本地已发布数据；每个来源抓取后立即执行来源专属标准化和校验，再计算缺失证券并切换下一来源；股票、ETF 和 LOF 日线目标口径统一为前复权；支持分红事件批量发现、人工历史重建、全量重做与断点重做；任务成功后归档来源证据，正式数据通过原子发布进入 `data/canonical`。
>
> 核心术语、目录职责和逐来源数据变化以 [市场数据中心术语与数据流程规范](stock-data-terminology-and-data-flow.md) 为准。
>
> 前复权历史变化、分红事件发现和多日期统一切换以 [前复权日线与历史重建设计](stock-data-design-qfq-history-rebuild.md) 为准。当前代码尚未完整实现这条链路。
>
> Provider 能力声明、能力探针、证据生命周期、覆盖矩阵和 YAML 归一化规则以 [Provider 能力验证与归一化设计](provider-capability-verification-and-normalization.md) 为准。

> 2026-10-09明确目录边界：`provider_validation/` 只承担正式实现前的来源可行性验证，包含当时的接口原始返回。进入业务开发后，代码、实现测试和说明分别位于 `src/`、`tests/`、`docs/`；业务响应、联调及验收新产物使用 `data/` 或 `tmp/` 下的隔离运行布局。已有验证证据只读引用。raw按“来源＋接口＋数据日期＋请求范围”每天唯一，正常、失败、重做和转正遵循 [任务流程](docs/pipeline/collection-tasks.md) 与 [存储说明](docs/storage/README.md)。这些按日路径和入口校正仍待代码实现；总体设计中的多来源、历史重建及管理能力不能据此视为已经上线。

## 1. 产品定位

市场数据中心的职责只有一句话：

> 把外部不稳定、格式不一致、质量参差的数据源，整理成稳定、统一、可追溯、可重跑的数据产品。

投资业务不直接接触腾讯、AkShare、BaoStock、pytdx 等来源，也不直接读取 Parquet 文件，只通过统一数据接口访问：

```python
market.daily_bars(...)
market.realtime_quotes(...)
market.fundamentals(...)
market.market_snapshot(...)
```

市场数据中心负责：数据源管理、能力路由、失败降级、字段与单位标准化、质量检查、原始数据留存、缺失补数、重跑、版本、发布、查询、运行状态和审计。

---

## 2. 产品总体架构

```text
股票分析工具 V2
│
├── 投资工作台
│   ├── 市场
│   ├── 股票筛选
│   ├── 观察池
│   ├── 研究分析
│   ├── 模拟验证
│   ├── 投资组合
│   └── 投资复盘
│
└── 市场数据中心
    ├── 数据总览
    ├── 数据源管理
    ├── 数据能力与路由
    ├── 数据集管理
    ├── 数据质量
    ├── 任务运行
    └── 数据修复
```

### 2.1 投资工作台负责

- 股票搜索、筛选、观察
- 研究分析、策略、模拟
- 投资组合、收益分析、复盘

### 2.2 市场数据中心负责

- 外部数据接入
- 多来源管理
- 数据标准化
- 数据质量与修复
- 数据持久化与版本
- 数据发布
- 对外数据访问

### 2.3 明确禁止的跨层行为

投资业务禁止：

```python
akshare.stock_zh_a_hist(...)
requests.get("腾讯接口")
pd.read_parquet("/data/...")
```

投资业务只能通过统一市场数据服务读取数据。

---

## 3. 市场数据中心总体结构

```text
本地证券主数据
    ↓
冻结本次目标证券集合
    ↓
按能力路由选择第一个来源
    ↓
抓取并保存该来源原始响应
    ↓
执行该来源专属标准化和基础校验
    ↓
有效数据加入当前结果
    ↓
重新计算缺失证券
    ↓
仍有缺失？── 是 ──→ 切换下一来源补缺
    │
    否
    ↓
多来源标准数据按路由优先级合并和去重
    ↓
生成唯一候选数据
    ↓
完整性和质量检查
    ↓
原子发布正式数据
    ↓
Python 接口 / HTTP 接口
    ↓
投资工作台
```

其中，缺失证券只能根据已经完成标准化并通过基础校验的数据计算。来源接口返回过某个证券，不代表该证券已经有效完成。

---

## 4. 内部模块划分

第一版拆成 8 个模块：

1. 配置中心
2. 数据源适配器
3. 能力路由器
4. 标准化处理
5. 数据质量
6. 数据存储
7. 数据生产与发布
8. 数据访问服务

这 8 个模块属于同一个代码库，不做微服务拆分。

---

## 5. 配置中心：YAML 是静态策略的唯一事实源

YAML 只保存“系统应该怎样运行”，运行时状态不写回 YAML。

### 5.1 数据源配置

`config/providers.yaml`

```yaml
providers:
  tencent:
    name: 腾讯
    enabled: true
    capabilities:
      stock_daily: true
      realtime_quote: true
    timeout_seconds: 5
    rate_limit:
      requests_per_second: 5
    circuit_breaker:
      failure_threshold: 5
      recovery_seconds: 60

  baostock:
    name: BaoStock
    enabled: true
    capabilities:
      stock_daily: true
      stock_basic: true
      trade_calendar: true

  akshare:
    name: AkShare
    enabled: true
    capabilities:
      stock_daily: true
      stock_basic: true
      fundamentals: true
```

### 5.2 数据能力配置

`config/capabilities.yaml`

```yaml
capabilities:
  stock_daily:
    name: 股票日线
    price_adjustment: qfq
    routes:
      - provider: tencent
        priority: 10
        acquisition: full
      - provider: baostock
        priority: 20
        acquisition: symbol
      - provider: akshare
        priority: 30
        acquisition: symbol
    quality:
      required_fields:
        - symbol
        - trade_date
        - open
        - high
        - low
        - close
        - volume

  realtime_quote:
    name: 实时行情
    routes:
      - provider: pytdx
        priority: 10
      - provider: sina
        priority: 20
      - provider: tencent
        priority: 30
    quality:
      max_staleness_seconds: 15
```

### 5.3 数据集配置

`config/datasets.yaml`

```yaml
datasets:
  instruments:
    name: 证券主数据
    acquisition:
      mode: full
    publish:
      mode: replace

  stock_daily:
    name: 股票日线
    universe:
      source: instruments
      asset_type: stock
    grain:
      - symbol
      - trade_date
    primary_key:
      - symbol
      - trade_date
    partition:
      - trade_date
    acquisition:
      mode: routed
    publish:
      mode: atomic_replace
      max_missing_ratio: 0.01
      max_missing_count: 50
    history_rebuild:
      enabled: false
      require_manual_approval: true
    dividend_event:
      provider: eastmoney
      report_name: RPT_SHAREBONUS_DET
      lookback_calendar_days: 5
    storage:
      type: parquet
```

以上是目标配置结构。当前运行配置在前复权来源探针、历史重建和统一切换实现完成前，不得提前改成前复权，避免配置与实际数据口径不一致。

### 5.4 配置原则

```text
YAML   = 我希望系统怎么运行
SQLite = 系统现在实际运行得怎么样
Web    = 让人看得懂并能操作
```

README、能力矩阵、管理页面都应从 YAML 和运行时数据生成，避免多处维护造成漂移。

### 5.5 能力事实与运行策略分离

Provider、Endpoint、Capability、Evidence、Health 和 Routing Role 必须分开管理：

```text
Capability = 来源理论上能提供什么
Evidence = 最近是否用探针证明过
Health = 当前是否健康、是否冷却
Routing Role = 当前希望如何使用
```

适配器存在、配置已声明或单次 HTTP 成功，都不能单独证明能力具备正式路由资格。能力必须按数据集、市场、资产类型、复权方式和频率细分。详细的能力采集、证据和归一化规则见 [Provider 能力验证与归一化设计](provider-capability-verification-and-normalization.md)。

---

## 6. 数据源适配器

数据源适配器只做三件事：

1. 调用外部接口
2. 返回原始结果
3. 报告失败原因

数据源本身不决定备用源、不决定发布、不包含投资逻辑。

Provider 适配器只返回来源原始语义和结构化失败信息。字段、单位、时间和复权转换由 YAML 归一化规则驱动，不能在 Pipeline 中通过来源名称写死转换逻辑。

统一接口示例：

```python
class MarketDataProvider:
    def fetch_daily_bars(self, trade_date, symbols=None):
        raise NotImplementedError

    def fetch_realtime_quotes(self, symbols):
        raise NotImplementedError
```

实现：

```python
class TencentProvider(MarketDataProvider): ...
class AkShareProvider(MarketDataProvider): ...
class BaoStockProvider(MarketDataProvider): ...
```

---

## 7. 按“数据能力”管理来源

系统不定义：

```text
主数据源 = 腾讯
备用数据源 = AkShare
```

而定义：

```text
股票日线：
腾讯 → BaoStock → AkShare

实时行情：
pytdx → 新浪 → 腾讯

财务数据：
AkShare → 其他来源

交易日历：
BaoStock → AkShare
```

同一个数据源在不同能力上的可靠性、覆盖度和实时性可能完全不同。

运行时健康状态也按：

```text
数据能力 + 数据源
```

独立计算。

---

## 8. 能力路由器

能力路由器负责按固定路由顺序获取数据，但具体是否真正发起请求由当前数据目标和该来源的采集模式决定。

路由器只能选择同时满足静态配置、适配器实现、有效 Probe Evidence、未过期、未冷却、覆盖范围匹配且存在唯一归一化规则的 Capability。未实现或未验证的来源可以保留在能力矩阵中，但不得自动进入正式路由。

第一版只保留两种与股票日线相关的采集模式：

```text
FULL
全量获取；适合腾讯这类一次返回全市场快照的来源。

SYMBOL
按证券获取；适合 BaoStock、AkShare 这类可针对缺失证券继续补数的来源。
```

路由器的核心输入不是“上一次失败到了哪个来源”，而是：

```text
当前目标证券集合
-
当前工作区已经拥有有效来源标准数据的证券集合
=
当前缺失证券集合
```

断点重做时仍然从正常路由第一位开始扫描：

```text
Tencent（全量获取）
    ↓
已有本次数据构建的腾讯原始响应和标准数据 → 直接复用
没有 → 真正全量请求
    ↓
腾讯专属标准化和校验
    ↓
重新计算缺失证券
    ↓
BaoStock（按证券获取）
    ↓
当前缺失证券中，本地 BaoStock 标准数据已经有的直接复用
仍缺的直接请求
    ↓
BaoStock 专属标准化和校验
    ↓
重新计算缺失证券
    ↓
AkShare（按证券获取）
    ↓
仍缺什么就请求什么
```

第一版不维护复杂的证券逐阶段状态。断点重做只关心“当前已经有什么有效来源标准数据”和“当前还缺什么”。

伪代码：

```python
def fill_missing(build, target_symbols, routes):
    missing = target_symbols - build.current_symbols()

    for route in routes:
        if not missing:
            break

        if route.acquisition == "full":
            if not build.has_source_file(route.provider):
                data = route.provider.fetch_full()
                build.save_source(route.provider, data)

        elif route.acquisition == "symbol":
            existing = build.source_symbols(route.provider)
            request_symbols = missing - existing
            if request_symbols:
                data = route.provider.fetch_symbols(request_symbols)
                build.append_source(route.provider, data)

        missing = target_symbols - build.current_symbols()

    return missing
```

这里的“复用”仅发生在当前数据构建的任务工作区中；全量重做会创建新的数据构建，不复用旧任务的数据。

---

## 9. 数据源运行状态

运行状态存 SQLite。

核心表：`provider_runtime_state`

```text
provider
capability
status
health_score
last_success_at
last_failure_at
consecutive_failures
success_rate_1h
success_rate_1d
avg_latency_ms
circuit_state
updated_at
```

状态只保留三种：

```text
正常 NORMAL
降级 DEGRADED
阻断 BLOCKED
```

- 正常：参与正常路由
- 降级：可降低优先级或临时跳过
- 阻断：Schema 漂移、核心字段缺失或长期不可用，恢复验证后才能重新加入

---

## 10. 标准数据模型

外部来源最终必须进入统一内部模型。

股票日线示例：

```text
symbol
trade_date
open
high
low
close
pre_close
volume
amount
pct_change
```

必须统一：

- 股票代码
- 日期格式
- 字段名称
- 数据类型
- 成交量单位
- 成交额单位
- 百分比表达
- 空值语义
- 市场标识

如果不同来源单位不同，例如“手”和“股”，内部统一使用一种标准单位。任何不确定单位不能静默猜测。

---

## 11. 数据质量

质量检查分两层。

### 11.1 单来源质量

判断某个来源本次返回是否可用：

- 是否为空
- 必要字段是否存在
- 数据类型是否正确
- 日期是否正确
- 证券代码是否合法
- 数据是否过期
- 价格和数量是否合理
- 单位是否可确定
- 是否返回验证码或风控页面

### 11.2 数据集质量

对最终候选数据检查：

- 覆盖率
- 缺失证券
- 主键唯一性
- 重复数据
- 日期连续性
- 价格异常
- 成交量异常
- 跨来源冲突
- 与历史数据的异常突变

只有通过数据集质量检查，并满足该数据集发布门槛的数据才允许正式发布。股票日线第一版允许最多缺失 1%，且最多缺失 50 只；两个条件必须同时满足。门槛来自 `config/datasets.yaml`，不能写死在采集代码中。

---

## 12. 数据生命周期

市场数据中心不采用一个面向所有场景的复杂 Workflow。第一版按“最终数据目标 + 数据获取形态”驱动。

一个任务目标定义为：

```text
数据集 + 分区
```

例如：

```text
股票日线 / 2026-09-28
```

这个任务的最终目标只有一个：生成一份满足配置发布门槛、质量检查通过、可以正式发布的 `2026-09-28.parquet`。

### 12.1 数据获取形态

不同数据集根据外部接口本身的形态选择最自然的更新策略：

| 获取形态 | 适用数据 | 更新策略 |
|---|---|---|
| 全量 `FULL` | 证券主数据、交易日历、全市场快照 | 临时拉全量，校验后整体替换 |
| 按证券 `SYMBOL` | 股票历史、缺失证券补数 | 当前缺什么就请求什么 |
| 增量 `APPEND` | 公告、新闻等事件数据 | 按时间窗口追加并按主键去重 |
| 实时 `REALTIME` | 实时行情 | 同步路由、短缓存，不走历史发布流水线 |

不要求所有数据集使用同一套更新算法。

### 12.2 证券主数据是本地基础数据

股票日线任务启动时不触发外部证券主数据采集，而是直接读取本地已发布的 `instruments`。

```text
本地 instruments
      ↓
根据 asset_type / list_date / delist_date 计算目标证券
      ↓
冻结为本次数据构建的目标证券集合
      ↓
开始股票日线采集
```

证券主数据自身是一个独立低频任务：

```text
BaoStock（主）/ AkShare（备）
        ↓
证券全量基础信息
        ↓
任务工作区
        ↓
校验
        ↓
整体更新本地 instruments
```

证券主数据同步失败不会阻塞股票日线任务，后者继续使用当前已发布的本地证券主数据。首次部署、`instruments` 为空时才必须执行初始化同步。

### 12.3 数据构建与执行记录

任务按最终数据目标创建一个数据构建（Build）：

```text
数据构建 #1001
数据集：stock_daily
分区：2026-09-28
目标证券：5421
```

同一个数据构建可以产生多次执行记录（Attempt）：

```text
数据构建 #1001
├── 第一次执行  失败
├── 第二次执行  失败
└── 第三次执行  成功
```

执行记录只是运行历史；目标证券、来源文件、缺失证券和候选数据都属于数据构建工作区。

### 12.4 任务工作区

数据构建开始时创建：

```text
data/
├── raw/_tmp/<数据日期>/<任务ID>/<单元哈希>/
├── raw/<来源>/<接口>/<数据日期>/scope-<范围哈希>/
├── task_workspace/<数据集>/<范围>/<运行ID>/sources/<来源>/<输入ID>/
│   ├── raw_refs.json
│   ├── parsed/
│   └── normalized.parquet
├── task_workspace/_tasks/<任务ID>/{prepared.parquet,coverage.json}
├── task_archive/
├── canonical/<数据集>/<既有业务分区>/
└── metadata/metadata.duckdb
```

其中：

- 原响应只保存在raw及既有稳定证据归档，来源工作区保存引用、解析和映射结果。
- 本次冻结目标、单元状态、缺失原因及提交记录由现有任务定义和元数据库维护，用户不维护来源文件清单。
- 来源工作区的 `normalized.parquet` 是该来源转换及基础校验后的候选数据。
- 任务工作区的 `prepared.parquet`、`coverage.json` 及正式发布清单关联最终构建、检查、数量和来源引用；最终覆盖汇总在发布前生成，中间状态供重做使用。
- 上述按日raw布局是目标，现存暂存和无日期转正路径仍需修正；其他最终数据仍使用既有业务分区，不在本次扩展证券历史服务或通用工作流。

### 12.5 全量重做与断点重做

提供全量、断点和指定证券三种操作，均限定到数据日期及任务冻结范围。来源不支持单证券请求时，指定证券重做明确拒绝；详细现状与目标以任务流程为准。

#### 全量重做

含义：不信任当前数据构建的采集结果，从源头重新生产。

```text
清空目标日期、当前任务的活动暂存及构建结果
    ↓
从第一来源真正重新抓取
    ↓
来源专属标准化和校验
    ↓
计算缺失证券
    ↓
备用来源补数
    ↓
合并、校验、发布
```

成功后替换该日期及范围的结果；失败时保留此前正式数据，其他日期保持。既有审计归档保存必要原字节及引用，不让用户选择多份活动版本。

#### 断点重做

含义：信任当前数据构建已有的来源原始响应和来源标准数据，只补齐当前仍缺失的数据。

```text
沿用同一个数据构建和任务工作区
    ↓
根据目标证券和有效来源标准数据重新计算缺失证券
    ↓
从正常路由第一位重新扫描
    ↓
全量来源：当前任务已有原始响应和标准数据则复用，否则全量拉取并转换
    ↓
按证券来源：当前缺什么、其现有标准数据里又没有什么，就直接拉什么并转换
    ↓
重新计算缺失证券
    ↓
直到补齐或所有来源扫描结束
```

断点重做不关心上次为什么失败，也不记录复杂证券状态。

### 12.6 缺失证券与补数

例如：

```text
目标证券：5421
腾讯原始返回：5405
腾讯标准化并校验有效：5398
缺失证券：23
```

继续：

```text
BaoStock 请求 23
    ↓
标准化并校验后补回 21
    ↓
缺失证券 = 2
    ↓
AkShare 请求 2
```

如果所有路由走完仍然缺失，`missing_symbols.json` 记录最终缺失的证券，再由数据集发布门槛判断任务能否发布。来源接口已经返回但标准化或基础校验失败的证券仍属于缺失证券。

股票日线第一版发布条件：

```text
缺失比例 <= 1%
并且
缺失数量 <= 50
```

两个条件同时满足才允许发布；任一条件超限都不发布，并保留已有正式数据。具体数值由 `config/datasets.yaml` 管理，后续管理台修改同一配置，不在代码中设置固定值。

可配置一个简单阈值避免“大面积缺失时逐只补”：

```yaml
repair:
  max_missing_ratio: 0.02
```

缺失比例较小时按证券补；缺失过多时直接判定首轮数据不可用，选择全量重做或改用另一个具备全量能力的来源。

### 12.7 合并与唯一结果

临时来源文件可以重叠，但正式结果不能重叠。

股票日线主键：

```text
symbol + trade_date
```

合并时按固定来源优先级保留先取得的有效数据：

```text
Tencent > BaoStock > AkShare
```

例如 Tencent 已经有效取得 `600519 / 2026-09-28`，该证券会从缺失集合移除，BaoStock 不再请求或接收这条记录；只有 Tencent 缺失时才使用 BaoStock。即使全量接口额外返回已有证券，也只接收当前缺失证券对应的数据。

最终正式文件只保留一条记录，并建议保留轻量来源字段：

```text
source
source_time
```

### 12.8 发布与任务归档

最终候选通过质量检查后才发布：

```text
candidate/stock_daily.parquet
      ↓
质量 PASS
      ↓
原子发布
      ↓
data/canonical/daily_bar/asset_type=stock/trade_date=2026-09-28/
```

来源证据沿用既有 `data/task_archive/_raw_evidence/` 稳定引用。整份任务归档是后续由已发布任务调用既有归档入口的操作，当前不会随发布自动搬迁报告；目标关系如下：

```text
task_workspace/.../task_1001/sources
manifest.json
quality_report.json
        ↓ move
task_archive/stock_daily/2026-09-28/task_1001/
```

临时计算文件的清理沿用明确的任务范围及审计策略；本次未新增发布后自动删除或整体归档能力。

因此：

```text
task_workspace = 正在采集、转换、补缺和检查的任务目录
task_archive = 已成功发布任务的来源证据和报告
canonical = 业务正式读取的数据
```

失败任务工作区、来源原始响应、来源标准数据和任务归档第一版默认不自动清理。后续再增加人工清理和可配置保留策略；清理能力上线前只统计磁盘占用，不删除数据。

### 12.9 目录结构

```text
data/
├── raw/_tmp/<数据日期>/<任务>/<单元>/
├── raw/<来源>/<接口>/<数据日期>/scope-<范围>/
├── task_workspace/                          # 来源候选、任务及运行报告
├── task_archive/                            # 来源稳定证据及执行追溯
├── canonical/                              # 既有数据集业务分区
└── metadata/metadata.duckdb
```

### 12.10 股票日线完整模拟

第一次执行：

```text
数据构建 #1001 / 第一次执行
目标 5421
    ↓
Tencent 全量原始返回 5405
    ↓
标准化并校验有效 5398
    ↓
缺失 23
    ↓
BaoStock 按证券请求 23 → 标准化并校验后补 21
    ↓
缺失 2
    ↓
AkShare 按证券请求 2 → 拉取失败
    ↓
任务失败
missing_symbols.json = [A, B]
```

此时 `task_workspace/.../task_1001` 保留。

选择“断点重做”：

```text
数据构建 #1001 / 第二次执行
    ↓
根据已有有效来源标准数据重新计算缺失 = [A, B]
    ↓
Tencent 全量来源
已有本次任务的原始响应和标准数据 → 复用
    ↓
BaoStock 按证券来源
当前标准数据无 A、B → 直接再拉 A、B 并执行标准化和校验
    ↓
假设补回 A
    ↓
缺失 = [B]
    ↓
AkShare(SYMBOL) 拉 B
    ↓
成功
    ↓
合并 + 主键去重
    ↓
5421 / 5421
    ↓
质量 PASS
    ↓
原子发布
    ↓
来源证据、任务说明和质量报告归档到 task_archive
    ↓
任务工作区清理
```

选择“全量重做”则清空目标日期及当前任务的活动暂存和构建结果，从源头真正重新采集；成功后替换当天范围，不复用旧候选填补新缺失，也不覆盖其他日期。上述多来源场景是总体设计示例，当前证券与日线任务的实际实现范围及未接通能力以模块流程文档为准。

### 12.11 第一版任务驱动

任务调度器只负责创建“最终数据目标”的 Build，不负责数据逻辑。

```text
Scheduler
    ↓
创建 stock_daily / 2026-09-28 Build
    ↓
Worker
    ↓
按照数据集配置执行 FULL / SYMBOL 路由
    ↓
Publish
```

任务运行中心第一版只需要支持：

- 定时创建 Build
- Worker 执行
- 全量重做
- 断点重做
- 有限网络重试
- 任务运行记录
- 失败任务工作区查看与人工清理入口

第一版“清理”只指人工确认后的清理入口和磁盘占用展示，默认不自动删除任务数据。

### 12.12 前复权历史变化与重建

股票、ETF 和 LOF 的正式日线统一使用前复权，指数保存指数发布方点位。由于前复权历史会在除权除息后变化，系统增加独立于普通日线补数的历史重建流程。

```text
东方财富 RPT_SHAREBONUS_DET
    ↓
按最近若干自然日批量查询全市场除权事件
    ↓
得到可能变化的证券集合
    ↓
从计划使用的前复权历史来源获取重叠历史
    ↓
同来源历史值确实变化？
    ├── 否：稍后重查
    └── 是：进入人工历史重建清单
            ↓
       人工批准并开启历史覆盖
            ↓
       整只证券重新获取前复权历史
            ↓
       按日期生成全部候选分区
            ↓
       全部完成后统一切换正式版本
```

第一版不自动执行历史重建，不依赖独立复权因子，也不逐只查询全市场分红。分红事件接口与东方财富行情接口分开配置和健康管理。

前复权历史主来源需要通过真实探针确定，不能在架构文档中预先写死。历史重建优先整段使用同一个来源；第一来源不完整时，放弃本次整段结果，再由下一来源重新获取整段历史。

历史重建可能影响多个日期分区，禁止逐日直接覆盖。所有候选分区准备并校验完成后，通过版本指针一次切换，保证业务不会读取到新旧历史混合结果。

第一版不做通用工作流引擎、复杂步骤状态机或证券级错误状态系统。

---

## 13. 实时行情流程

实时行情不走完整历史发布流程。

```text
股票详情页
   ↓
请求实时行情
   ↓
能力路由器
   ↓
pytdx
   ↓
质量检查
   ↓
成功？
┌────┴────┐
│         │
是        否
│         │
↓         ↓
返回      新浪
          ↓
        失败？
          ↓
         腾讯
```

实时重点检查：

- 新鲜度
- 必要字段
- 价格
- 时间戳
- 停牌状态
- 响应延迟

可增加 2～5 秒短时缓存，降低外部接口压力。

---

## 14. 管理后台

数据管理下设置以下入口：

```text
数据管理
│
├── 数据总览
├── 数据源
├── 数据能力
├── 数据集
├── 数据质量
├── 任务运行
└── 数据修复
```

### 14.1 数据总览

展示：

```text
股票日线       正常
实时行情       正常
财务数据       部分异常
交易日历       正常

今日任务
成功：18
失败：1
补数：2
```

### 14.2 数据源

| 数据源 | 状态 | 健康度 | 平均延迟 | 今日成功率 | 最近错误 |
|---|---|---:|---:|---:|---|
| 腾讯 | 正常 | 97 | 183ms | 99.4% | - |
| BaoStock | 正常 | 94 | 310ms | 98.7% | - |
| AkShare | 降级 | 72 | 960ms | 91.2% | 限流 |
| pytdx | 正常 | 98 | 31ms | 99.9% | - |

### 14.3 数据能力

例如“股票日线”：

```text
腾讯
 ↓
BaoStock
 ↓
AkShare

今日：
首选源命中率：98.4%
Fallback：1.6%
最终失败：0%
```

### 14.4 数据集

```text
股票日线
覆盖：2010-01-01 → 2026-09-28
当前正式版本：v181
最新候选版本：v182
最新候选状态：FAILED
```

### 14.5 数据修复

```text
2026-09-27 股票日线
缺失：23

操作：
- 重新采集缺失证券
- 指定 BaoStock 补数
- 指定 AkShare 补数
- 从 Raw 重新构建
- 查看质量报告
```

---

## 15. 路由追踪与可解释性

任意一次数据访问都应回答：

> 为什么最后用了这个来源？

历史示例：

```text
股票日线 / 2026-09-28

腾讯
 ↓
成功
 ↓
覆盖率 99.57%
 ↓
质量未通过

BaoStock
 ↓
补 21

AkShare
 ↓
补 2

最终：5421 / 5421
PASS
```

实时示例：

```text
600519 / 11:31:23

pytdx 主节点
 ↓
timeout

pytdx 热备
 ↓
成功

最终来源：pytdx / hot-backup-02
延迟：42ms
```

---

## 16. 第一版核心元数据表

第一版元数据表继续保持简单，围绕“数据目标、Build、Attempt、发布”组织。

建议核心表：

1. `instruments`：本地已发布证券主数据
2. `data_build`：一次最终数据构建，例如 `stock_daily / 2026-09-28`
3. `build_attempt`：某个 Build 的一次实际执行/重做
4. `provider_attempt`：一次来源调用的运行日志
5. `publication`：正式发布记录
6. `provider_runtime_state`：来源运行健康状态（若首期需要）

`instruments` 建议字段：

```text
symbol
name
exchange
asset_type
list_date
delist_date
status
updated_at
```

`data_build` 建议字段：

```text
id
dataset
partition
status
target_count
missing_symbols
workspace_path
created_at
published_at
```

`build_attempt` 建议字段：

```text
id
build_id
mode            # normal / resume / full_rebuild
status
started_at
finished_at
```

`provider_attempt` 只承担运行审计，不进入复杂恢复状态机：

```text
id
build_attempt_id
provider
requested_count
returned_count
started_at
finished_at
status
```

`publication`：

```text
id
dataset
partition
build_id
file_path
published_at
is_current
```

第一版不建立“证券 × Provider × 日期”的长期状态表。断点重做直接依据 Build 当前工作区和 Missing Set 重新计算。

---

## 17. 数据访问服务

### Python API

```python
market.daily_bars(
    symbols=["600519"],
    start="2026-01-01",
    end="2026-09-28",
)
```

```python
market.realtime_quotes(
    symbols=["600519", "000001"],
)
```

### HTTP API

```text
GET /api/market/daily-bars
GET /api/market/quotes
GET /api/market/instruments
GET /api/market/calendar
```

历史接口只读取 Published 数据。实时接口通过实时路由器获取当前合格数据。

---

## 18. 新增数据源流程

例如增加 iTick，只做四件事。

### 1. 实现 Provider

```python
class ITickProvider(MarketDataProvider):
    def fetch_daily_bars(...): ...
    def fetch_realtime_quotes(...): ...
```

### 2. 声明能力

```yaml
itick:
  enabled: true
  capabilities:
    stock_daily: true
    realtime_quote: true
```

### 3. 能力验收

验证：

- 字段
- 单位
- 覆盖率
- 延迟
- 新鲜度
- 历史范围
- 异常响应
- 限频行为

### 4. 加入路由

```yaml
stock_daily:
  routes:
    - provider: tencent
      priority: 10
    - provider: itick
      priority: 20
    - provider: baostock
      priority: 30
```

投资业务代码修改应为 0。

---

## 19. 首期不做什么

为了控制复杂度，第一版明确不做：

- 微服务拆分
- Kafka
- Spark
- OpenMetadata
- ArcticDB
- 完整 OpenBB 集成
- 通用插件系统
- 复杂 Workflow Engine
- 多租户
- 分布式调度
- 全市场全品种一次性重构

---

## 20. 首期技术栈

```text
Python

Pydantic
    标准数据模型

YAML
    静态配置

SQLite
    元数据 / 运行状态

Parquet
    正式历史数据

DuckDB
    分析查询

本地文件系统
    任务工作区 / 任务归档

FastAPI
    HTTP 数据服务

现有 Worker / Scheduler
    任务执行
```

Dagster 等任务框架在任务数量、依赖和回填复杂度明显增长后再单独评估，不作为首期必须依赖。

---

## 21. 代码目录建议

```text
stock_data_manage/

├── config/
│   ├── providers.yaml
│   ├── capabilities.yaml
│   └── datasets.yaml
│
├── domain/
│   ├── instrument.py
│   ├── daily_bar.py
│   ├── quote.py
│   ├── data_build.py
│   └── publication.py
│
├── providers/
│   ├── tencent/
│   ├── akshare/
│   ├── baostock/
│   └── pytdx/
│
├── routing/
│   └── router.py
│
├── quality/
│   ├── schema.py
│   ├── coverage.py
│   ├── freshness.py
│   └── rules.py
│
├── pipeline/
│   ├── build.py
│   ├── merge.py
│   ├── validate.py
│   └── publish.py
│
├── storage/
│   ├── workspace.py      # 任务工作区
│   ├── task_archive.py   # 已成功发布任务的来源证据归档
│   ├── parquet.py
│   └── metadata.py
│
├── service/
│   ├── instruments.py
│   ├── market_data.py
│   └── api.py
│
└── worker/
    ├── scheduler.py
    └── build_worker.py
```

首期优先保持模块职责直接、显式，不额外封装通用工作流框架。

---

## 22. 首期验证范围

第一阶段优先验证三个基础能力。

### 证券主数据

验证：

- BaoStock 全量初始化/同步
- AkShare 备用能力
- 本地 `instruments` 发布与读取
- 上市日期 / 退市日期 / 证券类型
- 股票日线任务直接使用本地证券清单，不触发前置网络同步

### 股票日线

验证：

- 最终任务维度为 `数据集 + 交易日`
- Tencent `FULL` 获取
- BaoStock / AkShare `SYMBOL` 补缺
- Missing Set
- 全量重做
- 断点重做
- 任务工作区复用
- 多来源合并与主键去重
- 质量检查
- 原子发布
- 任务工作区到任务归档的来源证据归档

### 实时行情

验证：

- 多来源固定路由
- 超时后 Fallback
- 基础质量检查
- 新鲜度
- 短时缓存

这三条链稳定后，再扩展交易日历、财务、估值、资金流、指数行情、分钟线和标准技术指标。

---

## 23. 架构验收标准

### 数据源可替换

新增数据源时投资业务代码修改应为 0。

### 数据目标明确

批量历史数据任务必须按：

```text
数据集 + 分区
```

定义最终目标，而不是按 Provider 定义任务。

### 重做语义清楚

必须同时支持：

```text
断点重做：复用当前数据构建的任务工作区，只拉当前缺失数据
全量重做：新建 Build，从第一来源重新完整获取
```

### 正式数据唯一

来源文件和补数文件只存在于 Build 工作区/Raw 归档，业务最终只读取一个正式分区文件。

### 错误数据不污染业务

新的候选数据失败时，已有正式数据保持不变。

### 数据可追溯

正式文件至少能追溯到：

```text
Build
使用过的来源文件
每个来源记录数
质量结果
发布时间
```

无需首期构建复杂血缘 DAG。

### 证券范围稳定

股票日线 Build 开始时从本地 `instruments` 计算并冻结 `target_symbols`；执行过程中证券主数据变化不影响当前 Build。

### 配置单一事实源

数据源能力、获取模式与固定路由以 YAML 为唯一静态事实源。

---

## 24. 推荐实施顺序

### 阶段 1：证券主数据与标准模型

先完成本地 `instruments`，并定义证券、股票日线、实时行情的代码、字段和单位规则。

### 阶段 2：Provider 获取模式

把现有来源收敛到统一 Provider 接口，并明确每项能力属于：

```text
FULL / SYMBOL / APPEND / REALTIME
```

首期股票日线：Tencent 为 `FULL`，BaoStock / AkShare 作为 `SYMBOL` 补数来源。

### 阶段 3：实现 Build 工作区

完成：

```text
创建 Build
→ 冻结 target_symbols
→ 任务工作区
→ Missing Set
→ 合并去重
```

### 阶段 4：实现两种重做

完成：

```text
断点重做
全量重做
```

重点验证断点重做不会重复全量拉取已有 `FULL` 输入，但会按照正常路由重新检查当前 Missing 并对 `SYMBOL` 来源直接补拉。

### 阶段 5：发布与归档

完成：

```text
质量检查
→ 原子发布 datasets
→ 来源证据、任务说明和质量报告从任务工作区移入任务归档
→ 清理 working
```

### 阶段 6：实时行情与管理后台

实现实时路由，以及数据总览、数据源、Build/Attempt、Missing、全量重做和断点重做操作入口。

### 阶段 7：接入股票分析工具 V2

将投资业务中的直接数据源调用替换成统一市场数据接口。

---

## 25. 最终原则

1. **业务与数据源隔离**：投资业务只访问标准数据服务。
2. **按数据能力管理来源**：不是“腾讯是主源”，而是“股票日线的首选来源是腾讯”。
3. **按数据形态选择更新方式**：能全量就全量，能按证券补就只补缺失，不强迫所有数据走同一种流水线。
4. **任务围绕最终数据目标**：批量历史数据按“数据集 + 分区”定义 Build。
5. **正式数据保持唯一和简单**：多来源与补数复杂度只存在于任务工作区和任务归档，业务读取单一正式结果。
6. **两种重做足够**：断点重做复用当前 Build；全量重做从源头重新生产。
7. **保持简单**：第一版不为极端异常引入复杂证券状态、血缘 DAG 或通用 Workflow Engine。

---

## 结论

`stock_data_manage V2` 不应继续成长为一个大而全的数据基础设施平台，而应该成为：

> 一个以 A 股标准数据模型为核心，以多源路由、质量闸门、Missing Set、补数、版本和发布为重点的轻量市场数据中心。

它吸收 pyqauto 一类项目在能力级路由、多源降级、健康状态、熔断、Schema/单位校验和审计方面的成熟思想，同时保留现有项目在 Raw、Missing Set、Parquet 和历史数据管理方面的积累，最终为股票分析工具 V2 提供稳定、统一的数据世界。
