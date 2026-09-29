# Provider 能力验证与归一化设计

> 本文定义数据来源能力如何声明、采集、验证、留存证据、形成覆盖矩阵，以及如何通过 YAML 描述来源归一化规则。本文是市场数据中心架构设计的配套设计，不替代 Provider 实现、数据集设计和前复权历史重建设计。

> 最小实现约束：本文中的“能力注册”只表示现有 Metadata 中的验证记录和运行时筛选，不建设独立的能力管理服务、后台、API、审批流或新的能力数据库。除非用户另行确认，不新增管理层。

## 1. 目标与边界

市场数据中心不能把“代码里有一个 Provider 类”当成“来源已经可用”。一个来源只有在以下链路完成后，才允许进入对应数据能力的正式路由：

```text
静态能力声明
    ↓
Provider Contract Fixture
    ↓
小流量 Live Probe
    ↓
字段、单位、时间和复权语义验证
    ↓
能力覆盖矩阵
    ↓
YAML 归一化规则
    ↓
端到端数据生产验证
    ↓
路由资格
```

本文覆盖：

- Provider、Endpoint、Capability、Evidence、Health、Routing Role 的概念边界；
- `providers.yaml`、`capabilities.yaml`、`normalization.yaml` 的职责；
- 日线、分钟、快照、公司行动能力的探针方法；
- 字段、单位、时间、复权和空值语义验证；
- 能力覆盖矩阵和路由资格判定；
- Tencent、Sina、TDX、BaoStock、EastMoney、AkShare 的接入边界；
- 能力验证和归一化规则相关的测试与验收标准。

本文不定义：

- 前复权历史重建的候选版本和统一切换细节；
- 管理台页面；
- 投资业务指标和策略逻辑；
- Provider 之外的外部 SDK 具体安装部署方式。

## 2. 核心定义

### 2.1 Provider

Provider 是一个外部数据来源的适配器集合，例如 Tencent、Sina、BaoStock。Provider 只负责调用外部接口、解析来源原始响应并报告失败，不决定备用来源和正式发布。

### 2.2 Endpoint

Endpoint 是 Provider 下一个明确的外部接口或 SDK 能力，例如：

```text
Tencent.forward_history
Tencent.bulk_snapshot
Tencent.native_1m
Sina.full_history
EastMoney.corporate_action
BaoStock.daily_history
```

不同 Endpoint 必须独立配置、独立探针、独立健康状态和独立路由资格。

### 2.3 Capability

Capability 是一个 Endpoint 在明确数据范围内能够提供的数据能力。Capability 的唯一识别不能只使用 Provider 名称，至少由以下维度组成：

```text
provider
endpoint
dataset
market / exchange
asset_type
adjustment
frequency
code_scope
capability_version
```

示例：

```text
tencent.forward_history.daily_bar.XSHG.stock.forward.daily
tencent.forward_history.daily_bar.XSHG.etf.forward.daily
tencent.native_1m.minute_bar_1m.XSHG.stock.none.1m
```

### 2.4 Evidence

Evidence 是一次能力验证产生的事实证据。它描述某次请求的范围、响应和语义，不是静态配置，也不是运行时健康状态。

Evidence 直接持久化到现有 `MetadataStore` 的验证记录中；它不是一个需要单独运营的管理对象。

Evidence 至少包含：

```text
provider
endpoint
capability_version
dataset
market
asset_type
adjustment
frequency
request_scope
request_parameters_hash
response_status
returned_window
row_count
first_key
last_key
field_semantics
unit_semantics
validated_at
expires_at
evidence_hash
eligible_for_selection
failure_class
```

### 2.5 Health

Health 描述来源当前运行状态，例如连续失败、冷却、最近成功和最近失败。Health 不改变静态能力的范围，也不替代 Evidence。

Health 继续使用现有 Provider 状态记录，不新增能力管理服务。

```text
Capability = 能不能提供
Evidence = 最近是否证明过
Health = 现在是否适合请求
```

### 2.6 Routing Role

Routing Role 描述使用策略，不是能力事实：

```text
primary
fallback
validation_only
discovery
disabled
```

一个来源可以已经具备能力，但仍然处于 `validation_only`；一个来源也可以因为网络不稳定而暂时 `disabled`。

## 3. 三类 YAML 的职责

### 3.1 `config/providers.yaml`

保存 Provider 和 Endpoint 的静态运行策略：

- 是否启用；
- Endpoint 名称；
- 支持的数据集；
- 资产类型和市场范围；
- 复权口径；
- 频率；
- 获取模式；
- 最大返回行数；
- 是否支持分页；
- 请求速率和并发；
- Capability Version；
- 初始 Routing Role。

它不保存：

- 最近一次 Probe 结果；
- 运行时失败计数；
- 冷却时间；
- 当前返回行数；
- 动态有效期。

### 3.2 `config/capabilities.yaml`

保存数据能力的路由顺序和质量要求：

```yaml
capabilities:
  daily_bar:
    routes:
      - provider: tencent
        endpoint: forward_history
        priority: 10
        adjustments: [forward]
      - provider: baostock
        endpoint: daily_history
        priority: 20
        adjustments: [none, forward]
        role: validation_only
```

它描述“希望怎样使用能力”，不代表对应能力已经被验证。

### 3.3 `config/normalization.yaml`

保存来源原始字段到统一字段的转换规则，必须包含解释性信息：

- 字段映射；
- 类型转换；
- 单位转换；
- 时间语义；
- 复权口径；
- 空值语义；
- 适用市场和资产；
- 证据来源；
- 规则版本；
- 验证状态。

代码只执行 YAML 规则，不在 Provider 或 Pipeline 中隐藏业务单位判断。

## 4. 能力采集流程

每个 Endpoint 都按四层验证。

### 4.1 第一层：Provider Contract Fixture

不访问网络，使用固定 Raw Fixture 验证：

- 正常响应；
- 空数组或空对象；
- 部分证券返回；
- 重复行和乱序行；
- 字段缺失和类型变化；
- HTML 伪成功；
- 403、429、5xx；
- 非法 JSON；
- 达到最大返回行数；
- 日期、时间和复权字段错误。

### 4.2 第二层：小流量 Live Probe

Probe 使用少量代表证券，不写生产数据目录。样本至少按以下矩阵选择：

| 维度 | 样本 |
|---|---|
| 市场 | XSHG、XSHE、BSE |
| 资产 | stock、ETF、LOF、index |
| 边界 | 新上市、停牌、代码边界、历史较长证券 |
| 日线 | 最近完成交易日、较早历史日、除权前稳定日 |
| 分钟 | 1m、5m、盘中和收盘附近 |
| 快照 | 正常交易、空值字段、批量边界 |

Probe 必须保存原始响应引用或脱敏摘要，不把完整响应默认写入正式数据目录。

### 4.3 第三层：语义验证

语义验证确认：

- 字段到底代表什么；
- 成交量和成交额单位；
- 时间戳时区和开始/结束语义；
- 空值与停牌语义；
- 普通、不复权、前复权、后复权口径；
- 返回窗口是否完整、部分或静默截断；
- 证券类型和市场覆盖范围；
- 重复请求是否稳定。

语义不明确时，能力可以标记为“适配器已实现”，但不得标记为“正式路由可用”。

### 4.4 第四层：端到端数据生产验证

使用已验证的 Provider 接入临时数据目录，验证：

```text
目标集合冻结
    ↓
来源请求
    ↓
Raw 留存
    ↓
YAML 归一化
    ↓
基础校验
    ↓
Missing Set
    ↓
Fallback / Validation
    ↓
Canonical 或 Hot 发布
```

端到端验证必须确认来源口径没有在路由、标准化和发布过程中被改变。

## 5. 能力覆盖矩阵

能力矩阵是验证事实记录，不是静态配置。每行对应一个明确 Capability：

```text
Provider
Endpoint
Dataset
Market
AssetType
Adjustment
Frequency
AcquisitionMode
HistoryStart
HistoryEnd
MaxRows
Pagination
FieldSemantics
UnitSemantics
ProbeStatus
EvidenceExpiresAt
RoutingRole
RoutingEligibility
```

能力状态至少分为：

```text
declared
adapter_implemented
contract_verified
probe_verified
semantics_verified
e2e_verified
eligible
expired
cooling_down
disabled
unsupported
```

不能用一个 Provider 总体状态覆盖所有 Endpoint 和资产类型。

示例：

```text
Tencent.forward_history.stock.XSHG.forward = eligible
Tencent.forward_history.etf.XSHG.forward = eligible
Tencent.forward_history.lof.XSHG.forward = unsupported
Sina.full_history.stock.BSE.none = eligible
BaoStock.daily_history.etf.forward = adapter_implemented
EastMoney.corporate_action.all_market.none = declared
```

## 6. YAML 归一化规则

### 6.1 规则匹配键

一条规则至少按以下条件匹配：

```text
provider
endpoint
exchange
asset_type
frequency
adjustment
code_prefixes
```

匹配结果必须恰好一条：

- 0 条：拒绝标准化，记录配置缺失；
- 1 条：执行规则；
- 多条：拒绝标准化，记录规则冲突。

### 6.2 规则示例

```yaml
rules:
  - provider: tencent
    endpoint: native_1m
    exchange: XSHG
    asset_type: stock
    frequency: 1
    adjustment: none
    field_mapping:
      trade_date: trade_date
      bar_time: bar_time
      open: open
      high: high
      low: low
      close: close
      volume: volume
      amount: amount
    units:
      volume:
        source: lot
        target: share
        multiplier: "100"
        reason: "Tencent 普通股票分钟成交量按手返回"
      amount:
        source: cny
        target: cny
        multiplier: "1"
    time:
      timezone: Asia/Shanghai
      semantics: end_time
    nulls:
      empty_string: null
      dash: null
    evidence:
      status: validated
      references:
        - docs/provider-probes/2026-09-29-tencent-minute.json
      validated_at: 2026-09-29
    version: tencent-minute-stock-v1
    notes: "688/689 代码使用独立的股单位规则，不复用本条规则。"
```

### 6.3 规则禁止事项

- 不在 Provider 代码中按来源名称偷偷乘单位；
- 不在 Pipeline 中根据代码前缀临时改变复权口径；
- 不用 `none` 规则处理 `forward` 数据；
- 不把指数套用股票式复权规则；
- 没有单位证据时不填写猜测倍数；
- 规则失效后不得继续进入正式路由；
- 不允许用一条宽泛规则覆盖语义不同的股票、ETF、LOF 和指数。

## 7. 路由资格

一个 Capability 进入正式路由必须同时满足：

```text
YAML 已声明
+ Provider Endpoint 已实现
+ Contract Fixture 通过
+ Live Probe 通过
+ 语义验证通过
+ 归一化规则唯一且有效
+ Evidence 未过期
+ Health 未冷却
+ 当前目标在市场/资产/复权/频率范围内
```

任一条件不满足时：

- `validation_only` 能力可以继续用于交叉校验；
- `discovery` 能力可以用于发现事件；
- `disabled` 能力不得请求；
- 没有替代来源时，证券保留在 Missing Set；
- 不允许静默改变复权方式或单位。

## 8. Provider 接入边界

### 8.1 Tencent

当前能力拆分为：

```text
daily_history
forward_history
bulk_snapshot
native_1m
```

前复权股票和 ETF 可以独立资格化；LOF 空返回不能推断为支持；指数禁止股票式前复权。

### 8.2 Sina

当前能力拆分为：

```text
full_history
snapshot
native_5m
```

普通历史能力通过不代表前复权能力通过。ETF、LOF、指数和北交所必须分别记录覆盖证据。

### 8.3 TDX

TDX 适配器依赖外部客户端注入。没有真实客户端和 Live Probe 时保持 disabled，调度不得触发。

### 8.4 BaoStock

BaoStock 作为 SDK Provider，第一阶段优先实现历史日线、5m、Security Master 和 Trading Calendar。初始角色为 `validation_only`，不因配置声明自动进入主路由。

### 8.5 EastMoney

EastMoney 的公司行动能力与行情能力独立管理。公司行动优先实现批量分红实施查询、补偿窗口和业务键去重；行情接口不可用不自动禁用公司行动能力。

### 8.6 AkShare

AkShare 按函数/Endpoint 管理，不把整个 AkShare 标成一个能力。每个函数必须独立确认字段、复权、单位、历史范围和资产覆盖。初始角色为 `validation_only`。

## 9. 证据生命周期

```text
declared
    ↓
adapter_implemented
    ↓
contract_verified
    ↓
probe_verified
    ↓
semantics_verified
    ↓
e2e_verified
    ↓
eligible
```

以下情况会使能力降级或失效：

- Evidence 到期；
- Schema 变化；
- 单位语义变化；
- 连续失败达到阈值；
- HTTP 403/429；
- 返回窗口异常；
- 配置版本变化但没有重新验证；
- 归一化规则版本变化。

能力失效后必须先重新 Probe，不能只重置失败计数恢复路由资格。

上述生命周期只是运行时筛选规则，不引入独立的能力审批、管理后台或状态工作流。

## 10. 测试与验收

每个正式 Endpoint 至少需要：

```text
Provider Contract Fixture
配置解析测试
归一化规则唯一匹配测试
Live Probe 或明确 disabled 证据
语义验证报告
端到端 Missing Set 测试
Raw / Canonical / Metadata 可追溯测试
```

与本设计对应的验收重点：

- 未实现 Provider 不得因 YAML 声明进入路由；
- Evidence 过期不得进入路由；
- 资产类型超出覆盖范围时保持 Missing；
- 归一化规则缺失或多匹配时拒绝处理；
- 不同复权口径不能静默切换；
- 单位转换必须与 YAML 和证据一致；
- Provider 失败不能改变能力覆盖范围；
- 备用来源不得覆盖已确认的前来源有效记录。

## 11. 当前实施差距

当前仓库已经具备：

- Provider 按来源拆分；
- `providers.yaml`、`capabilities.yaml`、`normalization.yaml` 基础加载；
- Tencent/Sina/TDX 部分正式适配器；
- Provider Contract 和小流量探针证据；
- Tencent QFQ 股票/ETF 原型链路。

尚未完成：

- BaoStock 正式 SDK Provider；
- EastMoney 公司行动 Provider；
- AkShare 函数级 Provider；
- 全量能力覆盖矩阵自动生成；
- Probe Evidence 对所有生产路由的动态资格绑定；
- 所有来源的完整 YAML 字段映射、单位和时间语义规则；
- 公司行动触发的前复权历史变化确认；
- 人工历史重建和候选版本统一切换。
