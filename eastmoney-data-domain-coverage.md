# EastMoney 数据域覆盖设计

> 本文说明 EastMoney 在市场数据中心中的完整数据域范围、当前实现状态和后续接入顺序。EastMoney 不是单一接口，也不能因为分红事件适配器已经存在，就认为 EastMoney 整体已经接入。

## 1. 数据域总览

| 数据域 | 目标 Endpoint | 当前状态 | 初始角色 | 说明 |
|---|---|---|---|---|
| 实时行情 | `realtime_quote` | 正式适配器已实现；当前正式探针 RemoteDisconnected | validation_only | 面向证券实时价格、涨跌、成交量、成交额等 |
| 全市场证券列表 | `security_list` | 正式适配器已实现；本次 Live Probe RemoteDisconnected | validation_only | 股票、ETF、LOF、指数及板块清单分开建模 |
| 历史日线 | `daily_history` | 只有临时探针 | 待验证 | 股票、ETF、指数、板块分别验证，不默认前复权 |
| 收盘快照 | `bulk_snapshot` | 只有临时探针/旧研究链路 | 待验证 | 只能形成 provisional 数据，不能代替历史日线 |
| 行业板块 | `industry_board` | 只有临时探针 | 待验证 | 行业列表、行业行情、行业历史独立能力 |
| 概念板块 | `concept_board` | 只有临时探针 | 待验证 | 概念列表、概念行情、概念历史独立能力 |
| 资金流 | `stock_fund_flow` 等 | 个股资金流正式适配器已实现；当前正式探针 RemoteDisconnected | validation_only | 个股、行业、概念、主力资金等按 Endpoint 拆分 |
| 财务数据 | `financial_main` / `financial_statement` | 财务主指标正式适配器已实现；完整报表未实现 | validation_only | 资产负债表、利润表、现金流量表和指标分开定义 |
| 股东户数 | `shareholder_count` | 正式适配器已实现；单股票样本已通过 | validation_only | 按报告期和公告日期记录，不能与实时行情混用 |
| 龙虎榜 | `longhubang` | 未实现正式适配器 | validation_only 候选 | 上榜日期、证券、营业部和买卖金额独立建模 |
| 分红实施 | `dividend_event` | 已有正式适配器 | discovery，当前禁用 | `RPT_SHAREBONUS_DET`，用于缩小前复权历史检查范围 |

## 2. 当前已实现范围

当前正式代码已覆盖：

```text
providers/eastmoney/dividend.py
providers/eastmoney/security_list.py
providers/eastmoney/realtime.py
providers/eastmoney/fund_flow.py
providers/eastmoney/financial.py
providers/eastmoney/shareholder.py
```

它实现的是：

- 批量分红实施查询；
- 除权除息日期过滤；
- 分页参数；
- 分红字段标准化；
- 事件业务键去重；
- Raw 引用和 Metadata 持久化。

仍未实现的 EastMoney 数据域包括：

- EastMoney 历史日线；
- EastMoney ETF/LOF 快照；
- EastMoney 行业和概念板块；
- EastMoney 行业/概念资金流；
- EastMoney 完整财务报表；
- EastMoney 龙虎榜。

当前另有正式的 `security_list` 适配器，用于分页读取股票、ETF、LOF 和指数清单；由于当前网络 Live Probe 返回 `RemoteDisconnected`，它暂不具备正式路由资格。

当前还已有 `realtime.py` 适配器，覆盖单证券行情、批量行情和 5 日分时走势；已按已验证脚本迁移 Session、请求头、Referer、重试退避、Host 降级和业务有效性判断。本次正式复测仍返回 `RemoteDisconnected`，保持 `validation_only`。

当前还已有 `fund_flow.py` 适配器，覆盖个股日级资金流；原始脚本曾取得 20 条数据，当前正式复测受 `push2his` 断连影响，保持 `validation_only`。`financial.py` 和 `shareholder.py` 已覆盖脚本中验证过的财务主指标和股东户数字段，当前单股票探针通过，保持 `validation_only`。

## 3. 接入原则

每个数据域必须独立具备：

```text
正式适配器
+ Provider Contract
+ YAML Endpoint 配置
+ YAML 归一化规则
+ 小流量 Live Probe
+ 字段和单位语义验证
+ 能力覆盖矩阵记录
+ 端到端数据生产测试
```

不能使用以下方式代替正式接入：

- 临时脚本成功运行一次；
- HTTP 返回 200；
- AkShare 或其他封装函数存在；
- 配置文件声明了 Endpoint；
- 某个数据域返回过少量样本。

## 4. 实施优先级

### 第一优先级：市场数据基础能力

1. `security_list`
2. `realtime_quote`
3. `bulk_snapshot`
4. `daily_history`
5. `industry_board`
6. `concept_board`

这些能力服务于证券主数据、行情展示、快照日线和板块基础数据。

### 第二优先级：研究数据能力

1. `fund_flow`
2. `financial_statement`
3. `shareholder_count`
4. `longhubang`

这些能力属于扩展指标，必须使用独立 Dataset Contract，不直接套用日线或快照的数据质量结论。

### 已单独实现的前复权前置能力

```text
dividend_event
```

它不属于普通行情主链路，而是服务于：

```text
分红事件发现
    ↓
前复权历史重叠比较
    ↓
人工历史重建清单
```

## 5. 路由和启用边界

每个数据域单独配置：

```yaml
providers:
  eastmoney:
    enabled: false
    endpoints:
      realtime_quote:
        enabled: false
        role: primary
      daily_history:
        enabled: false
        role: fallback
      dividend_event:
        enabled: false
        role: discovery
```

EastMoney 某个数据域失败时：

- 只影响该 Endpoint；
- 不自动禁用其他 EastMoney 数据域；
- 不自动改变其他 Provider 的能力资格；
- 不把失败域当成空数据成功；
- 不修改已经确认的正式记录。

## 6. 当前结论

当前应准确描述为：

```text
EastMoney 分红事件 Endpoint：已实现，已完成 HTTP 200/空窗口探针，保持 discovery/disabled
EastMoney 其他数据域：已有设计范围和临时研究证据，但尚未形成正式运行时适配器
```

后续实现某个 EastMoney 数据域前，必须先明确该数据域的目标、字段、单位、更新频率、覆盖范围和验收标准；不把多个数据域合并成一个“大 EastMoney Provider”。
