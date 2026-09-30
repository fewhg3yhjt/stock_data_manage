# 项目开发进度

更新时间：2026-09-30

本文是当前项目的进度总览，记录代码、验证和设计落地状态。详细架构以设计文档为准，来源事实以 [Provider 能力验证矩阵](PROVIDER_CAPABILITY_MATRIX.md) 为准。

## 当前阶段

当前处于：

```text
Phase 1 基础链路完成
→ 来源适配器和能力覆盖建设中
→ 逐数据域补齐 YAML、Contract、Probe 和归一化规则
→ 尚未进入正式生产试运行
```

## 已完成基础能力

- Provider 按来源拆分为 Tencent、Sina、TDX、BaoStock、EastMoney、AkShare 适配器目录；
- Provider Contract、HTTP 失败分类和静默截断识别；
- Raw、Hot、Canonical、Metadata 存储；
- Missing Set 和来源补缺；
- Provider 健康、失败计数、冷却和探针验证记录；
- Canonical 幂等、Manifest、文件锁和恢复；
- 日线、分钟、快照和盘后 Reconciliation 基础流程；
- `config/providers.yaml`、`config/capabilities.yaml`；
- `config/datasets/` 数据集标准定义；
- `config/normalization/` 按数据集拆分的归一化规则；
- 项目范围控制、已验证脚本等价迁移和权限规范。

## 来源实现状态

| 来源 | 已实现能力 | 验证状态 | 当前路由角色 |
|---|---|---|---|
| Tencent | 普通日线、前复权日线、快照、1m | 部分 Live Probe 通过 | 候选，未统一开放正式路由 |
| Sina | 普通日线、快照、5m | 部分 Live Probe 通过 | 候选/校验 |
| TDX | 注入式延迟 1m | 无真实客户端 Probe | disabled |
| BaoStock | 历史日线、5m | 日线样本通过，5m 空返回 | validation_only |
| EastMoney | 分红事件、证券列表、实时行情、个股资金流、财务主指标、股东户数 | 分红 HTTP 200/空窗口；证券列表/实时行情/资金流当前网络断连；财务主指标和股东户数单股票样本通过 | disabled/validation_only |
| AkShare | 股票、ETF、LOF、指数历史日线 | 代表样本 Live Probe 通过 | validation_only |
| THS | 行业/概念板块列表和成分 | 列表分页与代表板块成分页 Live Probe 通过；Provider 内部 3 秒间隔；Raw 快照链路通过；未执行全量抓取或 Canonical 发布 | validation_only |

## 当前已完成的 EastMoney 数据域

| 数据域 | 代码状态 | 真实验证状态 |
|---|---|---|
| 分红事件 `dividend_event` | 已实现 | HTTP 200，窗口为空，未获正式资格 |
| 证券列表 `security_list` | 已实现 | 当前网络 `RemoteDisconnected` |
| 实时行情 `realtime_quote` | 已实现单证券、批量和分时适配器 | 原始脚本曾成功；行为等价迁移后当前复测仍 `RemoteDisconnected` |
| 个股资金流 `stock_fund_flow` | 本轮实现 | 原始探针曾成功；正式复测 `push2his` RemoteDisconnected |
| 财务主指标 `financial_main` | 已实现 | 单股票样本返回 8 个报告期 |
| 股东户数 `shareholder_count` | 已实现 | 单股票样本返回 10 个报告期 |

## 当前未实现 EastMoney 数据域

- 历史日线和前复权历史日线；
- 收盘快照正式 Provider；
- 行业板块；
- 概念板块；
- 行业/概念资金流；
- 完整财务报表；
- 龙虎榜。

这些数据域保持独立，不合并成一个“大 EastMoney Provider”。

## 前复权状态

已完成：

- Tencent QFQ 股票适配器；
- Tencent QFQ ETF 适配器；
- `none`/`forward` Canonical 隔离；
- 股票、ETF、LOF Fixture 链路；
- 指数禁止股票式前复权。

未完成：

- 前复权完整历史窗口拆分；
- 北交所和 LOF 前复权来源；
- 分红事件触发的历史变化确认；
- 人工历史重建；
- 多日期候选版本统一切换。

## 验证结果

当前系统测试：

```text
130 passed
```

离线 M1 已覆盖：

- 20 个交易日回放；
- Canonical 幂等；
- 中断恢复；
- 200 只 Watchlist 容量；
- Missing Set 和发布门槛。

尚未完成：

- M1 Gate B 全部来源能力正式通过；
- 连续 5 个真实交易日 M2 试运行；
- 生产数据正式路由开放。

THS 板块关系目前保留为独立 Provider 快照，已验证 Raw 快照写入；现有 Canonical/日线 Pipeline 只接受 `BarRecord`，因此本轮不将板块关系伪装成行情分区。

## 本轮主线

当前按以下顺序逐项覆盖来源数据域：

1. EastMoney 财务主指标和股东户数扩大样本验证；
2. EastMoney 完整财务报表；
3. THS 行业板块和概念板块列表、分页与小规模数据链路；
4. EastMoney 龙虎榜；
5. EastMoney 历史日线；
6. 根据能力证据决定各数据域的 `primary`、`fallback`、`validation_only` 或 `disabled`。

## 明确不做

- 不创建独立能力管理服务、后台或能力 API；
- 不因为单次 HTTP 200 自动启用正式路由；
- 不因为当前网络失败判定整个 Provider 不可用；
- 不把多个 EastMoney 数据域合并实现；
- 不在没有用户确认时扩展到相邻数据域；
- 不在本阶段实现管理台、历史重建或自动复权版本切换。
