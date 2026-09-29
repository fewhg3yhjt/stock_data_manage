# Provider 能力验证矩阵

验证日期：2026-09-29

本文记录当前正式代码中 Provider 适配器的验证状态。配置声明、临时研究脚本和适配器类的存在，都不能单独证明能力已经可以进入正式路由。

能力模型、Probe 生命周期、覆盖矩阵字段和 YAML 归一化规则见 [Provider 能力验证与归一化设计](provider-capability-verification-and-normalization.md)。本文只记录当前事实和证据，不替代专项设计。

## 验证分层

| 层级 | 目的 | 当前结果 |
|---|---|---|
| Provider Contract Fixture | 验证状态码、响应格式、字段和返回窗口分类 | 已通过 |
| 小流量 Live Probe | 验证真实接口可访问、真实返回窗口和字段 | Tencent/Sina 已执行；TDX 未执行 |
| 语义验证 | 确认复权口径、单位、时间和覆盖范围 | 部分完成；前复权仍未达到正式资格 |
| 端到端生产验证 | 验证路由、补缺、标准化、发布和恢复 | Fixture 链路已通过；真实来源尚未进入生产运行 |

## 正式适配器状态

| Provider | Endpoint | 数据集 | 适配器 | 离线 Contract | Live Probe | 当前语义 | 路由结论 |
|---|---|---|---|---|---|---|---|
| Tencent | `recent_history` | `daily_bar` | `providers/tencent/daily.py` | 通过 | 通过 | 当前正式适配器为不复权；约 1024 行上限，实际本次返回 1025 行并标记截断；沪深、ETF、LOF、指数样本通过，北交所样本临时空返回 | 仅可按已验证资产/市场范围作为不复权近期历史能力；不能作为前复权正式主源 |
| Sina | `full_history` | `daily_bar` | `providers/sina/daily.py` | 通过 | 通过 | 当前正式适配器为不复权；1023 行上限，本次返回 1023 行并标记截断；沪深、北交所、ETF、LOF、指数样本通过 | 仅可按已验证资产/市场范围作为不复权近期历史能力；不能作为前复权正式主源 |
| Tencent | `forward_history` | `daily_bar` | `providers/tencent/daily.py` | 通过 | 通过 | QFQ Endpoint；本次股票和 ETF 返回 641 行并标记截断，LOF 返回空；只声明股票/ETF 资格 | 股票/ETF 可进入前复权候选；LOF 暂不可用；指数在流水线入口拒绝前复权 |
| Tencent | `native_1m` | `minute_bar_1m` | `providers/tencent/minute.py` | 通过 | 通过 | OHLCV 返回；本次成交量语义为 `lot` | 可进入分钟能力候选，但仍需按市场和资产类型扩展验证 |
| Sina | `native_5m` | `minute_bar_5m` | `providers/sina/minute.py` | 通过 | 通过 | OHLCV 返回；本次成交量语义为 `share` | 可进入分钟能力候选，但仍需按市场和资产类型扩展验证 |
| TDX | `delayed_1m` | `minute_bar_1m` | `providers/tdx/minute.py` | 通过 | 未执行 | 适配器要求外部 TDX 客户端注入；当前没有真实客户端探针 | 保持禁用，不进入正式路由 |

## 配置或设计中但尚未形成正式适配器

| Provider | 配置/设计状态 | 正式代码状态 | 路由结论 |
|---|---|---|---|
| BaoStock | `config/providers.yaml` 中为 `validation_only` | 当前 `providers/` 没有 BaoStock Provider 实现；只有测试 Fixture 和临时研究脚本 | 不能按已实现渠道使用，保持校验/未实现状态 |
| EastMoney | 配置中禁用，设计用于公司行动和低频能力 | 当前没有正式 EastMoney Provider 适配器；临时脚本不属于运行时实现 | 保持禁用；公司行动能力尚未实现 |
| AkShare | 设计文档提及 | 当前没有正式适配器或正式配置 | 明确为未实现，不得进入路由 |

## Live Probe 证据

探针使用少量代表证券，证据写入 `/tmp/opencode/provider-probes/`，摘要证据同步保存于 `docs/provider-probes/`，没有写入项目生产目录或项目 `metadata/` 目录。

| Provider | Endpoint | 请求范围 | HTTP | 返回窗口 | 行数 | 首键 | 末键 | 字段语义 | 单位 | 资格 |
|---|---|---|---:|---|---:|---|---|---|---|---|
| Sina | `full_history` | `trade_date=2026-09-11` | 200 | `truncated` | 1023 | 2022-07-13 | 2026-09-28 | trade_date/open/high/low/close/volume/amount | volume/amount 未确认 | 不复权能力可选；前复权不可选 |
| Tencent | `recent_history` | `trade_date=2026-09-11` | 200 | `truncated` | 1025 | 2022-07-12 | 2026-09-29 | trade_date/open/high/low/close/volume/amount | volume/amount 未确认 | 不复权能力可选；前复权不可选 |
| Sina | `native_5m` | `as_of=2026-09-29T09:24:00+08:00` | 200 | `complete` | 119 | 2026-09-23T13:10:00 | 2026-09-28T15:00:00 | trade_date/bar_time/open/high/low/close/volume/amount | volume/share | 分钟候选可选 |
| Tencent | `native_1m` | `as_of=2026-09-29T09:24:00+08:00` | 200 | `complete` | 118 | 2026-09-28T13:03:00 | 2026-09-28T15:00:00 | trade_date/bar_time/open/high/low/close/volume | volume/lot | 分钟候选可选 |

日线资产矩阵补充结果：

| Provider | 沪市股票 | 深市股票 | 北交所股票 | ETF | LOF | 指数 |
|---|---|---|---|---|---|---|
| Sina | 截断窗口，可解析 | 截断窗口，可解析 | 截断窗口，可解析 | 截断窗口，可解析 | 截断窗口，可解析 | 截断窗口，可解析 |
| Tencent | 截断窗口，可解析 | 截断窗口，可解析 | 目标日临时空返回，不具备本次资格 | 截断窗口，可解析 | 截断窗口，可解析 | 截断窗口，可解析 |

探针证据文件：

- `docs/provider-probes/2026-09-29-sina-daily.json`
- `docs/provider-probes/2026-09-29-tencent-daily.json`
- `docs/provider-probes/2026-09-29-sina-minute.json`
- `docs/provider-probes/2026-09-29-tencent-minute.json`
- 完整本地运行目录：`/tmp/opencode/provider-probes/`

每份证据包含 Provider、Endpoint、能力版本、验证时间、有效期、请求范围、HTTP 状态、返回窗口、字段语义、单位、证据哈希和路由资格。

## 语义结论

1. Tencent 正式代码已经支持 `forward_history` QFQ Endpoint；Sina 仍只支持不复权日线。
2. Tencent QFQ 本次股票和 ETF 样本通过，LOF 返回空；指数不允许套用股票前复权。
3. Tencent/Sina 不复权日线和 Tencent QFQ 日线本次都达到返回上限，不能直接用于一只证券的完整历史重建。
4. Tencent 和 Sina 的成交量单位存在差异，已经进入探针证据；日线成交量和成交额单位仍需针对历史接口分别完成语义验证，当前不能标记为已确认。
5. 分钟探针现在会按 `as_of` 过滤未来记录，并记录来源单位；本次只验证了沪市股票，分钟的全市场、北交所、ETF、LOF 和指数覆盖仍需代表矩阵验证。
6. 没有正式适配器的 BaoStock、EastMoney 和 AkShare，不能因为配置或临时脚本存在而进入正式路由；它们在 `providers.yaml` 中仍是显式配置但未实现状态。

## 后续资格门槛

前复权日线扩大正式路由前，必须补齐：

- 北交所、LOF 的可用来源或明确替代路由；
- 沪深股票、北交所、ETF、LOF 的完整覆盖证据；
- 历史首尾边界和最大返回窗口；
- 分页或窗口拆分规则；
- OHLC 前复权语义和重复请求稳定性；
- 除权后历史更新时间确认；
- 日线成交量、成交额单位确认；
- 与当前正式数据的重叠历史比较证据。

在这些证据完成前，正式日线目标仍不能切换为前复权生产口径。
