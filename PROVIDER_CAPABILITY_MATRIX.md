# Provider 能力验证矩阵

验证日期：2026-09-29

本文记录当前正式代码中 Provider 适配器的验证状态。配置声明、临时研究脚本和适配器类的存在，都不能单独证明能力已经可以进入正式路由。

能力模型、探针生命周期、覆盖矩阵字段和 YAML 归一化规则见 [Provider 能力验证与归一化设计](provider-capability-verification-and-normalization.md)。本文只记录当前事实和证据，不替代专项设计；能力记录直接使用现有 Metadata，不建设独立管理系统。

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
| BaoStock | `daily_history` | `daily_bar` | `providers/baostock/daily.py` | 通过 | 通过 | SDK 日线；本次返回 6077 行，2001-08-27 至 2026-09-11；成交量按股、成交额按元 | 保持 `validation_only`，待资产矩阵和归一化规则扩展 |
| BaoStock | `minute_5m` | `minute_bar_5m` | `providers/baostock/minute.py` | 通过 | 空返回 | 本次沪市股票 5m 返回空 | 保持 `validation_only` 且当前不可选 |
| EastMoney | `dividend_event` | `dividend_event` | `providers/eastmoney/dividend.py` | 通过 | HTTP 200/空窗口 | `RPT_SHAREBONUS_DET` 响应可解析；2026-09-24 至 2026-09-29 返回 0 事件 | 保持 disabled/discovery，待更宽窗口和字段语义验证 |
| EastMoney | `security_list` | `security_master` | `providers/eastmoney/security_list.py` | 通过 | RemoteDisconnected | 分页和股票/ETF/LOF/指数映射已实现；当前网络未取得响应 | 保持 validation_only/disabled，不进入正式路由 |
| EastMoney | `single_quote` / `batch_quote` / `intraday_trend` | `realtime_quote` / `intraday_trend` | `providers/eastmoney/realtime.py` | 通过 | 原脚本曾通过；正式迁移后当前复测 RemoteDisconnected | 行为已对齐原脚本的 Session、请求头、Referer、重试、Host 降级和业务判断；当前运行环境仍未取得新证据 | 保持 validation_only/disabled，不进入正式路由 |
| EastMoney | `stock_fund_flow` | `stock_fund_flow` | `providers/eastmoney/fund_flow.py` | 通过 | 原脚本曾通过；正式迁移后当前复测 RemoteDisconnected | 个股日级资金流字段映射已实现，当前运行环境未取得新证据 | 保持 validation_only/disabled，不进入正式路由 |
| EastMoney | `financial_main` | `financial_main` | `providers/eastmoney/financial.py` | 通过 | 通过 | 单股票返回 8 个报告期，财务主指标字段可解析 | 保持 validation_only，待更宽资产/报告期覆盖 |
| EastMoney | `shareholder_count` | `shareholder_count` | `providers/eastmoney/shareholder.py` | 通过 | 通过 | 单股票返回 10 个报告期，股东户数和变动字段可解析 | 保持 validation_only，待更宽资产/报告期覆盖 |
| AkShare | `stock_daily` / `etf_daily` / `lof_daily` / `index_daily` | `daily_bar` | `providers/akshare/daily.py` | 通过 | 通过 | AkShare 1.18.97；股票、ETF、LOF、指数代表样本均可返回目标日期，历史首尾范围已记录 | 保持 `validation_only`，待更完整字段/单位/复权矩阵验证 |
| THS | `industry_board` / `concept_board` | `industry_board` / `concept_board` | `providers/ths/boards.py` | 通过 | 通过 | 同花顺网页代表板块行业 20 行、概念 10 行；代码/名称字段可解析；Provider 内部执行 3 秒请求间隔 | 保持 validation_only，待板块列表、分页和全量覆盖验证 |

## 配置或设计中但尚未形成正式适配器

| Provider | 配置/设计状态 | 正式代码状态 | 路由结论 |
|---|---|---|---|
| BaoStock | `config/providers.yaml` 中为 `validation_only` | 已有 `providers/baostock/` SDK 适配器；历史日线已完成小样本验证，5m 本次空返回 | 保持校验来源，不提升为主来源 |
| EastMoney | 配置中禁用，设计用于分红事件和其他低频能力 | 已有 `providers/eastmoney/dividend.py` 分红事件适配器；其他数据域仍待实现 | 保持禁用/discovery；分红事件尚未具备正式发现资格 |
| AkShare | `config/providers.yaml` 中为 `validation_only` | 已有函数级日线适配器和可选依赖声明 | 保持校验来源，不提升为主来源 |

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
- `docs/provider-probes/2026-09-29-baostock-daily.json`
- `docs/provider-probes/2026-09-29-baostock-minute-5m.json`
- `docs/provider-probes/2026-09-29-eastmoney-dividend-event.json`
- `docs/provider-probes/2026-09-29-eastmoney-security-list.json`
- `docs/provider-probes/2026-09-29-eastmoney-realtime-quote.json`
- `docs/provider-probes/2026-09-29-eastmoney-stock-fund-flow.json`
- `docs/provider-probes/2026-09-29-eastmoney-full-api-3s.json`
- `docs/provider-probes/2026-09-29-eastmoney-financial-main.json`
- `docs/provider-probes/2026-09-29-eastmoney-shareholder-count.json`
- `docs/provider-probes/2026-09-29-akshare-daily.json`
- `docs/provider-probes/2026-09-29-akshare-daily-live.json`
- `docs/provider-probes/2026-09-30-ths-board-members.json`
- `docs/provider-probes/2026-09-30-ths-board-members-v2.json`
- 完整本地运行目录：`/tmp/opencode/provider-probes/`

每份证据包含 Provider、Endpoint、能力版本、验证时间、有效期、请求范围、HTTP 状态、返回窗口、字段语义、单位、证据哈希和路由资格。

## 语义结论

1. Tencent 正式代码已经支持 `forward_history` QFQ Endpoint；Sina 仍只支持不复权日线。
2. Tencent QFQ 本次股票和 ETF 样本通过，LOF 返回空；指数不允许套用股票前复权。
3. Tencent/Sina 不复权日线和 Tencent QFQ 日线本次都达到返回上限，不能直接用于一只证券的完整历史重建。
4. Tencent 和 Sina 的成交量单位存在差异，已经进入探针证据；日线成交量和成交额单位仍需针对历史接口分别完成语义验证，当前不能标记为已确认。
5. 分钟探针现在会按 `as_of` 过滤未来记录，并记录来源单位；本次只验证了沪市股票，分钟的全市场、北交所、ETF、LOF 和指数覆盖仍需代表矩阵验证。
6. EastMoney 3 秒间隔综合探针中，个股资金流、财务主指标、股东户数、分红事件和实时行情原始脚本接口返回了业务数据；历史日线、前复权日线、1m、证券列表和行业板块受当前网络断连影响；龙虎榜本轮未猜测接口。正式适配器仍保持 validation_only/disabled。
7. BaoStock 已有正式 SDK 适配器，但当前仅历史日线小样本通过，保持 `validation_only`；5m 本次空返回。AkShare 股票、ETF、LOF、指数日线代表样本已通过真实探针，仍保持 `validation_only`，等待更完整字段、单位、复权和覆盖矩阵验证。

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
