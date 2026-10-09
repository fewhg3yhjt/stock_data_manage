# Provider 能力验证矩阵（历史记录）

验证日期：2026-10-03

> 2026-10-09文档整理：本文保留历史窗口、样本、失败分类和证据索引，不再维护为当前能力清单。下文“当前”“本次”“资格”均指当时验证范围，不能授予今天的生产路由资格。当前输入和说明入口见 [正式接口说明](docs/providers/README.md)，证券名单专项变化见 [来源名单](docs/providers/security-catalog.md)，运行时以现有代码、YAML及未过期的能力证据为准。

> 保留原因：旧探针首尾窗口、资产矩阵和证据索引未被正式接口说明完整覆盖；删除会丢失独有的历史解释。本次不移动或改写原始验证数据。

本文记录当前正式代码中 Provider 适配器的验证状态。配置声明、临时研究脚本和适配器类的存在，都不能单独证明能力已经可以进入正式路由。

能力模型、探针生命周期、覆盖矩阵字段和 YAML 归一化规则见 [Provider 能力验证与归一化设计](provider-capability-verification-and-normalization.md)。本文只记录当前事实和证据，不替代专项设计；能力记录直接使用现有 Metadata，不建设独立管理系统。

## 验证分层

| 层级 | 目的 | 当前结果 |
|---|---|---|
| Provider Contract Fixture | 验证状态码、响应格式、字段和返回窗口分类 | 已通过 |
| 小流量 Live Probe | 验证真实接口可访问、真实返回窗口和字段 | Tencent/Sina/BaoStock/AkShare 已执行；TDX 未执行 |
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
| EastMoney | `security_board_membership` | `industry_board` / `concept_board` | 待迁移，参考 `push2/api/qt/slist/get` | 未登记 | 通过 | `secid=1.600519` 返回 28 条混合行业/概念/地域板块；`f12/f14/f3/f128` 语义可解析 | 保持 validation_only，待正式 Provider 和更宽股票样本验证 |
| EastMoney | `board_list` | `industry_board` / `concept_board` | 待迁移，参考 `push2/api/qt/clist/get` | 未登记 | 连接失败 | 行业 `m:90+t:2`、概念 `m:90+t:3` 小流量请求均 RemoteDisconnected；不能判定来源不可用 | 暂不进入路由，待其他网络/时段复测 |
| AkShare | `stock_daily` / `etf_daily` / `lof_daily` / `index_daily` | `daily_bar` | `providers/akshare/daily.py` | 通过 | 通过 | AkShare 1.18.97；股票、ETF、LOF、指数代表样本均可返回目标日期，历史首尾范围已记录 | 保持 `validation_only`，待更完整字段/单位/复权矩阵验证 |
| THS | `industry_board` / `concept_board` | `industry_board` / `concept_board` | `providers/ths/boards.py` | 通过 | 部分通过 | 原始 URL 行业列表 2/2 页成功；概念列表第 1-5 页成功；qstock 风格分页 URL 无 Cookie 返回 401，带动态 `v` Cookie 后第 6/7 页 HTTP 200 但跳转登录；6 个样本板块第 1 页均 HTTP 200；Provider 内部执行 3 秒请求间隔 | 保持 validation_only，概念列表未完成，未执行全量成分抓取和 Canonical 发布 |
| BaoStock | `query_all_stock` / `query_stock_industry` | `industry_membership` | `providers/baostock/industry.py` | Fixture 通过；旧全量结果离线重放 | 旧全市场验证脚本曾通过；当前 Provider 尚无同参数新 Live Probe | 沪深 5,212 只在市证券中 5,210 只有分类（99.9616%），缺少 001246、301716；分类更新日 2026-09-28。当前 Provider 已对齐两次全量快照参数并可重放旧结果；原始 SDK 行和 TCP 帧未留存，不能算独立的 Provider 端到端验证 | 保持 validation_only；`2026-10-03` 的单股 `code=` 请求与已验证 `date=` 全量查询不同，已标记为过期证据 |
| AkShare / THS | `stock_board_industry_name_ths` | `industry_index_daily` 的目录映射辅助 | `providers/akshare/boards.py` | Fixture 通过 | 通过 | 2026-10-02 返回 90 个行业代码且无重复；原始 HTTP 响应已归档 | 只作为指数 Provider 的目录映射；不单独注册生产目录，也不表示概念成分已验证 |
| AkShare / THS | `stock_board_industry_index_ths` | `industry_index_daily` | `providers/akshare/boards.py` | Fixture 通过 | 部分通过 | 半导体（881121）请求 2026-09-01 至 2026-10-02，返回 21 个唯一交易日，首末日 2026-09-01 至 2026-09-30；成交量、成交额单位未确认；原始响应已归档 | validation_only；只验证一个行业和一个窗口 |
| AkShare / THS | `stock_fund_flow_industry` | `board_fund_flow` | `providers/akshare/boards.py` | Fixture 通过 | 通过 | `即时` 返回 90 个不重复行业；快照没有逐行交易日期，资金字段单位未确认；原始分页响应已归档 | validation_only；不作为历史资金流，也不代表行业成分关系 |
| AkShare / THS | `stock_fund_flow_concept` | `board_fund_flow` | `providers/akshare/boards.py` | Fixture 通过 | 通过 | `即时` 返回 387 个不重复概念；快照没有逐行交易日期，资金字段单位未确认；原始分页响应已归档 | validation_only；概念资金流不等同概念成员关系 |

## 配置或设计中但尚未形成正式适配器

| Provider | 配置/设计状态 | 正式代码状态 | 路由结论 |
|---|---|---|---|
| BaoStock | `config/providers.yaml` 中为 `validation_only` | 已有 `providers/baostock/` SDK 适配器；历史日线已完成小样本验证，5m 本次空返回 | 保持校验来源，不提升为主来源 |
| EastMoney | 配置中禁用，设计用于分红事件和其他低频能力 | 已有 `providers/eastmoney/dividend.py` 分红事件适配器；其他数据域仍待实现 | 保持禁用/discovery；分红事件尚未具备正式发现资格 |
| AkShare | `config/providers.yaml` 中为 `validation_only` | 已有函数级日线适配器和可选依赖声明 | 保持校验来源，不提升为主来源 |

## Live Probe 证据

探针使用少量代表证券，证据写入 `/tmp/opencode/provider-probes/`，摘要证据同步保存于 `provider_validation/results/legacy/`，没有写入项目生产目录或项目 `metadata/` 目录。

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

- `provider_validation/results/legacy/2026-09-29-sina-daily.json`
- `provider_validation/results/legacy/2026-09-29-tencent-daily.json`
- `provider_validation/results/legacy/2026-09-29-sina-minute.json`
- `provider_validation/results/legacy/2026-09-29-tencent-minute.json`
- `provider_validation/results/legacy/2026-09-29-baostock-daily.json`
- `provider_validation/results/legacy/2026-09-29-baostock-minute-5m.json`
- `provider_validation/results/legacy/2026-09-29-eastmoney-dividend-event.json`
- `provider_validation/results/legacy/2026-09-29-eastmoney-security-list.json`
- `provider_validation/results/legacy/2026-09-29-eastmoney-realtime-quote.json`
- `provider_validation/results/legacy/2026-09-29-eastmoney-stock-fund-flow.json`
- `provider_validation/results/legacy/2026-09-29-eastmoney-full-api-3s.json`
- `provider_validation/results/legacy/2026-09-29-eastmoney-financial-main.json`
- `provider_validation/results/legacy/2026-09-29-eastmoney-shareholder-count.json`
- `provider_validation/results/legacy/2026-09-29-akshare-daily.json`
- `provider_validation/results/legacy/2026-09-29-akshare-daily-live.json`
- `provider_validation/results/legacy/2026-09-30-ths-board-members.json`
- `provider_validation/results/legacy/2026-09-30-ths-board-members-v2.json`
- `provider_validation/results/legacy/2026-09-30-ths-board-list-pagination.json`
- `provider_validation/results/legacy/2026-09-30-ths-small-batch.json`
- `provider_validation/results/legacy/2026-09-30-ths-qstock-request-variant.json`
- `provider_validation/results/legacy/2026-10-01-eastmoney-board-probe.json`
- `provider_validation/results/legacy/2026-10-01-security-board-coverage.json`
- `provider_validation/results/legacy/2026-10-02-sector-capabilities-live.json`（隔离网络连接失败）
- `provider_validation/results/legacy/2026-10-02-sector-capabilities-network-retry.json`
- `provider_validation/results/legacy/2026-10-02-sector-derived-validation.json`
- `provider_validation/tests/replay_sector_capability_archives.py`（离线重放脚本，不发起网络请求）
- `provider_validation/tests/replay_sector_capability_archives.py`（离线重放脚本，不发起网络请求）
- `provider_validation/results/legacy/2026-10-03-baostock-csrc-provider.json`（请求参数变体已标记 superseded，不计为迁移验证）
- 原始响应：`provider_validation/results/raw/2026-10-02-sector-capabilities-v1/`、`provider_validation/results/raw/2026-10-02-sector-capabilities-network-retry/`、`provider_validation/results/raw/2026-10-03-csrc-provider-small-probe/`
- 派生 CSV：`provider_validation/results/2026-10-02-sector-derived/`
- 完整本地运行目录：`/tmp/opencode/provider-probes/`

每份证据包含 Provider、Endpoint、能力版本、验证时间、有效期、请求范围、HTTP 状态、返回窗口、字段语义、单位、证据哈希和路由资格。

## 语义结论

1. Tencent 正式代码已经支持 `forward_history` QFQ Endpoint；Sina 仍只支持不复权日线。
2. Tencent QFQ 本次股票和 ETF 样本通过，LOF 返回空；指数不允许套用股票前复权。
3. Tencent/Sina 不复权日线和 Tencent QFQ 日线本次都达到返回上限，不能直接用于一只证券的完整历史重建。
4. Tencent 和 Sina 的成交量单位存在差异，已经进入探针证据；日线成交量和成交额单位仍需针对历史接口分别完成语义验证，当前不能标记为已确认。
5. 分钟探针现在会按 `as_of` 过滤未来记录，并记录来源单位；本次只验证了沪市股票，分钟的全市场、北交所、ETF、LOF 和指数覆盖仍需代表矩阵验证。
6. EastMoney 3 秒间隔综合探针中，个股资金流、财务主指标、股东户数、分红事件和实时行情原始脚本接口返回了业务数据；历史日线、前复权日线、1m、证券列表和行业板块受当前网络断连影响；龙虎榜本轮未猜测接口。正式适配器仍保持 validation_only/disabled。
7. BaoStock 日线、行业成分 Provider 均保持 `validation_only`；行业成分 Provider 已通过 Fixture 与旧全量证据的离线重放，历史全市场证据的在市沪深覆盖为 99.9616%，但旧证据没有原始 SDK 行或 TCP 帧，当前 Provider 尚未进行同参数新 Live Probe。AkShare 股票、ETF、LOF、指数日线代表样本已通过真实探针，THS 行业指数和资金流新增能力只验证了报告列明的样本/窗口，仍保持 `validation_only`。
8. THS 即时行业和概念资金流是有时间戳的排行快照，不含证券-概念成员关系。所测金额字段按原值保存，单位未确认；不可用于历史逐日资金流或概念成分覆盖结论。

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


## 2026-09-13旧总体设计中的抽样摘要

以下是 2026-09-13 在当前运行环境中的抽样验证结论。它曾用于讨论首版配置，不代表今天的配置或第三方接口的永久承诺；每项能力必须保留验证时间、样本、首尾时间、行数和结果摘要。

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
provider_validation/results/legacy/tmp_test/etf_daily_probe/gap_lof_20260913/summary.json
provider_validation/results/legacy/tmp_test/etf_daily_probe/gap_lof_eastmoney_only_20260913/summary.json
```

探针脚本中“请求完成”不得直接映射为 `published`；零行结果必须按 `temporary_empty` 处理，这一规则同样适用于未来所有 Capability Probe。

上述内容从被合并的旧总体设计保留，原文版本为 `72f94aca247691c3b0b72ee185505761f6197fb4`。本次仅保存独有的历史抽样解释，没有重新执行探针；其来源顺序、数量、TDX样本和时效不构成当前正式能力。当前TDX未取得正式Provider在线资格，证券清单任务仍受来源门禁约束。

2026-10-09目录整理只更新以上历史摘要的文件位置，原字节和验证范围不变。原 `tmp_test/` 已归档；原路径与新路径的关系见 [迁移清单](provider_validation/results/legacy/tmp_test/archive-manifest.json)。
