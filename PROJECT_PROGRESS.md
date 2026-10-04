# 项目开发进度

更新时间：2026-10-04

本文是当前项目的进度总览，记录代码、验证和设计落地状态。详细架构以设计文档为准，来源事实以 [Provider 能力验证矩阵](PROVIDER_CAPABILITY_MATRIX.md) 为准。

## 当前阶段

通用候选入口已接通三十八项输入：腾讯行情快照、前复权日线、最近5分钟线，东财涨停池、新浪交易日历，同花顺行业目录、行业指数日线、行业/概念即时资金流，以及复用原两查询完成的 BaoStock 证券快照和行业归属，及原东财股东户数、已实施分红历史、个股资金流，和扩展原股票池模块的炸板、跌停、昨日涨停、强势股池，以及原财务Provider的业绩预告、机构调研汇总、股东增减持、回购、质押、新股申购日历和LPR历史，以及华尔街见闻A股快讯和新闻联播标题目录、回购定盘利率和可转债条款行情，以及新浪国内期货快照、期货日线和A50报价、财联社电报和新浪全球资讯、PMI与社融统计月历史、新浪研报列表和见闻宏观日历。参数来源、字段模板、来源映射与候选留证实际执行；自动调度和正式路由保持关闭。同花顺说明见 [同花顺记录](provider_validation/docs/2026-10-04-ths-input-collection.md)，BaoStock 两项见 [BaoStock记录](provider_validation/docs/2026-10-04-baostock-input-collection.md)，腾讯快照见 [本批记录](provider_validation/docs/2026-10-04-tencent-snapshot-input-collection.md)。下方历史来源状态表示各自已记录样本，不代表当前全量在线可用性。

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
| EastMoney | 分红事件、证券列表、实时行情、个股资金流、财务主指标、股东户数；板块归属探针 | 分红 HTTP 200/空窗口；证券列表/实时行情/资金流/板块列表当前网络断连；个股板块归属 `slist` 单股票返回 28 条；财务主指标和股东户数单股票样本通过 | disabled/validation_only |
| AkShare | 股票、ETF、LOF、指数历史日线 | 代表样本 Live Probe 通过 | validation_only |
| THS | 行业/概念板块列表和成分 | 原始 URL 行业列表 2/2 页通过；概念列表到第 5 页；qstock 风格 URL（含动态 v Cookie）第 6/7 页均进入登录跳转；6 个样本板块第 1 页通过；无 403/429 | validation_only |

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
312 passed
```

本轮测试证据为 `provider_validation/results/ths-final-20261004-tests.xml`；四项历史响应及原实现/CSV对照为 `provider_validation/results/ths-final-20261004/comparison.json`。离线样本行数分别为90、21、90、387；未新增实时验证，未核实单位不进入候选标准金额/量额字段。

后续 BaoStock 完整回归为 `provider_validation/results/bao-final-20261004-tests.xml`，对照及总索引为同前缀目录/验证文件。原证券分母5223、行业匹配5221和缺失2只均保留。SDK解码载荷之外的TCP帧不可见；未新增实时探针、生产写入或自动调度。

最新腾讯快照批次完整回归为 `provider_validation/results/tencent-final-20261004-v2-tests.xml`（270项全通过），对照及总索引为同前缀目录/验证文件。原脚本/原CSV/旧Provider/请求配置对照通过，真实来源范围仅sh600519；203只沪深北分批及部分覆盖属于合成离线验证，未扩大来源资格。快照复用原行情模板和YAML映射，量额标准列保持未核验为空，自动快照继续由在线批量验证门禁阻断。日线14行、最近5分钟96行及实际CLI快照1行回归通过。初版证据保留，第二版修正合成部分缺失夹具的字节长度。

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

东财三项转换记录见 [东财记录](provider_validation/docs/2026-10-04-eastmoney-input-collection.md)。最新回归证据为 `provider_validation/results/em-final-20261004-tests.xml`（292项），原脚本/SDK/CSV/请求及旧调用对照为 `results/em-final-20261004/`。600519样本股东63行、分红来源28行选实施27行且预披露1行留证、资金流120行；修正旧大小单净额/占比列错位。未核准单位保留来源原值、标准相应字段为空；本批没有实时请求或生产启用。

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

## 自主转换推进

用户已确认剩余51项按既有方向自主推进，普通实现细节不再逐批确认；口径不明与生产启用仍单独确认。首批接通四个股票池并将ASTOCK-023成功同花顺分支归并为SDA-BOARD-003别名。最新状态为18项候选、46项待转换、7项阻断、3项别名，合计74项契约。来源qdate=20260930，股票池样本12/9/57/199行，别名对照90行；原涨停池52行默认合同保持。详见 [股票池记录](provider_validation/docs/2026-10-04-stock-pool-input-collection.md)，最新完整回归为 `provider_validation/results/pools-final-20261004-tests.xml`（312项），最终回放、CLI及哈希索引为同前缀证据。单位、实时、独立覆盖/容量和生产门禁仍待完成。

## 自主转换后续：业绩预告与机构调研（2026-10-04）

扩展原财务Provider，两项各50行的原始响应、原可执行脚本和候选标准字段逐项对照；默认财务方法保持。补两组必要字段模板/映射，数量上限可配置，有效筛选空表与失败分开。当前20项候选、44项待转换、7项阻断、3项别名；完整回归335项，见 `provider_validation/results/events-final-20261004-tests.xml`，详细证据见 [本批记录](provider_validation/docs/2026-10-04-eastmoney-events-input-collection.md)。ST原东财失败、BaoStock兜底缺可回放结果，继续待转换；金额单位、实时可用性、全量覆盖和生产资格未认证。

## 自主转换后续：四项金融事件（2026-10-04）

复用原财务Provider及原会话/分页查询，转换增减持、回购、质押、新股日历，分别与原脚本50/50/50/30行对照。质押保留先查最新统计日再查名单的原请求流程；未知回购进度和新股未来排期保留。回购进度标签与板块备用字段在原归一化YAML管理。当前24项候选、40项待转换、7项阻断、3项别名；完整回归354项，见 `provider_validation/results/actions-final-20261004-tests.xml`，详细记录见 [四项事件记录](provider_validation/docs/2026-10-04-eastmoney-actions-input-collection.md)。单位、实时、独立完整性、持续容量和生产资格均未认证。

## 自主转换后续：LPR历史（2026-10-04）

继续扩展原财务Provider，原四页1576行中按原脚本选择1538行、排除38行并分别留证；保持每日/月度机制原日期及五年期空值，不新增筛选参数或重采样。必要YAML模板和映射、原脚本对照、6类失败、Session四页复用/缓存与实际CLI回放已核验。当前25项候选、39项待转换、7项阻断、3项别名；完整回归363项，证据见 [LPR记录](provider_validation/docs/2026-10-04-lpr-input-collection.md) 和 `provider_validation/results/lpr-final-v2-20261004-tests.xml`。利率单位、实时、独立完整性和生产资格仍未认证。

## 2026-10-04 新闻输入候选采集

迁移ASTOCK-034见闻A股快讯50行与ASTOCK-035央视目录14行，必要新增两来源Provider、两套字段模板和映射。保留原请求策略、正文禁用、播出日/北京时间及游标语义；回归380项，证据在news-final-20261004和news-cli-20261004，最终索引news-final-20261004-verification.json。当前27项候选、37项未实现、7项阻塞、3项别名，路由和调度仍关闭。

## 2026-10-04 回购定盘和可转债候选输入

ASTOCK-064沿原CSV转换FR747条，ASTOCK-084修改原财务Provider，原三页1059条按捕获日保留322条及737条排除证据，支持包含已摘牌。字段模板、映射、完整解析行/异常/原时钟/真实入口回放已检查，回归401项，证据及哈希索引在rates-bonds-final-v2-20261004等目录。当前29项候选、35项未实现、7项阻塞、3项别名；单位、实时可用性与全量覆盖仍待认证，不启动自动调度或正式路由。

## 2026-10-04 新浪三项期货候选输入

修改原新浪快照/日线类，新增国内三合约快照、RB0/M0日线窗口各181条及A50一条；保留原请求、源时间、代码身份与零值规则，既有股票方法与原源码回归一致。三套YAML及原始/解析/排除/入口留证已完成，完整回归421项。LF换行前后语法树一致，最终证据在sina-futures-final-v2-20261004、sina-futures-cli-v2-20261004及哈希索引。当前32项候选、32项未实现、7项阻塞、3项别名，未启用调度或正式路由。

## 财联社与新浪资讯输入迁移（2026-10-04）

两来源各20条原成功归档与原SDK、原CSV、候选字段逐项一致；新增必要新闻适配器和四份YAML，不重写股票或期货Provider。原SDK、限频重试和主机暂停保留；财联社签名留证脱敏、回放查询时钟仅作用于原截止时间。完整回归及最终证据见sdk-news-final-20261004、sdk-news-cli-20261004和同前缀XML/哈希索引。当前34项候选、30项未实现、7项阻塞、3项别名；实际当前在线能力和历史完整性未认证，未启用生产或调度。

## PMI与社融输入迁移（2026-10-04）

PMI扩展原财务Provider，社融补必要商务部来源适配器；原225月/136月与SDK、CSV、原数值逐项对照。四份YAML实际执行，原TLS适配器、无请求体POST和传输策略保留；统计月不当发布时间，单位未认证数字仍完整留证。最终证据见macro-final-20261004、macro-cli-20261004及同前缀XML/哈希索引。当前36项候选、28项未实现、7项阻塞、3项别名，正式路由和调度关闭。

## 新浪研报和见闻宏观日历输入迁移（2026-10-04）

在原两新闻适配器扩展方法，不新增Provider。原新浪全市场最新页40条、见闻三段552条及中国重要度筛选48条对照一致；504条排除行留证。四份YAML落地，原研报6秒/假空页重试和日历分段/文本单位保持；实际入口和完整回归见reports-calendar-final-20261004、reports-calendar-cli-20261004及同前缀XML/哈希索引。当前38项候选、26项待转换、7项阻塞、3项别名；生产路由和调度关闭。
