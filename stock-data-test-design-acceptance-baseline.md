# 证券数据采集平台测试设计与验收基线

> 版本：2026-09-13 Phase 1 基线版  
> 对应设计：[证券全量数据采集与管理平台设计方案](stock-data-design-realtime.md)  
> 适用范围：Security Master、Trading Calendar、日线、Watchlist 1m/5m、快照、Raw/Hot/Canonical、来源切换与恢复

> 2026-10-09补充：接口验证区只用于正式实现前的可行性验证；业务开发及验收使用正式模块和业务运行布局。raw每天唯一及真实任务验收要求见下文4.5；新增要求当前待代码实现及执行验收，不表示M1或M2已通过。

---

# 1. 文档目标

本文件用于回答两个问题：

1. 如何证明 Phase 1 已经开发完成；
2. 如何证明系统可以进入持续运行，而不只是“接口偶尔能返回数据”。

验收不以 HTTP 成功率或采集脚本不报错作为唯一标准，而以以下结果为准：

```text
数据可用
+ 缺失可识别
+ 冲突可解释
+ 重复不污染
+ 故障可恢复
+ 来源可追溯
+ 时效满足约定
```

---

# 2. 验收阶段定义

Phase 1 分为两个验收里程碑。

## 2.1 M1：开发完成

满足以下条件，可以判定代码开发完成并进入试运行：

```text
单元测试通过
+ Provider Contract 测试通过
+ 最近20个交易日离线回放通过
+ 全部故障注入测试通过
+ 200只 Watchlist 容量测试通过
+ 端到端结果可追溯
```

M1 不要求等待多个真实交易日，但必须用固定 Raw Fixture、虚拟时钟和故障代理完整模拟盘中、盘后、换源和重启。

## 2.2 M2：试运行验收

满足以下条件，可以判定 Phase 1 具备持续运行条件：

```text
M1 已通过
+ 至少连续5个真实交易日运行
+ 每日 Reconciliation 在 SLA 内结束
+ 无未解释数据缺口
+ 无重复 Canonical 主键
+ 无未恢复的半成品分区
+ 无 P0 / P1 未关闭缺陷
```

M1 和 M2 必须分别出具测试报告，不能用一次手工查询代替。

---

# 3. 测试范围

## 3.1 Phase 1 必测范围

```text
Security Master
security_symbol_history
Trading Calendar

daily_bar
 dividend_event
qfq_history_rebuild
minute_bar_1m
minute_bar_5m
snapshot

股票：XSHG / XSHE / BSE
基金：ETF / LOF
指数：XSHG / XSHE 及已纳入指数目录的其他命名空间

Sina / Tencent 常规来源
TDX 延迟分钟来源（可选；公共主站不稳定时允许禁用并保留探针证据）
EastMoney / BaoStock 低优先级来源及禁用、恢复逻辑

Raw Storage
SQLite WAL Hot Store
Parquet Canonical
DuckDB Metadata

Missing Set
Retry / Fallback
Resolution / Conflict Log
Atomic Publish
Collection Attempt / Lease / Recovery
```

## 3.2 本阶段不作为完成条件

```text
可转债全量能力
估值、行业、概念、财务等扩展指标的全量 SLA
全市场实时分钟采集
复杂 DAG 或分布式调度
管理后台
交易系统或实盘下单
```

这些能力以后增加时，应单独补充 Dataset Contract 和验收基线，不能沿用日线或分钟结论直接推定。

---

# 4. 核心验收原则

## 4.1 第一版只区分有效数据和缺失数据

日线第一版不区分停牌、确认无数据、临时空值等证券状态，只计算：

```text
有效数据证券集合
缺失证券集合 = 目标证券集合 - 有效数据证券集合
```

接口请求成功、返回空值或只写入原始响应，都不代表已经取得有效数据。没有通过标准化和基础校验的证券仍然属于缺失证券。

## 4.2 发布门槛

```text
有效数据覆盖率
= 有效数据证券数 / 目标证券数
```

股票日线只有在缺失比例不超过 1%，并且缺失数量不超过 50 只时才允许发布。两个条件必须同时满足，具体数值以 `config/datasets.yaml` 为准。

## 4.3 来源优先级不是正确性证明

Sina / Tencent 是默认常规来源，TDX 是分钟补采与校准来源，EastMoney / BaoStock 是低优先级来源。任何来源仍必须通过：

```text
Capability 有效期
Schema Contract
覆盖范围
历史首尾边界
字段完整度
单位和时间语义
来源健康状态
```

## 4.4 采集 At-least-once，发布必须幂等

采集任务可以重跑，精确响应字节可作为审计证据留存；raw同来源、接口、数据日期及请求范围只有一份活动结果，Canonical同一业务主键只能存在一条正式记录。

---

## 4.5 开发目录、按日raw和真实任务验收补充

| 用例 | 验收要求 |
|---|---|
| RAW-DATE-001 不同日期 | 同来源、接口及范围的两个数据日期分别保存，后一天不改变前一天文件及哈希 |
| RAW-DATE-002 同日全量 | 清理目标任务暂存、强制从源头重采；成功后替换当天范围，失败不破坏已有正式记录 |
| RAW-DATE-003 断点及指定证券 | 识别缺失和异常单元，补采后合并派生记录及去重，保留当天其他证券和其他日期；不修改拼接原响应字节 |
| RAW-DATE-004 补采与回放日期 | 抓取时间和数据所属日期独立；历史日线按目标日保存，当前目录按原响应采集日保存，回放不改写数据日期 |
| RUN-PATH-001 开发边界 | 实现前可行性探针可以输出验证区；进入业务开发后代码、数据、Provider联调及验收新产物均不写入该区，历史证据仅只读引用 |
| RUN-TASK-001 正式执行 | 通过实际业务任务或调度入口生成任务状态、raw、来源候选及最终结果；资格阻断、只取得来源候选或未发布时据实报告，不能标记正式完成 |
| RUN-TASK-002 自动报告 | 任务在发布前自动生成最终检查，包含范围、请求日期、实际日期、来源、数量、失败及回退原因和原始引用；恢复后报告与数据相符 |
| RUN-TASK-003 覆盖口径 | 证券清单以已核准日期及状态口径核对；无独立分母只报告检查结果。日线按冻结目标集合计算覆盖，不以返回行数充当市场分母 |

所有用例关联实际入口、参数、代码/配置版本、输入响应哈希、自动输出文件及验证结论。中间只维护基础合同和单元状态供重做使用，最终覆盖与有效性检查发生在正式发布前。人工汇总或刷新接口说明表不能代替真实任务报告。详细职责见 [存储说明](docs/storage/README.md) 和 [任务流程](docs/pipeline/collection-tasks.md)。

# 5. 测试数据设计

## 5.1 全量 Universe

日线和 Security Master 验收使用运行日完整 Universe，不使用固定证券数量作为长期断言。测试报告必须记录当次实际数量和来源证据。

北交所列表需与运行日北交所官网及代码映射证据核对。2026-09-13 抽样得到的 343 只只作为回归参考，不作为永久固定值。

## 5.2 功能 Watchlist

功能 Watchlist 不少于 30 只，并至少包含：

| 类型 | 最低数量 | 必含边界样本 |
|---|---:|---|
| 上交所股票 | 5 | 高价股、低成交量股、停牌或临停样本 |
| 深交所股票 | 5 | 主板、创业板样本 |
| 北交所股票 | 5 | 至少一个存在新旧代码映射的证券 |
| ETF | 5 | 沪深市场均覆盖 |
| LOF | 4 | 沪深市场均覆盖，包含东财列表独有但新浪历史可取样本 |
| 指数 | 4 | 上证、深证、跨市场代表指数 |
| 动态边界样本 | 2 | 新上市、退市前历史、全天停牌等按运行日动态选择 |

边界证券不能永久写死；测试准备阶段根据 Security Master 动态选择，并把最终清单固化到本次 `test_run_manifest`。

## 5.3 容量 Watchlist

容量测试使用 200 只去重证券，覆盖全部已支持资产类型。若配置的 `max_watchlist_symbols` 小于 200，则使用配置上限，并另行验证第 `max + 1` 只会被拒绝。

## 5.4 日期集合

测试日期至少包含：

```text
最近20个已完成交易日
1个周末
1个法定节假日
1个新上市日
1个全天停牌日
1个临时停牌或分钟数量非240的样本日
1个北交所代码迁移边界日
1个指数正式发布日期之前的回溯历史日期
```

若最近20个交易日没有对应边界事件，可以从更早历史中补充，不替换最近20日回放。

## 5.5 Raw Fixture

CI 和离线回放使用脱敏、不可变的 Raw Fixture，至少覆盖：

```text
正常响应
空数组
部分证券缺失
重复行
乱序行
字段缺失
字段类型变化
HTML 响应
403 / 429 / 5xx
连接中断 / timeout
静默截断到 max_rows_per_request
累计成交量跨日重置
分钟迟到与重复快照
跨来源软冲突和硬冲突
```

Fixture 必须保存内容 hash、采集时间、来源、Endpoint、Capability 版本和期望解析结果。禁止在 Fixture 中保存口令、Cookie 或访问令牌。

---

# 6. 量化验收指标

| 指标 | M1 / M2 通过标准 |
|---|---|
| Security Master 状态覆盖率 | 100% |
| 北交所新旧代码映射 | 官方已公布映射 100% 可解析，非法重叠为 0 |
| 日线 Expected Item 状态覆盖率 | 100% |
| 正常交易项目的日线有效覆盖率 | 每日不低于 99.5%；这是持续运行验收目标，高于单次任务的最低发布门槛 |
| 未分类缺失 | 0 |
| Canonical 重复主键 | 0 |
| Raw 到 Canonical 可追溯率 | 100% |
| 正常全天交易分钟完整性 | Reconciliation 后 240/240；特殊状态按 Session 规则计算 |
| 沪深 1m Hot 时效 | 腾讯正常时，分钟闭合后 10 秒内可查，P95 达标 |
| 北交所 1m 时效 | TDX 正常时，分钟闭合后 20 分钟内可查，P95 达标 |
| 收到来源数据后的入 Hot 延迟 | P95 ≤ 2 秒 |
| 5m 派生正确率 | 对完整 1m 输入为 100% |
| 重跑幂等 | 相同输入连续运行 3 次，Canonical 内容 hash 不变 |
| 故障切源范围 | 只请求 Missing Set；已成功项目重复请求数为 0 |
| 故障识别 | 规定的故障 Fixture 识别率 100% |
| 断电恢复 RPO | 已成功 fsync 的 Raw 和已发布 Canonical 数据丢失为 0 |
| 恢复 RTO | 参考单机、200只 Watchlist 条件下 10 分钟内恢复调度 |
| 日线完成时限 | T 日数据最迟 T+1 08:30 完成 Reconciliation |
| 严重缺陷 | P0 / P1 未关闭数为 0 |

外部 Provider 自身延迟和系统内部延迟必须分别记录。北交所约 15 分钟来源延迟不能算作 SQLite 写入延迟。

---

# 7. 测试分层

## 7.1 L1：单元测试

覆盖纯逻辑和边界：

```text
代码与 instrument_id 映射
单位转换
时间戳转换
交易 Session 分钟归属
Missing Set
Resolution Policy
冲突阈值
row_hash
Manifest 生成和校验
状态机合法迁移
租约过期判断
1m → 5m 重采样
```

单元测试必须使用固定时钟和固定随机种子，不访问网络。

## 7.2 L2：Provider Contract 测试

每个 Endpoint 分别验证：

```text
市场与资产类型
代码格式
频率和复权方式
字段存在性和类型
最大返回条数
分页或不可分页行为
首尾时间
volume / amount 单位与累计方式
时间戳和集合竞价语义
freshness / finality
空返回语义
```

Contract 测试分为 Fixture Contract 和小流量 Live Probe。CI 默认运行 Fixture Contract；Live Probe 按计划或发布前运行，遵守限流配置。

Provider 能力验证与归一化规则的专项标准见：[Provider 能力验证与归一化设计](provider-capability-verification-and-normalization.md)。

每个 Endpoint 的验收必须同时检查：静态配置、适配器实现、Probe Evidence、能力覆盖范围和唯一 YAML 归一化规则。只有这些条件全部满足，才允许进入正式路由。

## 7.3 L3：组件集成测试

使用独立临时数据目录串联：

```text
Planner
→ Collector
→ Raw
→ Normalizer
→ Validator
→ Resolution
→ Canonical / Hot
→ Metadata
```

不得读写生产数据目录。每次测试完成后保留报告和 manifest，测试数据目录可以回收。

## 7.4 L4：历史回放测试

使用最近20个交易日和边界日期 Raw Fixture，模拟当时 Security Master、Trading Calendar、来源响应和调度时钟。

回放必须能够重复运行并得到相同 Canonical hash；只有显式启用新的 Resolution Policy 版本时，才允许结果变化。

## 7.5 L5：故障注入测试

在网络、解析、存储、发布和元数据提交环节注入故障，验证恢复行为和幂等性。

## 7.6 L6：实盘试运行

在不影响生产正式数据的隔离路径中连续运行至少5个真实交易日，验证实际限流、延迟、数据变更和盘后校准。

---

# 8. 功能验收用例

## 8.1 Security Master

| 编号 | 场景 | 通过标准 |
|---|---|---|
| SM-001 | 获取沪深股票、ETF、LOF、指数、北交所列表 | 各类资产均进入候选集，无整类静默缺失 |
| SM-002 | 单一来源当天少返回证券 | 保留前一日可信记录，标记来源缺失，不判定退市 |
| SM-003 | ETF/LOF 分类不一致 | 记录 `classification_conflict` 和双方证据 |
| SM-004 | 北交所新旧代码映射 | 映射到同一 `instrument_id`，历史按有效期拼接 |
| SM-005 | 指数存在正式发布日前历史 | 标记 `is_backfilled_history`，不伪造成当时已发布指数 |
| SM-006 | 新上市和退市证券 | 历史 Expected Set 按当日有效期生成 |

## 8.2 Trading Calendar

| 编号 | 场景 | 通过标准 |
|---|---|---|
| CAL-001 | 普通交易日 | 正确生成日线与分钟 Expected Set |
| CAL-002 | 周末和节假日 | 不创建错误 Missing Set |
| CAL-003 | 单一来源整日为空 | 不直接把交易日改成休市日 |
| CAL-004 | 日历来源不一致 | 保留证据并告警，交易所证据优先 |

## 8.3 日线

| 编号 | 场景 | 通过标准 |
|---|---|---|
| DAY-001 | 全 Universe 日线采集 | 能从目标证券和有效数据直接计算最终缺失清单，并按配置发布门槛判断 |
| DAY-002 | 腾讯近期窗口达到行数上限 | 标记截断，不能确认更早历史完成 |
| DAY-003 | 新浪北交所返回上市前新三板历史 | 按 BSE 上市有效期过滤并保留 Raw |
| DAY-004 | LOF 在 BaoStock 返回空 | 仍作为缺失证券，按规则切至兼容来源 |
| DAY-005 | 指数来源起始日期不同 | 保留回溯标记并执行冲突规则 |
| DAY-006 | 收盘快照生成日线 | 记录 `provisional`，后续历史行校准为 `final` |
| DAY-007 | 历史补采 | 只补具体日期和 Missing Items，不产生区间重叠文件 |
| DAY-008 | 单位转换 | 手/股、万元/元等转换与 Golden Record 完全一致 |
| DAY-009 | 东方财富批量查询分红实施事件 | 按日期范围分页取得全市场结果，不逐只查询证券，按证券身份和除权日去重 |
| DAY-010 | 分红事件接口单日失败 | 下一次任务通过补偿窗口重新取得遗漏事件，不影响普通日线任务 |
| DAY-011 | 目标前复权来源尚未更新 | 重叠历史没有变化，不创建可执行历史重建，不覆盖正式数据 |
| DAY-012 | 目标前复权来源已经换版 | 相同来源和参数下历史价格变化，经二次请求确认后进入人工重建清单 |
| DAY-013 | 历史覆盖开关关闭 | 拒绝创建历史重建任务 |
| DAY-014 | 指定证券前复权历史重建 | 整段历史优先来自同一来源，按日期生成候选分区 |
| DAY-015 | 多日期统一切换 | 所有候选分区完成前业务继续读取旧版本，完成后一次切换到新版本 |
| DAY-016 | 历史重建中断 | 当前版本保持不变，重启后继续候选构建或放弃候选，不出现部分日期切换 |

## 8.4 分钟与快照

| 编号 | 场景 | 通过标准 |
|---|---|---|
| MIN-001 | 腾讯沪深原生 1m | 分钟闭合后按 SLA 写入 Hot Store |
| MIN-002 | TDX 北交所延迟 1m | 来源可见后立即入 Hot，记录真实来源延迟 |
| MIN-003 | 正常全天交易 | Reconciliation 后 240 根、无重复分钟 |
| MIN-004 | 停牌或临停 | 按 Session 和交易状态计算 Expected Rows |
| MIN-005 | 09:31 首根集合竞价差异 | 不用 1m 聚合结果无条件覆盖官方日线 open |
| MIN-006 | Snapshot 聚合分钟 | 标记 `provisional` 和 `incomplete_sampling` 风险 |
| MIN-007 | 累计成交量跨日重置 | 不生成负成交量，异常进入隔离或重试 |
| MIN-008 | 1m 重采样 5m | OHLCV/amount 与 Golden Aggregation 完全一致 |
| MIN-009 | 原生 5m 与派生 5m 冲突 | 完整 1m 派生结果优先，差异写冲突日志 |
| MIN-010 | Watchlist 超过容量 | Planner 拒绝扩容并给出吞吐量原因 |

---

# 9. 来源失败与切换用例

| 编号 | 注入故障 | 预期行为 |
|---|---|---|
| SRC-001 | timeout / connection reset | 在 Endpoint 限额内重试，仍失败后只切 Missing Set |
| SRC-002 | 403 / 429 | 进入冷却，不由同日后续任务反复撞击 |
| SRC-003 | 返回 HTML 且状态码为 200 | 识别为协议失败，Raw 可留证但不得 Normalize |
| SRC-004 | 返回空数组 | 不能从缺失证券集合移除，继续下一来源 |
| SRC-005 | 只返回请求证券的一部分 | 已成功部分发布，剩余部分换源 |
| SRC-006 | Schema 字段改名或缺失 | Contract 失败并隔离，不生成全空 Canonical 列 |
| SRC-007 | 历史结果恰好达到行数上限 | 标记 `partial/truncated`，分页、拆窗或换源 |
| SRC-008 | EastMoney 持续断开 | 保持 disabled/cooldown，不影响新浪、腾讯任务 |
| SRC-009 | BaoStock 对不支持资产返回空 | 不提升为完成，不错误改变 Capability 覆盖范围 |
| SRC-010 | 来源冷却后恢复 | 先执行小流量 Probe，成功后才恢复候选资格 |
| SRC-011 | 备用源只有 5m | 不得用 5m 冒充 1m |
| SRC-012 | 备用源复权方式不同 | 拒绝静默切换，保持 missing/single_source |

切源测试必须额外断言：已经成功的证券没有被备用来源重新请求。

---

# 10. 重复、修订与冲突用例

| 编号 | 场景 | 通过标准 |
|---|---|---|
| RES-001 | 同一任务连续运行 3 次 | Raw 可增加，Canonical 主键数和内容 hash 不变 |
| RES-002 | 两个相同任务并发 | 只有一个 Publish，另一任务 No-op 或重新合并 |
| RES-003 | 同来源返回完全相同数据 | 根据 `row_hash` 识别为重复 |
| RES-004 | 同来源发布修订数据 | 新值通过校验后更新，旧 Raw 和修订记录保留 |
| RES-005 | 后续来源额外返回已有证券 | 丢弃额外记录，不覆盖前面优先来源已经取得的有效数据 |
| RES-006 | 全量来源返回当前不缺失的证券 | 只接收返回结果与当前缺失证券集合的交集 |
| RES-007 | 正式记录来源追溯 | 每条正式记录能反查来源、接口、抓取时间和原始响应引用 |
| RES-008 | 一个来源缺少必要字段 | 该记录校验失败并保持缺失，不从另一来源拼接单个字段 |
| RES-009 | 路由优先级调整 | 新任务按新优先级执行，已运行任务继续使用启动时冻结的配置 |

---

# 11. 断电与存储恢复用例

使用进程强制结束或故障代理模拟断电，不需要真的关闭测试机器。

| 编号 | 中断位置 | 重启后的通过标准 |
|---|---|---|
| REC-001 | 请求尚未完成 | 租约过期后重试，不产生成功记录 |
| REC-002 | Raw 写到一半 | hash 校验失败，文件隔离并重新请求 |
| REC-003 | Raw 已 fsync、尚未 Normalize | 复用 Raw 继续，不重新访问 Provider |
| REC-004 | Normalize 完成、尚未 Validate | 从最近合法状态继续，最终只发布一次 |
| REC-005 | 临时 Parquet 写到一半 | 正式查询不可见，临时文件进入 quarantine |
| REC-006 | Parquet 和 manifest 完整、尚未 replace | 复读校验后完成原子替换 |
| REC-007 | replace 完成、元数据未提交 | 根据 manifest 修复 DuckDB 元数据，不重新下载 |
| REC-008 | 元数据事务中断 | 回滚或启动修复后文件和元数据一致 |
| REC-009 | Publish Lock 持有进程退出 | stale lock/lease 可回收，其他任务可继续 |
| REC-010 | Hot Store 写入期间退出 | SQLite WAL 恢复后已提交分钟不丢失、不重复 |

每个恢复用例都必须在重启后重新执行以下断言：

```text
Canonical 主键重复数 = 0
正式文件 manifest 校验失败数 = 0
已 fsync Raw 丢失数 = 0
未分类 Missing Item 数 = 0
半成品对正式查询可见数 = 0
```

---

# 12. 性能与稳定性测试

## 12.1 基准条件

测试报告必须记录：

```text
CPU / 内存 / 磁盘类型
Python 与依赖版本
操作系统
测试 Universe 数量
Provider Endpoint 和 Capability 版本
网络环境
开始、结束时间
并发、QPS、timeout 和 retry 配置
```

不在文档中写死某台机器的吞吐量作为永久标准；以业务 SLA 是否满足作为最终判定。

## 12.2 必测性能场景

| 编号 | 场景 | 通过标准 |
|---|---|---|
| PERF-001 | 全 Universe 单日日线 | 在 T+1 08:30 前完成，且不突破各 Endpoint 限流 |
| PERF-002 | 最近20日 Reconciliation | 无内存失控、重复发布或不可解释缺口 |
| PERF-003 | 200只沪深混合 Watchlist | 腾讯正常时分钟时效 P95 ≤ 10 秒 |
| PERF-004 | 北交所延迟分钟增量 | 来源可见后入库 P95 ≤ 2 秒，总时效 P95 ≤ 20 分钟 |
| PERF-005 | Hot + Canonical 联合查询 | 查询结果连续、无重复，响应时间记录为基线 |
| PERF-006 | 连续5日运行 | 无持续增长的 stale lease、临时文件或未关闭数据库事务 |

查询响应时间第一版只记录基线，不作为 P0 阻断项；但正确性、分钟时效和任务完成时限是阻断项。

---

# 13. 可观测性与审计验收

任意 Canonical 记录必须能够反查：

```text
Provider / Endpoint
Capability 版本
collection_task / collection_attempt
Raw object path / content hash
采集时间
Normalizer / Resolution Policy 版本
quality_status / freshness_class / as_of
冲突记录和选值原因
```

系统必须能生成以下运行汇总：

```text
Expected / Published / No Trade / Missing / Conflict 数量
各 Provider 请求数、成功数、失败分类、429/403 数量
当前冷却能力及恢复时间
重试和 Fallback 数量
Raw 复用数量
重复记录拦截数量
各阶段耗时和分钟时效分位数
断电恢复处理数量
```

如果只能看到最终 Parquet，而无法解释数据来自哪里、为何被选择，则审计验收不通过。

---

# 14. 自动化测试运行策略

建议测试分组：

```text
unit                 # 每次提交
contract_fixture     # 每次提交
integration          # 每次提交
replay_20d           # 合并前或每日
fault_injection      # 合并前或每日
live_probe           # 发布前、定时低频
live_trading_day     # 真实交易日
soak_5d              # M2 验收
```

网络 Live Probe 不应成为普通单元测试的隐式依赖。网络不可用时，Fixture 测试仍应稳定运行；但 M1 发布前必须单独取得当前网络环境下的 Live Probe 报告。

所有测试使用隔离路径，例如：

```text
tmp/test-runs/{test_run_id}/data/raw
tmp/test-runs/{test_run_id}/data/hot
tmp/test-runs/{test_run_id}/data/canonical
tmp/test-runs/{test_run_id}/data/metadata
```

不得直接对生产 Canonical 执行破坏性故障注入。

---

# 15. 发布关卡

## Gate A：代码与静态契约

```text
单元测试 100% 通过
Schema / Dataset Contract 通过
关键模块静态检查通过
```

## Gate B：Provider 能力

```text
Sina / Tencent 主路径 Live Probe 通过
TDX 启用时，沪深与北交所分钟 Probe 通过；禁用时必须记录原因且调度不得触发
EastMoney / BaoStock 的 enabled/disabled 状态符合探针结果
 现有 Metadata 能力验证记录已保存验证时间和到期时间
```

## Gate C：端到端与历史回放

```text
全资产类型垂直链路通过
最近20个交易日回放通过
重复执行 Canonical hash 不变
未分类缺失为 0
```

## Gate D：故障与恢复

```text
SRC-001 ~ SRC-012 全部通过
REC-001 ~ REC-010 全部通过
RES-001 ~ RES-009 全部通过
```

## Gate E：容量与时效

```text
200只 Watchlist 容量测试通过
沪深与北交所各自时效 SLA 通过
日线在规定时限内闭环
```

Gate A～E 全部通过，即达到 M1“开发完成”。

## Gate F：连续试运行

```text
连续5个真实交易日
每日状态覆盖率 100%
无 P0 / P1 缺陷
无数据丢失、重复主键或未恢复半成品
所有日线和分钟 Reconciliation 在 SLA 内完成
```

Gate F 通过，即达到 M2“试运行验收”。

---

# 16. 缺陷分级

| 等级 | 定义 | 示例 | 验收要求 |
|---|---|---|---|
| P0 | 数据破坏、不可恢复或大范围错误发布 | Canonical 被清空、跨证券串数据、原子发布失效 | 必须为 0 |
| P1 | 核心范围无法闭环或结果不可信 | 整类证券缺失、重复主键、换源后伪完成 | 必须为 0 |
| P2 | 有替代路径但影响局部质量或运维 | 单个低优先级来源探针异常、非核心查询较慢 | 必须有负责人和修复计划 |
| P3 | 不影响正确性的体验或文档问题 | 日志措辞、非关键报表布局 | 可带入下一迭代 |

任何通过降低断言、删除失败样本或把 `missing` 改成 `no_trade` 的方式关闭缺陷，均不视为修复。

---

# 17. 测试报告最小内容

每次 M1/M2 验收报告至少包含：

```text
test_run_id
代码版本 / 配置版本
开始时间 / 结束时间
环境信息
Universe 与 Watchlist manifest
测试日期集合
Capability 版本及健康状态

用例总数 / 通过 / 失败 / 跳过
各 Dataset 覆盖率
未分类 Missing 清单
冲突和隔离清单
重复主键检查结果
分钟时效 P50 / P95 / Max
日线完成时间
故障注入和恢复证据
正式文件 manifest 校验结果

P0 / P1 / P2 / P3 缺陷清单
最终结论
```

跳过的 P0 用例按失败处理。因来源临时不可用而跳过 Live Probe 时，不能签署 M1 Provider Gate 通过。

---

# 18. 验收结论模板

```text
验收对象：Stock Data Platform Phase 1
代码版本：
配置版本：
测试报告编号：

Gate A 代码与静态契约：PASS / FAIL
Gate B Provider 能力：PASS / FAIL
Gate C 端到端与历史回放：PASS / FAIL
Gate D 故障与恢复：PASS / FAIL
Gate E 容量与时效：PASS / FAIL
Gate F 连续试运行：PASS / FAIL / NOT STARTED

未关闭 P0：
未关闭 P1：
已知 P2：

M1 开发完成结论：PASS / FAIL
M2 试运行结论：PASS / FAIL / NOT STARTED

验收人：
验收时间：
备注：
```

---

# 19. 最终完成定义

Phase 1 的最终完成标准为：

```text
全部核心资产类型能够采集
+ 每个 Expected Item 都有明确状态
+ Canonical 没有重复主键
+ 数据冲突不会被静默掩盖
+ 来源失败只补 Missing Set
+ 断电后能够从最近可靠状态继续
+ 任意正式数据能够追溯到 Raw 和采集尝试
+ 时效与容量满足本文件 SLA
+ M1 和 M2 发布关卡均通过
```

系统不需要保证第三方来源永不失败；系统必须保证第三方来源失败时，不会把失败误报成成功，也不会破坏已经发布的可信数据。
