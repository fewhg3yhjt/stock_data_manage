# 工程决策与被替代合同

本文供维护者追溯重要取舍，只记录对当前设计有影响的决策，不作为运行状态、来源资格或测试结果的第二份事实来源。当前规则回到各主要设计，当前任务回到[项目进度](../PROJECT_PROGRESS.md)，原始响应仍在原位置。

| 决策日期 | 原合同或问题 | 已确认的选择 | 影响与现行维护位置 |
|---|---|---|---|
| 2026-10-09至11日 | 验证区与业务运行混用、旧目录过深 | 正式业务代码/测试/说明归src、tests、docs，运行归data或隔离tmp；按数据日期管理活动raw | [存储说明](storage/README.md)维护现行布局；历史目录和哈希不改写 |
| 2026-10-28 | 解析质量结果反向驱动补数，职责混杂 | 来源策略只处理允许的采集故障；后置质量问题不自动发起本轮请求 | [总体设计](../STOCK_ANALYSIS_V2_MARKET_DATA_CENTER_ARCHITECTURE_V1.md)维护阶段边界；其他独立组件按自身合同验证 |
| 2026-10-28 | 三种复权作为独立最终记录 | 股票同证券同日期保存一条完整三口径记录，候选仍带口径 | [日线合同](../stock-data-design-qfq-history-rebuild.md)；旧单口径记录保留，不自动迁移 |
| 2026-10-29 | kline行数与北交所历史目标日合同不满足任务预期 | 股票daily_fixed三口径使用newfqkline；不复权显式none | [来源说明](providers/README.md)；保留640条窗口边界，不据样本授予市场资格 |
| 2026-10-30 | 逐证券逐日期单位白名单难以维护 | 按来源、市场、显式类型、代码段和实际序列匹配单位合同 | [日线单位合同](../stock-data-design-qfq-history-rebuild.md#22-来源单位合同2026-10-30)；未覆盖范围保持空值，不推断单位 |
| 2026-10-30 | 文档现状与历史混排，组件自检被读成项目验收 | 当前任务、业务合同、历史决策分开；组件只报告实际范围 | [验收基线](../stock-data-test-design-acceptance-baseline.md#工程检查与阶段验收的职责合同)；M1/M2总要求没有缩减 |
| 2026-10-30后续指令 | 源码指纹将无关模块、资格和缓存耦合 | 删除项目与SDK源码指纹生成及运行依赖，按实际影响分层测试；助手示例不能归为用户原话 | [版本与验证合同](../provider-capability-verification-and-normalization.md#实现版本记录与验证范围)；历史证据、数据完整性与冻结配置保留 |

## 关键原始证据索引

| 主题 | 证据及验证边界 |
|---|---|
| 历史证券名单发布 | [正式验收](../data/task_workspace/_checks/security-release-20261012/verification.json)，目标日2026-10-09，7267条；不证明任意历史日全市场覆盖 |
| 后复权接口差异 | [扩样核验](../data/task_workspace/_checks/tencent-daily-hfq-expanded-2026-10-28/README.md)，两位/三位小数差异发生于源响应，不认定算法等价或哪组更准确 |
| kline历史目标与v6阻断 | [v6报告](../data/task_workspace/_checks/tencent-daily-v6-qualification-2026-10-29/README.md)，沪深1025行与北交所目标日缺失；不能推断所有时点永久不可用 |
| v7指定不复权接口对照 | [请求对照](../data/task_workspace/_checks/tencent-unadjusted-route-parity-2026-10-29/README.md)，三只证券、一个目标日，不授权全市场 |
| v7三股正式留存发布 | [正式验收](../data/task_workspace/_checks/tencent-v7-formal-three-stock-2026-10-28/README.md)，3条记录、9组口径，量额当时为空，仅精确留存范围 |
| v8单位规则 | [留存验收](../data/task_workspace/_checks/unit-contracts-20261030/README.md)，隔离回放，旧正式文件和资格不自动更新 |
| 日线进度持久化优化 | [性能对照](../data/task_workspace/_checks/daily-progress-opt-20261013/performance-comparison.json)，只证明报告列出的样本、写入次数及耗时，不外推全市场吞吐 |
| 工程问题复核 | [原审查](../tmp/engineering-review-20261030/report.json)，基线05bb3ae；后续实现须用新证据，不把旧审查改写为新通过 |

## 历史内容承接与不变项

总体设计V1.3与原进度完整正文保存在Git提交 `78208bc`，可直接查看[原总体设计](https://github.com/fewhg3yhjt/stock_data_manage/blob/78208bc/STOCK_ANALYSIS_V2_MARKET_DATA_CENTER_ARCHITECTURE_V1.md)和[原进度](https://github.com/fewhg3yhjt/stock_data_manage/blob/78208bc/PROJECT_PROGRESS.md)。本次迁移另在本机 `tmp/engineering-remediation-20261030/before/` 保存逐字节原文及承接记录；这些是审查产物，不是长期规则或生产数据。当前状态回到进度，详细存储与分钟约束回到模块文档，重要取舍由本页维护。

旧单口径任务JSON、按口径分区示例、质量触发自动补缺图、旧目录建议和旧技术栈示意退出现行操作说明；原文仍可从上述Git版本复查。不是删除旧任务、行情文件、来源报告或接口响应，也不是修改既有历史记录。

不在本文逐批追加测试数量或维护当前来源资格。新增能力、资格、全量重采、调度启用和历史重建仍需明确范围及独立验收。
