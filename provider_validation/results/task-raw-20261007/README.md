# 原始暂存与任务重做验收

本目录保存既有来源证据的离线端到端回放和任务恢复测试结果。正式功能说明见 [任务流程](../../../docs/pipeline/collection-tasks.md)，运行代码仍在 `src/`。`data-proof/` 是验证隔离数据根目录，不是项目生产 `data/`。

完整回归754项通过，失败、错误、跳过均为0；其中17项为本轮新增流程测试。独立核验两份发布清单、数据文件和原始响应哈希，业务主键无重复。重复日线任务返回 `no_op: true`，恢复命令未发现损坏分区，分别保存在 `daily-repeat.json` 和 `recovery-result.json`。

| 证据 | 内容 |
|---|---|
| `master-task.json`、`master-result.json` | 2026-09-30 BaoStock 沪深股票名单，发布5,223条；没有独立全市场总数基准 |
| `daily-task.json`、`daily-result.json` | 腾讯 sh600519，来源请求2026-09-01至18，来源标准文件保留窗口全部记录，发布2026-09-18一条 |
| `data-proof/raw/` | 完整响应及请求清单，包含原证据位置和响应哈希 |
| `data-proof/task_workspace/` | 原解析、字段映射、标准来源文件、配置和代码版本、质量报告及待发布构建文件 |
| `data-proof/canonical/` | 验证发布结果与数据哈希清单 |
| `data-proof/task_archive/_raw_evidence/` | 正式记录可稳定追溯的响应证据 |
| `data-proof/metadata/metadata.duckdb` | 任务和单元状态、发布位置、最终原始响应位置及提交记录 |
| `regression.xml` | 项目完整离线回归结果，恢复故障使用人工夹具，不能替代在线验证 |
| `verification.json` | 验证范围、时间、原始响应哈希、转换版本、源标准行数、发布行数、覆盖依据与产物索引 |
| `verify.py` | 对持久化产物、原始响应和完整回归报告独立核验，重新生成结论；从项目根目录运行 |
| `source-line-endings.json`、`line-ending-check.xml` | 两份源码固定LF前后语法树相同，随后16项任务流程复验通过 |

原始 BaoStock 证据为 [既有 SDK 探针](../live-probes/baostock-industry-20260930-20261003/_raw/baostock-industry/manifest.ndjson)，证据只涵盖SDK可见的解码字段和行，不声称保留其底层TCP帧。腾讯证据为 [既有HTTP响应](../raw/2026-10-01-v39-live-escalated/manifest.ndjson)，严格匹配原请求，没有为新任务伪造日期窗口。

本次没有网络请求，没有生产目录写入，没有授予来源路由资格，没有验证ETF、北交所完整名单或全市场日线。人工故障用例覆盖全量重做、断点重做、指定证券重做、原始损坏、业务有效性失败、覆盖不足、发布过程四个位置中断，以及实际进程退出后锁回收。

复现操作见正式说明的“操作方式”。原始证据和生成记录不依赖终端输出；再次回放已发布同一任务会先验证并返回，不重新请求来源。
