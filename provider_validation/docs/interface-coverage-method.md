# 接口测试覆盖度表说明

## 范围与分母

阅读版和机器可读版均以 `coverage/interface-coverage.csv` 为逐接口主表。当前包含 95 行：`a-stock-data` 清单 87 项、2 条明确不计入分母的说明/参数变体，以及此前讨论的 `stock-data-analyse` 板块相关 6 个底层调用。板块补充项不加入 87 项分母，避免重复计数。

当前 87 项结论按证据口径统计：26 项“通过”（真实接口验证且原始响应已归档），42 项“部分通过”（补抓脚本得到非空解析输出，但该批没有保存精确原始响应，且只代表记录的样本范围），18 项“未通过”（失败、不可用或返回不完整），1 项“未验证”（需要凭据）。每行另有“本轮补抓脚本结果”，用于展示 61 条用户执行结果；它和原始响应审计结论分开，避免用解析后的 CSV 替代原始响应。

## 字段和证据读取

- `接口ID` 是稳定行标识；`是否计入87项` 用于筛选并复核分母。
- `类别`、`项目/来源`、`接口地址/协议`、`接口说明`、`接口内容/主要字段`、`调用方式/参数范围` 用于回答接口来自哪里、做什么、怎么调用及返回什么。
- `来源代码文件/行号`、`代码SHA-256` 指向本地留存的上游代码快照/实现；`测试代码文件/行号` 指向探针或测试快照。无法定位专属测试时在行内标明，不把文档说明当作已运行测试。
- `接口取数结果` 表示该能力在已记录范围内的验证结论；`本轮补抓脚本结果` 单独记录 61 项脚本的 success/failed/unavailable/credential 状态。
- `原始响应Manifest`、`原始响应文件/哈希` 指向精确 HTTP 响应归档。压缩 body 的 SHA-256 以解压后的原始响应字节计算；文件名也按该哈希命名。
- `解析结果/输出文件`、`结果文件SHA-256/行数`、`返回示例` 指向解析结果与样例。失败与未执行项也有单独 JSON 结果记录，说明证据缺口。
- `逐接口结果记录` 指向 `results/interface-records/<接口ID>.json`。每条记录包括范围、结论、代码/数据文件哈希、行数、原始响应关联和验证时间。

## 留存位置与重建

- 上游代码快照及 SHA-256 清单：`tests/source_snapshots/` 和 `tests/source_snapshots/manifest.json`。`a-stock-data` 固定于 `f814dcfe209dd7958f4858f9d878d591ee85fb56`；`stock-data-analyse` 固定于 `c26cabcf89443ec8f1445d0e5af86bf3d6dacf3`。
- 61 项手动补抓解析输出：`results/a-stock-data-missing-output/`；该批原始 HTTP body 未由上游脚本持久化，因此表中不会谎称这些结果有原始响应。
- 已捕获原始请求和响应：`results/raw/<run-id>/manifest.ndjson` 与 `bodies/`。
- 派生验证数据和旧版板块结果：`results/` 下对应日期目录及 `results/legacy/`。
- 逐行记录：`results/interface-records/`；总体核验摘要：`results/interface-coverage-summary.json`。
- 重新生成 CSV 和逐接口 JSON：在项目根目录执行 `python provider_validation/tests/build_interface_coverage.py`。此脚本只读取本地证据，不发起网络请求。

## 结果解释边界

“通过”仅表示表中明确的接口、参数、证券/日期范围和语义检查通过，不表示全市场、长期稳定性或正式 Provider 路由已获准。BaoStock 两项板块调用目前只有历史合并后的派生数据，SDK 不暴露原始 TCP 帧，本项目当前 Provider 也没有以相同日期参数完成新的独立全量 Live Probe；因此列为“部分通过”。THS 资金流是快照，不等同于证券-概念成分关系。缺原始数据、字段单位不明、范围不足和网络/凭据阻断都在各自行内备注。
