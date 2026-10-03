# Provider 接口验证区

本目录只存放 Provider 接口调研与验证材料，不属于生产数据目录。验证结论必须能追溯到原始响应、解析输出或明确的失败记录；只有文档声明或 HTTP 成功状态不能标记为通过。

## 目录

| 路径 | 内容 |
|---|---|
| `tests/` | 接口探针、离线归档重放及证据整理脚本。项目原有 pytest 回归测试仍在仓库根目录 `tests/`，不移动到此处。 |
| `coverage/` | 当前接口覆盖度表及审计结果。CSV 是可检索的逐项清单；XLSX 是阅读版结果和原始返回索引。 |
| `docs/` | 验证方法、能力审计报告和使用说明。 |
| `results/` | 按批次保存的原始响应、解析结果、校验摘要和历史探针输出；不用于生产数据。 |

## 覆盖表字段和判定

覆盖度主表为 `coverage/a-stock-data-capability-inventory.csv`。每项至少包含能力项、验证结果、备注和原始返回文件，并保留审计来源字段及本轮解析/核验信息。判定值使用“通过 / 未通过”；没有可复查的本轮证据按未通过记录，备注中说明是接口失败、解析问题、范围有限还是尚未验证。详细证据路径指向本目录下的结果文件；外部项目生成的派生数据路径会明确保留为外部路径。

## 结果与原始响应

- 原始 HTTP 捕获器位于 `tests/raw_response_archive.py`，默认将每批数据写入 `results/raw/<run-id>/`。
- 每批清单 `manifest.ndjson` 与 `bodies/` 同目录保存。清单中的 `body_storage` 是相对路径；搬迁时保留了目录和文件名，原始压缩响应没有改写。
- 脚本、表格或报告的输出应写入 `coverage/` 或 `results/`，不得写入生产数据目录。
- 既有未按批次归档的历史探针输出放在 `results/legacy/`，并保留原文件名。

## 常用脚本

在项目根目录运行：

```powershell
python provider_validation/tests/replay_sector_capability_archives.py
```

该脚本只读取已保存证据并重建行业派生表，不请求网络。北交所历史覆盖结果可直接查看 `results/legacy/2026-10-01-security-board-coverage.json`；如需刷新，应显式传入 `--refresh`，新原始响应会进入 `results/raw/`。

探针结果说明见 [验证运行手册](docs/a-stock-data-validation-runbook.md)，逐项状态见 [覆盖度 CSV](coverage/a-stock-data-capability-inventory.csv) 和 [阅读版工作簿](coverage/a-stock-data-capability-results.xlsx)。
