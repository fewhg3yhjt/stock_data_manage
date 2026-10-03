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

旧版87项能力审计清单为 `coverage/a-stock-data-capability-inventory.csv`。逐接口完整报告为 `coverage/interface-coverage.csv`，阅读版工作簿为 `coverage/interface-coverage.xlsx`。完整报告有95行：87项上游能力、2条不计入分母的说明/参数变体，以及6条 `stock-data-analyse` 板块接口补充项。每行记录来源、能力声明、实测范围、代码/测试代码、验证结果、返回样例及原始响应/解析结果引用；证据状态代码旁附中文解释。阅读版另有“状态释义”工作表。字段口径、状态统计和证据限制见 [接口测试覆盖度表说明](docs/interface-coverage-method.md)。

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

探针结果说明见 [验证运行手册](docs/a-stock-data-validation-runbook.md)。逐接口结果另有 `results/interface-records/` 下的 JSON 记录及 `results/interface-coverage-summary.json` 总结。主表可由 `python provider_validation/tests/build_interface_coverage.py` 从已留存证据重建；该脚本不请求网络。
