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

成功输入的第一阶段契约见 [输入能力框架说明](docs/2026-10-03-input-capability-framework.md)，阅读清单位于 `coverage/successful-input-capabilities.csv`，证据关联位于同名 JSON。可用 `python provider_validation/tests/prepare_capability_results.py --input-catalog` 离线重建；该模式校验已存响应/派生表/源码哈希，不改写原覆盖表、不自动授予生产路由资格。

本次低频真实验证使用 `python provider_validation/tests/run_a_stock_rate_limited_probes.py --ids <显式编号列表>`。入口要求显式列出能力编号，不默认全跑；请求串行执行，同一主机最短间隔3秒，瞬时错误最多重试2次且至少退避5秒，不重试403/429；主机触发403/429或连续两次传输/服务器错误后，本轮暂停该主机。报表/PDF最多请求1页/1份。每批写入独立 `results/live-probes/<run-id>/`，原始HTTP返回在 `_raw/`，策略文件、摘要CSV和解析输出与原始证据同批保存。

BaoStock行业快照使用 `python provider_validation/tests/run_baostock_industry_live_probe.py --date YYYY-MM-DD --output results/live-probes/<unique-run-id>`。只执行证券清单与行业快照各一次，查询间最少等待3秒；持久化SDK解码字段和行（不包含SDK未暴露的TCP线缆帧），再保存派生交叉核验表。

部分上游函数会自行分页或展开全市场（例如上证互动、同花顺资金流）。限速只控制频率，不能代替范围控制；发现超出单样本验证范围时应安全停止并把已取得响应作为部分证据，不能把未完成结果标为通过。本次资金流备用接口在105页全市场分页中止于第16个HTTP响应，报告中标为部分；未生成全量解析结果。对包含会话/认证语义的请求头（如 `hexin-v`）只留脱敏标记，不留原值。
