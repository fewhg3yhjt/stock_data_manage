# Provider 接口验证区

本目录用于正式功能实现前确认来源接口可行性，保存验证脚本、接口原始返回、解析证据和可用性结论。验证结论必须能追溯到原始响应、解析输出或明确的失败记录；只有文档声明或 HTTP 成功状态不能标记为通过。

## 与正式开发的边界

某接口进入正式业务开发后，其真正业务实现位于 `src/`，实现测试位于根目录 `tests/`，正式文档位于 `docs/`。业务采集、Provider 联调、质量检查、任务执行及端到端验收新产物均走正式存储流程：真实业务运行位于 `data/`，隔离回放与故障测试位于 `tmp/` 下的独立运行根目录。正式开发中的原始返回也遵守这一分工，不因名称带“探针”或“资格验证”写回本目录。

已有证据可被正式代码或测试只读引用。新增接口在正式功能实现前可以继续在本目录验证可行性。历史文件保留原路径、字节和哈希，整理或删除需另列清单确认。

下文已有运行命令和开发记录属于历史验证方法；进入正式开发的接口不再以这些输出目录作为新增业务验收位置。历史 `actions-cli-20261004` 与 `actions-cli-v2-20261004` 是同一实现的两次离线回放记录，后一次源于源码换行符及字节哈希复核；它们不是正式代码版本或业务数据分区。其模式、零网络请求和零生产写入记录见 [首轮记录](results/actions-cli-20261004/verification.json)、[复核记录](results/actions-cli-v2-20261004/verification.json)。

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
- 实现前的接口验证脚本、表格或报告输出写入 `coverage/` 或 `results/`；进入正式开发后的实现测试与业务验收按上面的目录边界执行。
- 既有未按批次归档的历史探针输出放在 `results/legacy/`，并保留原文件名。

## 常用脚本

先在使用的 Python 环境中执行 `python -m pip install -e .` 安装当前项目，再在项目根目录运行。正式代码直接位于 `src/`，验证脚本通过已安装的 `stock_data_manage` 包导入，不再自行添加源码目录。历史证据中的旧源码路径及哈希保留原样。

```powershell
python provider_validation/tests/replay_sector_capability_archives.py
```

该脚本只读取已保存证据并重建行业派生表，不请求网络。北交所历史覆盖结果可直接查看 `results/legacy/2026-10-01-security-board-coverage.json`；如需刷新，应显式传入 `--refresh`，新原始响应会进入 `results/raw/`。

探针结果说明见 [验证运行手册](docs/a-stock-data-validation-runbook.md)。逐接口结果另有 `results/interface-records/` 下的 JSON 记录及 `results/interface-coverage-summary.json` 总结。主表可由 `python provider_validation/tests/build_interface_coverage.py` 从已留存证据重建；该脚本不请求网络。

成功输入的第一阶段契约见 [输入能力框架说明](docs/2026-10-03-input-capability-framework.md)，阅读清单位于 `coverage/successful-input-capabilities.csv`，证据关联位于同名 JSON。可用 `python provider_validation/tests/prepare_capability_results.py --input-catalog` 离线重建；该模式校验已存响应/派生表/源码哈希，不改写原覆盖表、不自动授予生产路由资格。

四项输入已经通过现有配置与 Provider 执行字段模板和映射，入口为 `stock-data collect-input`。参数、示例与待验证边界见 [YAML 输入采集实现说明](docs/2026-10-03-yaml-input-collection.md)；[最终离线比较](results/input-verified-20261003/comparison.json)、[在线摘要](results/input-live-20261003/summary.json)及[回归测试结果](results/2026-10-03-yaml-input-collection-tests.xml)均已保存。可用 `python provider_validation/tests/replay_input_capabilities.py --output-root provider_validation/results/<新目录>` 离线重建候选与比较证据。

本次实现状态清单位于 `coverage/successful-input-capabilities-20261003-implemented.csv` 与同名 JSON。第一阶段同名无日期清单保留；生成独立版本可使用 `prepare_capability_results.py --input-catalog --catalog-name <新文件名>`。

本次低频真实验证使用 `python provider_validation/tests/run_a_stock_rate_limited_probes.py --ids <显式编号列表>`。入口要求显式列出能力编号，不默认全跑；请求串行执行，同一主机最短间隔3秒，瞬时错误最多重试2次且至少退避5秒，不重试403/429；主机触发403/429或连续两次传输/服务器错误后，本轮暂停该主机。报表/PDF最多请求1页/1份。每批写入独立 `results/live-probes/<run-id>/`，原始HTTP返回在 `_raw/`，策略文件、摘要CSV和解析输出与原始证据同批保存。

BaoStock行业快照使用 `python provider_validation/tests/run_baostock_industry_live_probe.py --date YYYY-MM-DD --output results/live-probes/<unique-run-id>`。只执行证券清单与行业快照各一次，查询间最少等待3秒；持久化SDK解码字段和行（不包含SDK未暴露的TCP线缆帧），再保存派生交叉核验表。

部分上游函数会自行分页或展开全市场（例如上证互动、同花顺资金流）。限速只控制频率，不能代替范围控制；发现超出单样本验证范围时应安全停止并把已取得响应作为部分证据，不能把未完成结果标为通过。本次资金流备用接口在105页全市场分页中止于第16个HTTP响应，报告中标为部分；未生成全量解析结果。对包含会话/认证语义的请求头（如 `hexin-v`）只留脱敏标记，不留原值。
