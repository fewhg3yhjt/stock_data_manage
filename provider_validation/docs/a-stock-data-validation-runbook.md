# a-stock-data 全能力验证阶段记录

## 基线

- 来源：`D:\Project\PythonProgram\external\a-stock-data`，分支 `main`，本地提交 `f814dcfe209dd7958f4858f9d878d591ee85fb56`。
- README 声明 82 个主入口和 5 个备胎；按文档计数规则，行业研报与个股研报合并、北向历史作为本地缓存、异动池的 list/count 分拆后为 87 个端点能力。
- [能力清单](../coverage/a-stock-data-capability-inventory.csv) 取自 README 的完整能力表，记录了原始行与计数规则；当前审计状态均为 `not_audited`，避免把静态声明误当运行结果。

## 留存位置和规则

- 每轮的能力矩阵、总结和校验发现保存在本目录。
- 新的在线 HTTP 原始响应保存在 `../results/raw/<run-id>/`：`manifest.ndjson` 记录来源、端点、脱敏请求、范围、时间、状态、长度、SHA-256；`bodies/<sha256>.body.gz` 保存解压后可还原的应用层响应字节。
- 报文包含密钥时不保存密钥或原文，manifest 记录哈希并标注 `suppressed_sensitive_content`。
- 非 HTTP SDK/二进制客户端在其传输边界保留 SDK 返回的源字段与原始行，并说明它不是线缆层字节。
- 每次发出新请求前先按来源、端点、规范化参数、数据范围和日期查询旧证据；不为补齐报告而重复调用。

## 已完成阶段

1. 静态清单：完成 README、SKILL、验证文档和测试入口审阅；已生成 87 项端点计数能力清单（另保留 2 个不计数的缓存/同端点行作说明）。
2. 离线合同测试：在上述提交执行 `python -m unittest discover -s tests -v`，168 项中 165 项通过，3 项显式联网测试跳过，失败 0、错误 0。结构化结果见 [离线测试证据](../results/legacy/a-stock-data-offline-test-evidence.json)，完整日志见 [stdout](../results/legacy/a-stock-data-offline-tests.stdout.log) 和 [stderr](../results/legacy/a-stock-data-offline-tests.stderr.log)。
3. 原始响应归档器：HTTP 捕获器通过合成响应自检；验证了响应压缩保存与字节还原、请求头/参数密钥脱敏、含密钥响应抑制、调用方仍收到原响应、严格同范围新鲜缓存命中。自检未发网络请求，记录见 [归档器自检](../results/legacy/raw-response-archive-smoke.json)。
4. 既有北交所覆盖探针：历史 JSON 是解析后结果，未保存原始 HTTP 报文；不为补 raw 文件重跑接口。详情见 [原覆盖结果](../results/legacy/2026-10-01-security-board-coverage.json)。

## 尚未完成

- 当前尚有 51 项仅有文档声明，没有本轮实时响应证据；另有 7 项只有历史实测记录但没有原始报文。见 [逐项矩阵](../coverage/a-stock-data-capability-inventory.csv) 与 [本轮审计报告](2026-10-01-a-stock-data-live-audit.md)。
- V3.9 的上证 e 互动在真实页面中遇到未支持的相对时间格式“今天 18:18”；原始页面已归档，不应为复现而重复请求。若修复解析器，应使用归档字节做回归。
- 本轮状态词：`verified_live_raw_saved` 表示本轮数据调用与返回结构检查通过且原文归档；`partial_live_format_error_raw_saved` 表示请求有响应但解析/契约失败；`historical_live_raw_missing` 表示文档记有实测但原始报文不在；`historical_or_documented_only` 不代表当前可用；`documented_unavailable_or_partial`、`key_required_not_live_verified`、`local_cache_unverified` 分别表示已知失效/部分能力、缺凭据、或本地缓存行为尚未验证。
- 后续补测旧能力前，先查本目录历史存档并核对端点、参数、证券、日期、市场范围和新鲜度；只能对缺少相同范围证据的项目请求一次，且必须经 `run_archived_live_tests.py` 或等价捕获器先保存原文。不要为了填满矩阵而重复历史接口调用。
