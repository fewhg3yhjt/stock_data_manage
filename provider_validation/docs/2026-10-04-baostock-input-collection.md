# BaoStock 两项既有接口的输入转换

日期：2026-10-04。范围为 SDA-BOARD-005 证券快照、SDA-BOARD-006 证监会行业归属，继续复用 `providers/baostock/industry.py` 中同一个 `BaoStockIndustryMembershipProvider.fetch_snapshot`。未新增 Provider 类、配置系统、证券主数据更新服务或生产入口。

## 两种输出与覆盖边界

| 输入 | SDK 原始解码行数 | 候选行数 | 覆盖分母与缺失 |
|---|---|---|---|
| SDA-BOARD-005 证券状态快照 | 证券查询7423行；行业查询5556行 | 5223 | 原有代码规则筛选的沪深A股5223只；证券快照没有遗漏，行业字段仍缺2只 |
| SDA-BOARD-006 行业归属快照 | 同上 | 5221 | 分母5223，缺少sz001246、sz301716；行业归属覆盖不完整 |

上述范围为 2026-09-30 查询、2026-10-03 采集的原归档。它不包含北交所，分母来自该 SDK 结果的原有代码筛选，不是独立的全市场证券普查。`coverage_complete` 只描述当前输入目标在该分母内的覆盖；证券快照的 `missing_industry_symbols` 和行业归属的 `missing_symbols` 分别明确报告。

默认旧方法继续返回原行业归属字段、行数、请求证券和缺失证券。通用工厂为两个输入配置不同的既有 `endpoint`，同一方法分别提取证券快照或行业归属输出。证券快照不因缺少行业而丢弃证券。两者仍执行原来的证券、行业两次查询，不另建查询实现。

## 参数和 YAML 的实际作用

- `providers.yaml`：绑定 `trade_date` 至原方法；SDK 内部仍使用 `query_all_stock(day=...)`、`query_stock_industry(date=...)`。日期必须在显式正向交易日历中。`request.symbols` 是可选的本地输出筛选，SDK 仍返回完整查询范围；单个 `request.symbol` 被拒绝，避免参数被忽略后意外收集整表。
- `datasets/security_snapshot.yaml`：新增确实缺失的证券状态快照字段模板，主键含查询日期、交易所和代码。既有 `industry_membership.yaml` 继续使用，仅补充来源分类更新时间说明。
- `normalization/`：补两个同名来源映射文件，直接映射原 SDK 名称、行业分类和更新时间列；代码、交易所和状态沿用原来源解析规则。名称及交易状态来自证券查询，行业属性来自行业查询。
- `collection.yaml`：继续使用已有 `reference_daily`，自动调度保持关闭。SDK 查询边界最少间隔3秒、并发1，保留原探针在第一份完整解码结果落盘后到第二次查询之间的间隔。SDK 内部物理报文次数不可见，不声明逐报文限速。

`classification_update_date` 使用 SDK 的 `updateDate`，原样本为2026-09-28，不使用查询日期2026-09-30替代。行业模板既有主键 `[stock_code, classification_update_date]` 保留；本批按运行分别保存候选文件。将来连续快照正式入库时仍须明确查询日期分区和主键用途，不能把当前候选写文件等同于生产历史表设计已完成。

## SDK 留证与回放

沿用原 Provider 的解码结果归档边界，在字段映射、目标证券过滤、行业合并前保存 SDK 可见的字段和行。精确保留的是解码载荷的归档表示，TCP 原始帧不可见，不能宣称已捕获服务端传输原字节。

现有 `providers/baostock/session.py` 补充解码载荷回放、匹配缓存复用、SDK 查询限速和会话留证。缓存查找在登录和查询前完成。在线路径仍调用原 SDK 的 `login/query_all_stock/query_stock_industry/logout`，参数、默认登录、传输和超时行为不替换；会话串行保护覆盖登录至登出，防止共享 SDK 会话被并发登录替换。SDK 源码快照中默认登录值脱敏，保留原源码哈希。

回放/缓存仅复现外部 SDK 必需的 `fields/error_code/next/get_row_data` 结果接口。这是已归档 SDK 合同的离线复现，不是平行 Provider 或在线兼容入口；缺失匹配、校验失败不会联网回退。原载荷先验证哈希并保存，随后解析；派生 SDK 行另外保存并关联来源哈希。查询失败、空返回、登录/登出状态、SDK 错误与覆盖缺失分别记录。SDK 成功但没有数据不等于合法空数据集。

## 使用方式

沿用 `collect-input --context-file`。上下文示例：

```json
{"request":{"trade_date":"2026-09-30"},"calendar":{"trading_dates":["2026-09-30"]}}
```

按范围筛选候选行时，可在 `request` 中加入 `"symbols":["sh600519","sz001246"]`。这不会缩小 SDK 的查询范围。

```powershell
$env:PYTHONPATH = 'src'
python -m stock_data_manage.cli collect-input `
  --input SDA-BOARD-005 --context-file <上下文文件路径> `
  --mode replay `
  --replay-manifest provider_validation/results/live-probes/baostock-industry-20260930-20261003/_raw/baostock-industry/manifest.ndjson `
  --output-root provider_validation/results/my-bao-input
```

行业归属改用 `--input SDA-BOARD-006`。每次创建独立候选目录，保留 SDK 解码载荷、来源行、标准行、缺失与覆盖报告，不更新生产目录或证券主数据。

## 验证与未完成项

修改前 Provider/会话源码及原 SDK 归档核对记录位于 `results/bao-original-20261004/`。最终离线证据为 `results/bao-final-20261004/comparison.json`，测试及总索引为同前缀 `tests.xml`、`verification.json`；实际 CLI 参数入口证据位于 `results/bao-cli-20261004/`。可通过原 `replay_input_capabilities.py --verify-bao-inputs --output-root <新目录>` 重现。

验证包括全部旧行业字段、5223只证券范围、原查询参数/SDK状态/载荷哈希、原归档CSV、YAML字段修改、本地筛选、覆盖缺失、错误/空返回、严格回放、源码脱敏、会话串行、落盘后间隔及缓存前置复用。网络测试允许 Windows 事件循环必需的本地回环通信，禁止外部来源联网；间隔/缓存的在线分支只使用注入 SDK 测试夹具，不属于新增实时探针。

开发中一次完整回归发现旧测试仍期望尚未绑定运行时的 `day` 参数，且联网拦截误阻断 Windows 本地事件循环；失败记录保存于 `bao-dev-20261004-full-tests.xml`，修正后独立复测，不改写失败证据。

本批未新增全市场实时查询，未认证当前在线容量或源接受度，未启用自动调度、正式路由或生产发布。腾讯快照、东财各项和同花顺重复输入契约归并均留给后续已确认批次。
