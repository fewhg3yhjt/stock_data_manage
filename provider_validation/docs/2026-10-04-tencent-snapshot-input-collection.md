# 腾讯行情快照转换实施与验证

## 本批结果

`ASTOCK-001` 已从迁移待完成改为仅候选验证实现，复用 `providers/tencent/snapshot.py` 的原批量 Provider 和 `fetch_snapshot` 方法，没有新增 Provider、采集服务或配置层。前复权日线和最近5分钟线沿用既有实现，本批一并回归；1分钟没有独立成功输入合同，继续未启用。

证券范围由明确传入的沪深北股票代码清单决定，每100只分批、遍历完整清单，不自动截取100只或200只。单次手工候选采集不会自动发现全市场；现有周期规划器仍从本地证券清单选择当日股票，不把基金、指数或未上市证券混入。清单完整性、实时批量覆盖和持续容量仍未认证。

真实来源证据只有 `sh600519`：2026-10-03保存的响应中，报价时间为2026-09-30 16:14:58，原应用响应550字节，SHA-256为 `6ed41c4189fef3f0c6daa5cccf77084b2920ba95e4df716d284aa74821231695`。它不证明2026-10-03的实时行情。原清单位于 `results/live-probes/pilot-20261003-tencent-quote-network/_raw/missing-capabilities-20261003T173649/manifest.ndjson`，解析结果为同批次 `01_腾讯财经/data.csv`。

## 参数、字段和频率

继续使用现有三类配置：

| 文件 | 本批作用 |
|---|---|
| `config/providers.yaml` | `symbols` 来自 `request.symbols`，`as_of` 来自带时区的 `request.as_of`；并发1、请求间隔至少3秒 |
| `config/datasets/realtime_quote.yaml` | 复用已有行情字段和主键 `[instrument_id, quote_time]`，未重新创建数据集 |
| `config/normalization/realtime_quote.yaml` | 增加腾讯输入的来源字段映射；东财原规则保持原样 |
| `config/collection.yaml` | 快照沿用交易日15:10、每天一次、全股票清单策略；天/分钟及间隔可配置，开关仍关闭 |

代码、名称、原价格/涨跌/换手等未映射的数组位置与完整 `raw` 载荷都保留在来源数据。标准价格、开高低、昨收、涨跌额、涨跌幅通过YAML执行映射，报价时间按原14位时间戳解释为北京时间。原数组第37位金额和 `volume_lot` 均保留；标准 `volume`、`amount` 由于单位未独立核验而置空，未映射的估值和市值字段也不编造值。

`as_of` 只要求返回报价日期与其北京时间日期一致，不是上游日期参数，也不把返回时间改成调度时刻。不能用它查询历史快照；归档回放才可重现历史返回。禁止忽略单证券 `request.symbol`、历史起止日期或 `trade_date` 参数。股票列表要求显式 `sh`/`sz`/`bj` 前缀，不通过宽松代码提取偷偷扩大范围。

## 保留原成功请求行为

候选工厂在现有 `RequestsTransport` 内使用延迟建立并复用的 `requests.Session`。快照保留原脚本Windows浏览器UA、Accept、Accept-Language、20秒超时、重定向及环境代理；原成功脚本没有Referer，本批也不添加。会话在候选完成或失败后关闭。既有日线/5分钟线仍使用原独立请求方式及自己的请求头/主机回退。

快照按实际成功低频探针入口的策略执行：连接/读取/状态重试上限2，失败后最少退避5秒，403/429不重试，500/502/503/504可重试，尊重Retry-After。现有捕获机制在解析前保存精确应用响应字节，保留请求配置和源清单引用；回放严格匹配URL与方法，失败不回退联网。符合输入代码版本、范围及新鲜度的缓存可复用。内部物理重试仍不可逐次观察，报告声明为调用边界限速，不声称认证了物理请求容量。

本批增加重复证券、返回代码与请求代码不一致、意外证券、格式变化、缺失时间、无有效报价和日期不匹配检查。部分有数据的响应可生成候选，但覆盖分母采用请求证券数，列出缺失证券；全部无有效报价归为暂时空响应。HTTP和连接错误仍保留原请求异常与状态证据，不因迁移错误判定源不可用。

## 使用和证据

通过既有命令行的 `--context-file` 提供股票列表和时间，例如：

```json
{"request":{"symbols":["sh600519"],"as_of":"2026-09-30T15:10:00+08:00"}}
```

```powershell
$env:PYTHONPATH = "src" # 直接从源码仓库运行时；已安装stock-data命令时无需此项
python -m stock_data_manage.cli collect-input --input ASTOCK-001 --context-file path/to/context.json --mode replay --replay-manifest provider_validation/results/live-probes/pilot-20261003-tencent-quote-network/_raw/missing-capabilities-20261003T173649/manifest.ndjson --output-root provider_validation/results/my-quote-replay
```

候选输出位于显式结果目录，不能写入生产存储路径。`--fields` 可选择模板字段，必须包含主键所需字段。报告保存来源行、映射行、原始响应哈希、代码/配置版本、返回时间窗口、请求分母和覆盖状态。

持久化验证入口：

```powershell
python provider_validation/tests/replay_input_capabilities.py --verify-tencent-snapshot --output-root provider_validation/results/new-quote-verification
```

原实现及调查清单保存在 `results/tencent-original-20261004/`，开发过程和失败记录保留在 `tencent-dev-20261004/` 及同前缀XML。最终回放位于 `results/tencent-final-20261004-v2/`，三项分别输出快照1、日线14、5分钟线96行；总索引为 `results/tencent-final-20261004-v2-verification.json`，完整回归为同前缀测试XML，真实命令行候选证据在 `results/tencent-cli-20261004/`。初版 `tencent-final-20261004/` 及索引保留，第二版仅纠正合成部分缺失夹具从父样本继承的字节长度，实际响应、Provider代码与输出语义未改变；父版本测试源码按原完整哈希恢复并保存，便于版本对照。

原快照脚本执行、原CSV和旧Provider返回逐字段对照，URL、头、20秒超时、代理/重定向配置、状态和原响应哈希相同。203只沪深北证券分为100/100/3批属于合成离线夹具，名称和价格借用单样本，只验证遍历、标识和映射执行，不证明这些证券的真实报价。另验证1条返回/2只请求的部分覆盖和11类失败。注入Session夹具验证同会话复用、3秒间隔、保守重试配置及缓存，未验证当前网络接受度或实际重试流量。

早期失败来自旧适配器及端到端测试夹具没有填写载荷证券代码，以及完整性失败报告未初始化请求计数；夹具补齐代码、报告计数修复后通过。原失败XML保留，不当作来源失败。命令行首次运行时源码目录未进入Python搜索路径，错误记录保留，设置源码路径后通过。Windows异步库需要回环套接字，离线保护允许回环而阻止外部连接。

## 尚未启用

自动快照采集继续关闭。即使修改开关，现有规划器仍以 `snapshot_bulk_live_validation_pending` 阻断快照；候选迁移完成不等于全市场容量认证。本批没有新增实时请求、后台服务、管理台、历史补采或分布覆盖，也未开放正式路由和生产发布。后续启用需在明确证券范围内完成小样本在线批量对照、单位语义及端到端生产验证。
