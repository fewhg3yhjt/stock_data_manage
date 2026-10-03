# YAML 驱动输入采集：实际实现与验证

本次在现有 Provider、配置加载、归一化、工厂、原始存储和命令行模块中补齐输入执行能力。实现范围是已经确认的四项输入；调度覆盖范围及生产发布另行确定。

## 现状与复用

此前已有腾讯日线与分钟 Provider、按数据集拆分的字段模板、归一化 YAML、交易日历存储与合并流程。但归一化加载器没有读取 `field_mapping`、规则状态和代码前缀；归一化执行仍直接读取固定来源字段名。原腾讯日线方法查询最近窗口并筛选单日，腾讯分钟方法固定为 1 分钟。

现在原 Provider 类增加 `fetch_window` 和 `fetch_recent`，原方法保留。日历新增来源适配器，接入已有日历协议；涨停池新增来源适配器，保持原验证的 AkShare 函数调用。它们分别位于 `providers/sina/calendar.py` 和 `providers/eastmoney/limit_pool.py`，不建立新的日历存储或管理服务。

原归一化执行器读取并执行来源字段映射，加载器读取规则状态、代码前缀和空值定义。手工输入流程位于现有 `pipeline/` 下的 `inputs.py`，由现有 `cli.py` 的 `collect-input` 子命令进入。

## 四项实际输入

| 输入 ID | 复用/补充位置 | 调用方法 | 字段模板与映射 | 本次实际验证范围 |
|---|---|---|---|---|
| ASTOCK-002-daily | 腾讯现有日线 Provider | fetch_window | daily_bar.yaml | 600519，2026-09-01 至 2026-09-18，前复权，14 条 |
| ASTOCK-002-5m | 腾讯现有分钟 Provider | fetch_recent | minute_bar_5m.yaml | 300750，最近 96 根，5 分钟，不复权 |
| ASTOCK-045 | 东财涨停池 Provider | fetch | limit_up_pool.yaml | 2026-09-30 的涨停池快照，52 条 |
| ASTOCK-070 | 新浪交易日历 Provider | fetch | trading_calendar.yaml | 来源返回 8797 个日期；包含来源提供的未来日期 |

这四项在 `providers.yaml` 中标记为 `implemented_validation_only`（已实现、限验证使用），对应归一化规则为 `pending_validation`（待验证）。适配器存在和样本采集成功不自动授予正式路由资格。

## 配置怎样参与执行

配置继续使用项目已有的三个位置：

1. `config/providers.yaml`：定义输入 ID、真实来源、数据集、调用方法、参数来源、请求形态、最短请求间隔与并发限制。证券代码来自 `request.symbol`，日期来自明确的请求日期，条数来自 `config.count`；涨停池日期必须属于明确提供的交易日历。
2. `config/datasets/<数据集>.yaml`：定义标准字段、字段类型、单位、必填约束与主键。字段模板实际参与候选输出生成，不只是说明文档。
3. `config/normalization/<数据集>.yaml`：定义来源字段到标准字段的映射、上下文字段、日期/时间格式、时区、倍率、空值、必要的来源数值校验和规则状态。

例如腾讯日线的来源位置 `1/3/4/2` 分别映射为开盘/最高/最低/收盘；日线与分钟线的来源成交量保留在来源数据中，因单位尚未独立核验，标准 `volume` 输出为空。分钟线第 8 个位置是换手率基点，通过 YAML 的 `0.01` 倍率输出百分数，不作为成交额。涨停池金额、市值与封板资金单位待核验，对应候选标准字段置空，原中文字段数值完整保留。

映射执行拒绝缺失必填字段、数值布尔值、必填非有限数值、非法整数、未声明的映射目标及重复主键。待验证规则只允许在明确的候选输入路径运行；普通归一化入口默认拒绝待验证、禁用或过期规则。输出字段可用 `--fields` 选择，但必须保留模板中的必填字段。这控制候选输出字段，不改变上游接口自身的返回字段。

本次没有把既有财务等所有适配器的硬编码转换一并迁移，也没有把已有 Bar 模型或 Parquet 写入器改为任意 YAML 动态结构。已经建立的是四项输入的可执行配置与映射路径。

更新后的输入清单为 [2026-10-03 实现版 CSV](../coverage/successful-input-capabilities-20261003-implemented.csv) 与 [证据关联 JSON](../coverage/successful-input-capabilities-20261003-implemented.json)，仍覆盖 73 个成功/部分成功接口的 74 个输入定义，逐项复查 330 个已有证据文件哈希。原成功清单 CSV 当前无法写入，因此保留第一阶段文件，另存本次版本。可用 `prepare_capability_results.py --input-catalog --catalog-name <新文件名>` 生成独立版本。

## 使用方式

在项目根目录运行。源码环境先设置模块搜索路径，已经安装项目时可直接使用 `stock-data collect-input`。

```powershell
$env:PYTHONPATH = 'src'
python -m stock_data_manage.cli collect-input `
  --input ASTOCK-002-daily --symbol 600519 `
  --start-date 2026-09-01 --end-date 2026-09-18 `
  --mode replay `
  --replay-manifest provider_validation/results/raw/2026-10-01-v39-live-escalated/manifest.ndjson `
  --output-root provider_validation/results/my-daily-input
```

每次运行创建独立目录，不覆盖原响应或原结果。目录包含 `_raw/manifest.ndjson`、按哈希保存的响应体、来源行、候选标准行与 `report.json`。报告关联响应哈希、来源行哈希、代码/配置版本、请求范围、返回数量、字段单位状态和失败类别。报告路径随运行日期生成，由命令返回。

腾讯 5 分钟线回放使用相同清单，参数为 `--input ASTOCK-002-5m --symbol 300750 --count 96`。日历回放使用 `--input ASTOCK-070`；日历与涨停池的原始清单为：

```text
provider_validation/results/live-probes/rate-limited-all-20261003/_raw/missing-capabilities-20261003T174623/manifest.ndjson
```

涨停池参数为 `--input ASTOCK-045 --trade-date 2026-09-30 --calendar-file <已保存的日历 mapped-rows.json>`。该文件必须包含显式日期数组，不能由周末规则猜测。日历只有来源提供的正向日期，来源未包含的日期不推断为休市。

需要在线采集时显式使用 `--mode live`，去掉回放清单参数。执行前先按请求、范围、代码版本与有效期搜索证据；匹配有效响应则复用。回放模式没有网络回退，找不到准确匹配会保存失败报告。

手工输出路径不能落入当前配置的生产原始目录、标准目录、元数据库文件或实时热存储目录。采集节奏来自已有 `collection_profiles`，调度开关保持关闭。独立进程之间不共享限速状态；当前入口是一次一个输入的串行执行，后续调度范围确认后再设计进程间协调。

## 传输与来源合同

腾讯窗口方法保留源码快照的浏览器请求头、`Referer`、`requests.request`、连接/读取超时 `(8, 20)`、重定向设置、环境代理、三个入口顺序和失败入口 120 秒冷却。日线按 700 日分段、每段请求 640 根；区间外日期、重复日期、空前复权列表用原价顶替等情况均拒绝。分钟线不接受日期窗口或复权参数，也拒绝重复时间。

涨停池和日历保持原 SDK 函数调用与 SDK 解析，不重新实现其 HTTP 和解码层。会话按原低频探针策略配置：最多两次瞬时错误重试、至少五秒退避，不重试 403/429。SDK 函数源代码版本和哈希保存在报告，源码快照的 token 类字面量脱敏。

响应在 Provider/SDK 解析前保存。请求 URL、请求头、代理与作用域中的 token、认证和凭据类值脱敏；发现敏感响应字段时抑制原文保存，保留哈希并阻止继续解析。保存的是 HTTP 库暴露的原始应用负载，未宣称捕获网络压缩帧或 SDK 内部重试。限速标记为 `call_boundary_only`（调用边界），同主机最短间隔至少三秒，并发为一。

## 可复现验证与限制

全量回归结果：[189 项测试通过](../results/2026-10-03-yaml-input-collection-tests.xml)。新增回归覆盖 YAML 改动实际影响输出、映射别名、前缀范围、状态门禁、数值/日期/时区/空值、字段选择、非法来源行留证、严格离线回放、会话策略恢复与有效响应复用。

统一证据引用与当前文件哈希核对见 [实现验证记录](../results/2026-10-03-yaml-input-collection-verification.json)，包含四项最终回放报告、在线摘要、测试结果及本次保存的 26 个响应体哈希复查。

原脚本与迁移后结果的持久化比较见 [比较记录](../results/input-verified-20261003/comparison.json)。腾讯两项比较原源码执行与 Provider 的 URL、参数、请求头、响应状态、失败类别、响应哈希、返回窗口、全部价格和来源成交量；SDK 两项逐列逐行对照原解析 CSV。历史分钟归档还有一次 `web3` 失败，但当前固定源码快照已使用 `web/proxy/ifzq`，本次按当前源码对照，不改写历史失败记录。

可离线重新执行，输出目录须为新的目录：

```powershell
python provider_validation/tests/replay_input_capabilities.py `
  --output-root provider_validation/results/my-input-replay
```

在线验证见 [在线采集摘要](../results/input-live-20261003/summary.json)，四项均生成候选输出，分别为 14、96、52、8797 条。默认受限网络环境中首次腾讯请求连接失败已保存；允许联网后按相同参数成功，未据此判定来源不可用。前期 Windows 长路径导致的存储失败记录亦保留，与来源错误区分。

验证分层：离线合同与原实现对照通过；小范围在线采集通过；字段单位及扩大范围后的完整语义仍待验证；端到端候选数据生产通过，正式生产发布未启用。此次不实现全股票或分布覆盖、不改调度范围、不写正式生产数据，也不批量生成其余成功接口的适配器。
