# 运行数据目录与归档

本文记录 **2026-10-25 核对的现有实现**。示例取自数据日期 `2026-10-09` 的已发布证券名单及其 BaoStock 来源文件；本机业务文件不随文档复制。历史文件保持原路径；新生成的原始响应和 SDK 证据文件使用可读名称，完整 SHA-256 仍记录在清单中。

阅读入口：[文件名与校验值](#文件名与校验值)、[原响应实例](#原响应实例)、[来源工作文件](#来源工作文件)、[任务文件](#任务文件)、[正式数据与数据库](#正式数据与数据库)、[重复内容与待实施方向](#重复内容与待实施方向)。运行命令、重做和进度规则见 [任务流程](../pipeline/collection-tasks.md)，本文解释实际落盘文件。

## 日线聚合存储实现（2026-10-28）

2026-10-30单位规则v8只改变新候选的单位合同匹配与换算，未迁移或重写旧正式分区。新候选报告中的 `applied_unit_contract` 记录匹配条件、倍率和证据；`unit_precision_flags` 按实际非空字段生成并进入聚合记录。旧报告的 `verified_unit_scopes` 是历史格式，不再作为新任务的匹配依据；详细规则由[单位合同](../../stock-data-design-qfq-history-rebuild.md#22-来源单位合同2026-10-30)维护。

新规范日线按证券与交易日期保存一条聚合记录，内含证券类型、价格或指数点数语义，以及不复权、前复权、后复权组的数值、状态和证据引用。候选与来源清单仍按证券、日期、口径留存；最终 Parquet 物理分区不再按口径拆开。字段和完整性以[专项设计](../../stock-data-design-qfq-history-rebuild.md)为准，不在本文维护第二套 schema。

规范路径为 `canonical/<日期>/daily_bar/<资产类型>/{data.parquet,manifest.json}`。文件中 `quotes.none/forward/backward` 是固定嵌套组；manifest 标记 `daily-aggregate-v1` 与 `adjustment=aggregate`。恢复复用现有任务提交标记和原子发布流程。旧 `adjustment=...` 分区与业务数据保持原位，不自动合并、不迁移，也不作为新查询的回退路径。

2026-10-29已在 `data/canonical/2026-10-28/daily_bar/stock/` 正式发布3条v7三口径样本，9份活动原响应请求清单位于 `data/raw/2026-10-28/tencent/kline_daily/`。精确资格的直接证据依赖位于data，隔离重做与故障验收仍在tmp；具体范围、哈希和回读结论由[项目进度](../../PROJECT_PROGRESS.md#2026-10-29-v7-三只股票验证收尾与正式验收)维护。重做可以更新被重做证券的审计引用，业务值与源响应哈希需独立检查，不能以Parquet字节不变替代业务验收。

## 目录约定

2026-10-11按用户确认的结构修改现有代码并迁移旧业务目录。正式运行根目录为 `data/`；离线回放、故障注入及实现验收使用 `tmp/` 下独立运行根目录，内部结构相同。`provider_validation/` 中的历史验证证据保留原位，不新增业务产物。

```text
data/
├── raw/
│   ├── _tmp/<数据日期>/<来源>/<接口>/   # 日期已确定、流程未完成
│   └── <数据日期>/<来源>/<接口>/        # 完整流程成功后的原响应
│       ├── bodies/<数据日期>_<输入标识>_<证券/日期/口径>_<采集时刻>.bin
│       ├── manifest.<可读请求标识>.ndjson # 一个请求范围的响应与字节引用
│       └── manifest.ndjson             # 本接口请求清单索引
├── task_workspace/
│   ├── <数据日期>/<数据集>/
│   │   ├── sources/<来源>/<接口>/       # 解析、映射、候选及报告
│   │   ├── task.json                   # 执行参数及冻结范围
│   │   ├── prepared.parquet            # 本次待发布数据
│   │   ├── coverage.json               # 名单最终覆盖检查
│   │   └── summary.json                # 状态、实际日期、检查及原响应引用
│   ├── trading_calendar/<日期>/<来源>/<接口>/refresh-<时分秒>/ # 轻量日历响应
│   ├── _checks/                        # 业务复核与验收记录
│   └── _locks/                         # 既有文件锁
├── canonical/<数据日期>/
│   ├── security_master/{data.parquet,manifest.json}
│   └── daily_bar/<资产类型>/{data.parquet,manifest.json} # 一证券一日一条，三组嵌套
├── task_archive/
│   ├── _raw_evidence/<清单哈希>/        # 发布引用的不可变原响应
│   ├── _raw_audit/<来源_日期_接口_范围>/ # 被替换或损坏的原响应证据
│   ├── _workspace_audit/<日期_数据集>/  # 被替换的构建结果与报告
│   └── <数据日期>/<数据集>/            # 显式归档的已发布工作区
└── metadata/metadata.duckdb             # 任务、单元、提交、分区及引用索引
```

不增加任务ID、范围哈希或 `full_market` 目录层。`security_master` 表示证券主数据，`security_snapshot` 表示来源证券快照；`bse`、`sse` 分别是北交所、上交所数据来源。定义和中文说明位于 `config/providers.yaml`、`config/collection.yaml`、`config/datasets/` 与 `config/normalization/`。

同接口可能请求不同证券、窗口、粒度或复权。新请求文件名使用输入标识及可读参数区分，例如 `manifest.ASTOCK-002-daily_symbol-sh600519_start-date-2026-10-01_end-date-2026-10-23_adjustment-qfq.ndjson`；较长的单值参数会缩短显示，列表参数显示条数和首尾项，完整值仍保存在 manifest。新正文文件名包含数据日期、输入标识、证券/日期/复权口径及采集时刻，来源和接口由父目录标明。名称不嵌入摘要。此前生成的带参数指纹文件名保留原样。接口层 `manifest.ndjson` 是索引，响应事件位于其指向的各请求清单。

新规范日线存储路径为 `<日期>/daily_bar/<资产类型>/`，业务键是证券与交易日期；查询从同一条记录投影所选口径。来源参数 `qfq/hfq` 在候选映射为 `forward/backward`。历史 `adjustment=...` 文件和旧混合目录不自动迁移，也不参与新聚合查询；不存在聚合结果时不回退旧单口径文件。分钟热数据 `data/hot/minute_hot.db` 保持原结构。证券名单不再新写 `security_master/current`，通过既有元数据选择不晚于目标日的最新合格分区。

## 文件名与校验值

| 标识 | 实际例子 | 含义 |
|---|---|---|
| 文件内容校验值 | `751c5d0399f8558e99d517a9c06a6d2ac2d3e613afb342a0f2dbb4e1032b47bc` | 文件字节的完整 SHA-256，写入 manifest 用于完整性校验；新生成的原始文件名不含摘要 |
| 请求标识 | `ASTOCK-002-daily_symbol-sh600519_adjustment-qfq` | 输入标识及可读请求参数，用于区分请求范围；不含摘要 |
| 范围标识 | `scope-ba08f4d3f1725a05` | 绑定参数的确定性 JSON 摘要前16位，用于执行记录，不是响应内容摘要 |

请求清单命名实现见 [storage/raw.py](../../src/storage/raw.py) 的 `request_manifest_name`；新 HTTP 响应正文命名见同文件的 `response_body_path`，SDK 及派生响应正文命名见 `_archived_body_path`，均在 `write_bytes` 保存。摘要实现见 [storage/integrity.py](../../src/storage/integrity.py)，仅作为完整性元数据，不放入新文件名。下方 `manifest.all-3f363376e304.ndjson` 是历史示例，保留其原名。

校验值根据内容计算，不是根据文件名计算：仅改名不改变它，改内容后须重新核验。已有哈希命名的历史文件保持原名；新生成的 HTTP 响应、SDK 函数快照及其他原始正文文件均使用日期、来源、接口、请求范围或用途、采集时刻等可读信息命名，完整 SHA-256 仅保存在 manifest 中。校验只能发现内容变化，不能证明价格、证券身份、日期或市场覆盖正确。

`.json` 是完整 JSON 文档或数组；`.ndjson` 每行一条 JSON，不能整体当一个 JSON 文档读取；`.parquet` 是有字段类型的列式表，不能当文本查看；`.bin` 是按字节保存，不代表特定业务格式。本例数据 `.bin` 为 JSON，SDK 快照为源码文本，其他接口可能是 HTML 等内容，须结合清单的类型与编码判断。

## 原响应实例

实际目录：[BaoStock 原响应](../../data/raw/2026-10-09/baostock/typed_security_universe/)。`typed_security_universe` 是 Provider 的输入接口标识，组合三个原生 SDK 查询，不是 BaoStock 的单个原生方法名。

```text
raw/2026-10-09/baostock/typed_security_universe/
├── manifest.ndjson
├── manifest.all-3f363376e304.ndjson
└── bodies/
    ├── 1816d517…bin   query_trade_dates 返回
    ├── 751c5d03…bin   query_all_stock 返回
    ├── 109d5b31…bin   query_stock_basic 返回
    ├── 14c6b32f…bin   query_trade_dates 源码快照
    ├── a76d7e5a…bin   query_all_stock 源码快照
    ├── dbc03109…bin   query_stock_basic 源码快照
    ├── c8fc45a6…bin   login 源码快照
    └── 3fcfcedd…bin   logout 源码快照
```

省略号只用于展示，这些是既有历史文件，实际文件名仍是完整64位摘要。它们不会因新命名规则而迁移或改名。这八份文件不是八份证券清单，只有三份为查询结果，另五份为经脱敏的 SDK 函数源码证据，其名字与路径记录于来源工作目录的 `raw_refs.json`。不能把整个 `bodies/` 都称为网络原始响应。

| 实际查询 | 返回条数 | 范围 |
|---|---:|---|
| `query_trade_dates(start_date="2026-10-09", end_date="2026-10-09")` | 1 | 请求日交易日记录 |
| `query_all_stock(day="2026-10-09")` | 7,424 | 请求日证券列表，含指数等非股票记录 |
| `query_stock_basic()` | 8,992 | 无日期参数的基础分类、上市/退市信息；不是直接查询10月9日基础快照 |

BaoStock SDK 不暴露 TCP 报文字节，此处保存 SDK 解码字段及记录的归档表示。`original_transport_bytes_available=false` 明确这个限制：可核验保存的字节，不能声称是原始 TCP 报文。HTTP 来源则捕获响应体原字节。

数据文件 `751c5d03…bin` 的实际结构如下，仅展示第一条记录，其余7,423条省略：

```json
{
  "fields": ["code", "tradeStatus", "code_name"],
  "rows": [
    {"code": "sh.000001", "tradeStatus": "1", "code_name": "上证综合指数"}
  ]
}
```

`fields` 是来源字段名，`rows` 是记录数组；`code` 是 BaoStock 代码，`tradeStatus` 是来源交易状态，`code_name` 是来源名称。第一条为指数，不能直接纳入股票名单。实际分类与历史日期依据统一见 [证券来源说明](../providers/security-catalog.md)。

接口 `manifest.ndjson` 的实际记录：

```json
{
  "event": "request_manifest",
  "request_manifest": "manifest.all-3f363376e304.ndjson",
  "sha256": "0d797abab77db0618c0262b2a0291fe37912be7d46eea04eb43bdbea6fb7a728"
}
```

它是请求清单索引，`sha256` 是清单文件摘要，不是响应体摘要。[请求清单](../../data/raw/2026-10-09/baostock/typed_security_universe/manifest.all-3f363376e304.ndjson) 内的一条实际事件节选为：

```json
{
  "event": "source_payload",
  "method": "query_all_stock",
  "request_parameters": {"day": "2026-10-09"},
  "mode": "replay",
  "fetched_at_utc": "2026-10-11T16:55:17.756268+00:00",
  "body_storage": "bodies/751c5d0399f8558e99d517a9c06a6d2ac2d3e613afb342a0f2dbb4e1032b47bc.bin",
  "body_sha256": "751c5d0399f8558e99d517a9c06a6d2ac2d3e613afb342a0f2dbb4e1032b47bc",
  "payload_bytes": 523887,
  "original_transport_bytes_available": false
}
```

| 字段 | 解读 |
|---|---|
| `event`、`method` | 记录类型和 SDK 调用方法；清单还有会话、派生、文件存储、日期分区事件 |
| `request_parameters` | 真正请求参数，不能用目录日期代替 |
| `mode` | 本次从已有字节回放，未再次联网；其他记录也可能为 `live`、`cached` |
| `fetched_at_utc` | 本例是本次回放时间，不是数据日期或最初联网时间 |
| `source_ref` | 完整事件中有原清单、行号和联网时间；本例原响应于 `2026-10-11T15:37:52.144446+00:00` 获取 |
| `body_storage` | 相对该请求清单目录的文件位置，不是文件内容 |
| `body_sha256`、`payload_bytes` | 保存内容的摘要、字节数，用于完整性检查 |
| `provider`、`endpoint`、`scope`、`input_scope`、`code_version` | 完整事件还记录来源、查询范围、输入契约和代码依据 |

HTTP 事件使用 `http_response`，另外记录 `url`、`status_code`、脱敏请求/响应头、`content_type`、`encoding`、`body_bytes`；HTTP 200 只说明收到响应，不说明业务合格。失败和空返回也留存，不能算成功空数据集。

raw 在解析前写入，恢复、解析和最终提交读取。全流程成功后对应请求移出 `_tmp`；失败保留暂存证据。暂存中还可能有 `_managed_task.<请求标识>.json`，表示正式任务管理的文件归属，防止覆盖无管理记录的文件，不存行情；请求移出暂存时清除。当前活动 raw 日/来源/接口/范围唯一，审计副本另存，不能仅按扩展名批量删除。

## 来源工作文件

实际目录：[BaoStock 来源工作文件](../../data/task_workspace/2026-10-09/security_master/sources/baostock/typed_security_universe/)。本例十份文件共用前缀 `all-3f363376e304`。下面的示例是实际内容节选，摘要缩写仅用于阅读。

| 后缀 | 内容示例及解读 | 生成与读取用途 |
|---|---|---|
| `.started.json` | `status="fetching"`、`task_id="fd16a2a04f7d"`、`scope={"trade_date":"2026-10-09"}`、`created_at` | 来源执行开始时保存；这个ID是输入执行ID，不是整个名单任务ID；不是完成标记 |
| `.source-rows.json` | `[{"code":"sh.000001","code_name":"上证综合指数","tradeStatus":"1"}]`，实际7,424条 | 提取来源行，供字段核对及诊断；已与 raw 重复保存 |
| `.parsed-rows.json` | 筛选解析后的5,224条股票，见下方行示例 | Provider 解析后保存候选，随后进行字段映射 |
| `.excluded-rows.json` | `code="sh.000001"`、`type="2"`、`reason="non-stock type or outside stock range"`，实际5,631条 | 从多个查询中排除的记录及原因，不能用7,424减5,631计算候选数 |
| `.normalized.json` | 解析行加标准来源字段 `source="baostock"`，实际5,224条 | YAML 映射后的标准候选数组，供回放对照和诊断 |
| `.normalized.parquet` | 同批5,224条标准候选，有日期等字段类型 | 后续任务合并实际读取的来源表；与 JSON 是同批数据的两种表示 |
| `.report.json` | `input_id`、`parameters`、`status="candidate_complete"`、`row_count=5224`、输出引用及版本 | 完整来源报告，恢复、摘要、发布读取；本例约2.76 MB，不是简短任务状态 |
| `.quality.json` | `status="candidate_complete"`、`row_count=5224`、`coverage_denominator=5224`、`failure_class=null`、`error=null` | 来源阶段检查结论；分母是来源候选，不是独立全市场数量 |
| `.raw_refs.json` | `raw_manifest={"path":"…manifest.all-3f363376e304.ndjson","sha256":"0d797a…"}`、`source_response_hashes`、`sdk_source_snapshots` | 原响应与 SDK 证据的位置及摘要，引用本身不复制字节 |
| `.manifest.json` | `scope`、`scope_key`、`raw_manifest`、`output`、`report`、`quality_report`、`status` | 来源处理汇总清单，区别于 raw 清单和正式数据清单 |

旧输入也可能使用 `.parsed.source-rows.json`、`.parsed.excluded-rows.json`、`.quality_report.json` 等后缀；以其报告路径为准，不代表新增业务阶段。`source-rows`、`parsed-rows`、`excluded-rows` 并非每个接口都有。日线常见八种为 started、source-rows、normalized JSON/Parquet、report、quality、raw_refs、manifest，当前逐证券生成，尚未合并为来源汇总表。

解析文件首行实际内容：

```json
{
  "asset_type": "stock",
  "date_basis": "direct_historical_type_checked",
  "delist_date": null,
  "exchange": "XSHG",
  "list_date": "1999-11-10",
  "script_sources": "bs_stock_basic;bs_all_stock",
  "status": "unknown",
  "stock_code": "600000",
  "stock_name": "浦发银行",
  "trade_date": "2026-10-09"
}
```

`stock_code`、`stock_name` 是解析后代码及名称；`exchange` 是市场，`asset_type` 是资产类型。`trade_date` 是候选所属日；`list_date`、`delist_date` 是上市/退市依据。`date_basis`、`script_sources` 是历史日期及分类证据来源。`status="unknown"` 表示交易状态未确认，不能由名称或记录存在推断为可交易。标准候选增加 `source="baostock"`；Parquet 按 YAML 类型保存。

引用对象的实际例子，节选自来源 `.manifest.json` 的 Parquet 描述符：

```json
{
  "path": "sources/baostock/typed_security_universe/all-3f363376e304.normalized.parquet",
  "row_count": 5224,
  "byte_length": 109071,
  "sha256": "b8aba9edeeba656ceda38dd3db59f5fa21000a1082890986d6ca23b39c6529be"
}
```

这些路径相对 `task_workspace/2026-10-09/security_master/`，不是相对 `sources/.../` 内的 JSON。`row_count` 是行数，`byte_length` 是字节数，`sha256` 是被引用文件的内容摘要。完整描述符另含来源响应摘要、代码/规则版本、字段类型。原响应摘要不能拿来校验派生 Parquet。

来源报告中的 `eligible_for_production_routing=false`、`publication_permitted=false` 是当时的候选阶段字段，不能据此断言整个任务未发布，也不能替代当前来源资格。报告保持历史快照：整个任务状态看根目录 summary，最终数据看 canonical 清单，来源资格看当前证据和元数据。

## 任务文件

位置为 `task_workspace/<数据日期>/<数据集>/`。当前工作区没有日线 `stock/etf` 层；同日任务串行复用，最外层文件表示最近执行，数据库按任务ID保留状态。不能把最新摘要当作全部任务的状态。

| 文件 | 内容示例与字段解读 | 生成、读取和保留 |
|---|---|---|
| `task.json` | `task_id`、`dataset`、`trade_date`、`units`；单元包含 `input_id`、`context`，日线另有证券代码 | 自动保存参数和冻结范围；恢复不得偷偷换名单或参数 |
| `prepared.parquet` | 来源合并后的待发布规范表，本例7,267条 | 检查/提交生成，发布读取；当前成功后仍保留，不自动清理 |
| `coverage.json` | `requested_date="2026-10-09"`、`passed=true`、`group_counts`、`missing_units`、`missing_groups`、`source_failures` | 名单最终覆盖检查；日线检查通常在摘要及正式清单中，不等同来源 quality |
| `summary.json` | 日期、状态、raw/processing/publication 进度、来源单元、最终检查及正式位置 | 自动更新；通常按5秒间隔在单元完成时保存，失败/阶段切换立即保存；可在数据库占用时只读查看 |
| `universe_review.json` | `target_date="2026-10-09"`、`historical_universe_verified_complete=false`、来源比较及逐证券依据 | 特定历史名单来源的复核文件，不是所有任务必备 |

本例 coverage 的五组数量为沪市股票2,320、深市股票2,904、北交所股票349、沪市ETF944、深市ETF750。`passed=true` 表示配置检查通过，`independent_full_market_coverage_verified=false` 表示未取得独立市场名录的全量证明，二者不冲突。

已发布名单 [summary.json](../../data/task_workspace/2026-10-09/security_master/summary.json) 的实际字段节选：

```json
{
  "task_id": "security-master-history-20261009",
  "status": "published",
  "requested_date": "2026-10-09",
  "data_date": "2026-10-09",
  "complete_today": true,
  "publication": {"published": true, "row_count": 7267}
}
```

`requested_date` 是目标日，`data_date` 是正式数据所属日，`complete_today` 表示本任务单元及配置检查完成，不是“今天采集”或独立市场全覆盖。`publication` 另含正式清单路径及摘要、原响应稳定引用；`progress` 是三阶段数量和耗时；`source_units` 是来源/证券执行结果，`missing_units` 是未完成或被拒绝单元。恢复使用数据库断点，不只依赖周期性 summary。

## 正式数据与数据库

以下清单实例是本机历史名单与旧布局的留档示例，不代表新的聚合日线布局。当前规范日线位于 `canonical/<日期>/daily_bar/<资产类型>/`，其 manifest 记录整条聚合记录范围、来源和完整性。名单 [正式清单](../../data/canonical/2026-10-09/security_master/manifest.json) 的实际节选：

```json
{
  "dataset": "security_master",
  "asset_type": "security",
  "partition_key": "2026-10-09",
  "row_count": 7267,
  "expected_count": 7267,
  "content_hash": "a8e4f84ed9427b7ec6f2efde3f33f6fac9df030280ca53880fbe27f7779844e5",
  "size_bytes": 185520,
  "schema_version": "v1"
}
```

`content_hash`、`size_bytes` 核验旁边的 data.parquet；`row_count`、`expected_count` 是实际行数和检查分母；`raw_refs` 关联发布依据的原响应；`publication_metadata` 保存来源、日期、覆盖结论。写入、查询、恢复读取正式清单。提交期间可能有 `task-commit.json`，表示发布收尾待恢复，正常收尾后删除，不是新业务数据。

正式名单首行实际字段为 `instrument_id="BSE:920000"`、`symbol="920000"`、`exchange="BSE"`、`asset_type="stock"`、`name="安徽凤凰"`、`list_date="2020-12-23"`、`delist_date=null`、`status="unknown"`。名单日期由分区和清单表示，规范名单不再使用来源候选的 trade_date 字段；日线记录另有证券、交易日、复权、OHLC等。标准字段与单位仍由 `config/datasets/`、`config/normalization/` 定义，本文不改变数据含义。

`metadata.duckdb` 还保存来源资格、发布索引、日历等模块记录，不只有任务状态；运行时的 `.wal` 是数据库写入日志。当前任务表由 [metadata.py](../../src/storage/metadata.py) 的 `save_collection_task` 写入：

| `collection_task` 列 | 当前含义 |
|---|---|
| `task_id` | 整个任务标识，主键 |
| `definition_hash` | 固定参数和范围的摘要，防止同ID换定义 |
| `dataset`、`status` | 类别及 collecting/checking/committing/failed/published 状态 |
| `state_json` | 完整任务 JSON：定义、所有单元、完整来源报告、进度和提交信息；不是轻量逐证券表 |
| `updated_at` | 最后持久化时间 |

例如 `state_json.units.sh600519.status="candidate_complete"` 是证券候选完成的断点，`artifact`、`raw_manifest` 是文件位置，各自哈希用于核验，`report` 当前还包含完整报告；这里不直接保存 OHLC 表，实际价格记录在来源/正式 Parquet 中。

每完成一只证券，当前仍重新脱敏、序列化整个 state_json；之前只减少了重复保存次数，没有改成逐证券单行更新。显示回调只更新内存；摘要周期性保存并保存数据库状态，其余已完成单元只保存数据库状态一次。任务断点用于恢复，引用用于定位，校验值用于发现内容变化；这些用途不要求每项另建独立 JSON。

## 重复内容与待实施方向

以下方向尚未实施，本文不授权自动清理，不改变现有代码、字段或路由资格：原始证据文件的可读命名已实现，历史哈希命名文件保留原位。轻量调度计划直接返回给调用方，不另存UUID命名的调度副本。

| 当前事实 | 讨论中的精简方向 |
|---|---|
| raw、source-rows、parsed-rows 重复保存记录 | 从 raw 转换到来源汇总表，不长期保留每层 JSON |
| 标准候选同时保存 JSON 和逐证券 Parquet | 保留来源汇总 Parquet，不逐证券生成八份工作文件 |
| report、quality、raw_refs、工作 manifest 重复部分内容 | 合并到原响应清单、轻量断点、任务摘要 |
| 本例报告含10,448条逐证券依据，约2.76 MB，随后再嵌入数据库 | 只保存必要依据及异常原因，不反复写完整报告 |
| prepared 发布后保留，发布还复制 raw 到 `_raw_evidence/` | 讨论临时产物清理及减少重复副本；先核验现有稳定引用的迁移 |
| 股票、ETF复用日期/数据集工作区 | 讨论按已有资产类型分开，避免最外层摘要相互覆盖 |

旧文件的独有数据、现有引用、承接位置和删除清单需先列明，再在确认范围内迁移核验。业务原响应、独有来源证据和正式数据不能当作无用文件直接删除。仍须保证断点/指定证券重做、失败不替换正式数据、日期范围唯一及原字节核验。

本文只补充现状说明和示例。文件职责总入口仍为 [CODE_STRUCTURE.md](../../CODE_STRUCTURE.md)，不另建重复说明。

本次说明核验：7个 JSON 示例逐字段对照本机原文件、8个 `.bin` 重新计算 SHA-256、来源 Parquet 5,224行及正式名单7,267行独立回读、26处文件/目录/章节链接检查均通过。未连接正在运行的生产数据库、未重新请求来源、未修改或删除业务文件。上述数量是本例核验范围，不是全市场日线验收结果。

## 日期含义

- 历史日线使用任务目标交易日，今天补采昨天仍保存于昨天。多日窗口保留完整原字节，任务只提取目标日，不拆成伪造原响应。
- 历史名单接口使用明确请求日；北交所、上交所当前目录使用原响应在上海时区的采集日。缓存、回放保留最初日期，不能用今天目录构建过去名单。
- 当前目录响应在解析前就进入实际采集日目录。各页跨日、日期不符或解析失败时，证据留在各自日期暂存区，任务记录失败，不转正或发布。任务日期是构建目标，摘要明确链接失败来源的实际位置和捕获时间。
- 沿用前次名单只记录旧日期、年龄及原因，不改写旧清单，不生成新日期正式名单或原响应。

## 正常、失败和重做

1. 通过来源资格检查后，精确响应先进入对应日期 `raw/_tmp`；失败及空返回也保留。
2. 解析、YAML映射和基础校验进入来源目录；任务和单元状态保存于既有元数据库。
3. 构建候选，检查覆盖、有效性与唯一键。名单保存 `coverage.json`；日线按既有YAML门槛检查，结果汇总于摘要和正式清单，不另造重复报告。
4. 发布日期分区，校验来源哈希并将成功请求移出 `_tmp`，完成收尾后标记已发布。
5. 断点重做复用有效单元，只补缺失、异常或检查拒绝的单元；合格响应之后失败则继续处理。指定证券重做只更新日线所选证券。合并的是派生记录，原字节不拼接。
6. 全量重做清空目标日期、本任务请求范围的活动暂存及构建结果，再从源头采集；清理前保留审计。失败不替换已发布数据，其他日期和请求范围不受影响。

同日期、数据集只有一个活动构建工作区，构建文件表示最近执行。任务ID仍用于元数据追踪。复用文件锁串行写入，有未完成提交时先恢复，其他任务不能覆盖其待发布文件。是否完成依据单元记录和哈希，不靠扫描残留文件。

活动raw每天每个请求范围只维护一份；审计和发布引用保留不可变证据。不同内容的重做仍增加审计占用，自动清理及期限未实现。来源报告中的暂存引用是采集当时的位置；发布后的稳定引用以摘要、正式清单和元数据为准，不改写已核验报告。

发布中断按提交记录恢复，不再请求接口。归档只允许已发布且摘要、候选、原响应和正式数据均核验的工作区，移动后更新元数据与摘要引用，拒绝覆盖已有归档。旧候选归档入口保留对既有三层路径的校验，但不再生成旧布局业务工作区。操作命令见 [任务流程](../pipeline/collection-tasks.md)。

## 验证区与当前边界

`provider_validation/` 只保存正式实现前的来源探针、原返回和派生证据。业务开发的新代码在 `src/`，测试在 `tests/`，说明在 `docs/`；响应、复核及端到端结果使用 `data/` 或隔离 `tmp/`，已有证据只读引用。

独立 `collect-input` 只生成候选，不自动转正。离线回放须指定独立 `--data-root`，禁止发布至生产根目录；显式 `--output-root` 保留既有隔离候选方式，禁止输出到验证区或生产路径。

已通过资格、日期和范围检查的留存原响应可由正式 `collect-task --mode retained` 重新解析及发布到data；它与隔离 `replay` 明确区分，不通过复制规范文件绕过检查。资格证据、原响应及四层核验材料保存在 `task_workspace/_checks/`，正式活动raw和发布审计仍使用上面的统一布局。三阶段进度位于既有任务摘要与元数据，不再增加进度目录或数据库，详见 [任务进度](../pipeline/collection-tasks.md#三阶段进度)。

### 历史目录迁移记录（2026-10-11）

以下描述的是当时的迁移与验收，不是当前采集进度或来源资格；当前文件解释与已发布名单示例见上文。

旧业务目录的152个文件已逐字节保存在 `data/task_archive/_workspace_audit/layout-migration-20261011/`。三份真实候选及其原响应迁入按日期的活动目录，四份历史回放只留归档；元数据引用同步更新。BaoStock原响应所属日为10月9日，北交所和上交所当前目录所属日为10月10日。当时失败任务继续失败，raw仍在各自日期的 `_tmp`，没有通过该次迁移生成正式证券名单。

迁移清单同时记录旧路径、审计位置和当前位置，原JSON、原响应及原哈希可追溯；新位置的派生描述符重新核验，历史代码指纹不修改。SDK依赖快照也按原哈希核验。见 [迁移结果](../../data/task_workspace/_checks/layout-production-20261011/migration-result.json) 和 [迁移核验](../../data/task_workspace/_checks/layout-production-20261011/migration-verification.json)。新的来源复核在隔离 `tmp/` 执行，资格和正式入口检查见 [阶段索引](../../data/task_workspace/_checks/layout-production-20261011/verification.json)。日线在线资格、常驻调度及管理页面仍未完成。

本轮276个不同测试用例的最终结果通过。聚合回归206项中曾有1项失败，发现同日不同任务的旧覆盖报告阻止失败保存；修复为先归档构建文件、更新旧摘要引用后，相关15项定向复验通过。运行布局67项、原响应缺失和恢复等7项补充检查通过。10月9日真实归档在隔离新布局发布7267条，包含五组证券和ETF；历史日线目标日提取也通过。原响应哈希、转正后暂存无文件及文档链接另行复查，结果见 [本轮核验索引](../../tmp/directory-layout-20261011/verification.json)。这些回放成果不代表生产发布或在线资格。

旧正式任务 `security-master-20261009` 因两个当前目录采集日为10月10日失败，当时迁移生成的摘要记录失败；该日期工作区后来已由新的已发布任务使用，当前摘要不能再当作旧失败任务证据。原摘要在上述迁移审计副本中逐字节保存。[历史检查索引](../../data/task_workspace/_checks/security-production-20261010/verification.json) 保持原样，其旧路径通过迁移清单追踪。此前隔离7267条名单见 [接入检查](../../data/task_workspace/_checks/summary-etf-20261010/verification.json)，按日raw验收见 [历史检查](../../data/task_workspace/_checks/daily-raw-20261010/verification.json)。
