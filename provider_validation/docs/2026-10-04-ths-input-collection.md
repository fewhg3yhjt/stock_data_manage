# 既有同花顺四项接口：输入转换实现

日期：2026-10-04。实现范围为用户确认的 SDA-BOARD-001 至 004，复用 `providers/akshare/boards.py` 中的同一个 `AkShareBoardProvider`，未新增 Provider 类、业务模块或管理服务。

## 已完成的输入

| 输入 | 既有方法 | 字段模板 | 归档回放范围 |
|---|---|---|---|
| SDA-BOARD-001 行业目录 | fetch_industry_list | industry_directory.yaml | 90 个行业名称与代码 |
| SDA-BOARD-002 行业指数日线 | fetch_industry_daily | industry_index_daily.yaml | 半导体 881121，2026-09-01 至 2026-10-02，21 行 |
| SDA-BOARD-003 行业资金流 | fetch_fund_flow | board_fund_flow.yaml | 即时快照，90 行 |
| SDA-BOARD-004 概念资金流 | 同一 fetch_fund_flow | 同一 board_fund_flow.yaml | 即时快照，387 行 |

这些数量仅表示原归档返回范围，不认证当前全市场完整性。此前四项输入继续可用，通用工厂现在绑定八项输入。SDA-BOARD-005/006 的旧实现仍存在，尚未接入本轮通用入口。

## 配置与实现如何衔接

- `providers.yaml`：四项增加实际 `dataset` 和 `runtime_method`。日线参数使用原方法的 `board_name/start_date/end_date/board_code`；名称和窗口来自请求，代码可来自 `dependency.board_code`，不提供代码时沿用原目录查询。代码与 SDK 来源目录不一致会阻断候选输出。
- `collection.yaml`：继续引用原 `reference_daily`、`market_after_close`、`market_intraday` 策略。请求之间按来源主机至少间隔 3 秒、并发 1；可提高各输入的间隔配置。四项引用的自动调度均关闭，本批没有将非证券输入放入证券遍历调度。
- `datasets/`：修改既有行业日线、板块资金流模板；新增确实缺失的行业目录模板。原 `industry_board.yaml` 仍表示证券成分关系。
- `normalization/`：补三个缺失映射文件，资金流文件有行业和概念两条独立规则。直接映射 SDK 的 `name/code` 和中文来源列，修改 YAML 会改变实际候选字段；字段投影仍通过 `--fields` 控制且必须保留必填字段。

旧方法继续返回原字段、来源单位和调用方提供的快照时间，字段改名复用现有 `Normalizer.map_fields` 与 YAML。结果补充 SDK 原列 `source_rows` 和身份上下文，供通用流程执行类型、必填、主键、OHLC 与单位检查。旧方法的非即时周期分支保留；新资金流输入合同只允许已验证的“即时”。

行业日线的量额单位、资金流的金额展示单位仍未独立核实，因此来源数值保存，候选标准字段置空。资金流 `snapshot_at` 使用这批原 HTTP 响应最后一次采集时间；报告同时保存最早/最后采集时间。它不是逐行行情日期，也不会使用回放执行时间冒充当前行情。

## 参数入口

已有 `collect-input` 增加 `--context-file`，读取 JSON 参数命名空间，明确的命令行参数覆盖同名值。上下文文件仅传递原有参数绑定器需要的值，不是新配置系统；报告保存文件路径和哈希。示例文件内容：

```json
{"request":{"board_name":"半导体","start_date":"2026-09-01","end_date":"2026-10-02"},"dependency":{"board_code":"881121"}}
```

在项目根目录将上面的内容保存为自己的上下文文件，然后执行：

```powershell
$env:PYTHONPATH = 'src'
python -m stock_data_manage.cli collect-input `
  --input SDA-BOARD-002 --context-file <上下文文件路径> `
  --mode replay `
  --replay-manifest provider_validation/results/raw/2026-10-02-sector-capabilities-network-retry/manifest.ndjson `
  --output-root provider_validation/results/my-ths-input
```

目录及两类即时资金流不要求上下文文件，换成对应输入 ID 即可。每次执行保存独立候选目录，包含原始负载、来源行、标准行和报告；不写生产数据。使用打包部署时须提供项目配置目录，旧 Provider 可显式指定 `normalization_root`。

## 行为与证据

SDK 函数、目录依赖函数及实际使用的 `ths.js` 源码保存哈希/快照；仍由原 SDK 生成 Cookie 和动态反爬头，保留其 URL、参数、Referer、Session、代理与分页方式。本批不为 THS 注入涨停池/日历采用的保守重试层。反爬头 `hexin-v` 在新请求元数据中脱敏，SDK 生成方式仍可对照。

回放读取旧清单的 `response_encoding`，保留原 GBK 解码。仅离线回放临时隔离 SDK 目录缓存，避免前一项回放留下内存结果而绕过本项原响应；在线缓存策略保留。缺少响应证据的目录内存结果不能计为候选验证成功。匹配或留证错误作为迁移失败报告，不据此宣称来源不可用。

修改前 Provider 的精确源码保存于 `results/ths-original-20261004/`，原 SHA-256 为 `2f8e988d652ecb2a5ecaeff665938be1a9aa6bf1d33e58628df66f706f99b5f4`。旧调查、逐接口记录和清单保留当时版本；它们指向可变工作区源码的哈希不应当作修改后代码哈希。新报告关联当前代码、配置、SDK、原响应与派生结果，未改写原记录来消除版本差异。

最终证据为 `results/ths-final-20261004/comparison.json`、`results/ths-final-20261004-tests.xml` 和 `results/ths-final-20261004-verification.json`。现有 `replay_input_capabilities.py --verify-ths-inputs --output-root <新的验证目录>` 可重现四项回放、原 Provider 返回与原 CSV 的全部来源字段对照、请求/状态/窗口对照和严格回放失败检查。动态反爬值不比较随机字节；原源码生成方式与请求头结构仍核对。原 CSV 快照采集时间与显式旧调用时间不混同，候选时间另与原 HTTP 批次核对。

本批完成离线合同与历史样本语义对照。未新增实时探针，未认证当前来源接受度、金额单位、全市场容量或生产端到端发布。自动调度、正式路由、BaoStock 两项转换和管理台均未纳入本次实现。
