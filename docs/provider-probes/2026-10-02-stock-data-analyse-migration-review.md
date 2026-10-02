# stock-data-analyse 行业数据能力迁移验证

验证日期：2026-10-02 至 2026-10-03（Asia/Shanghai）

本次迁移复用上游实际使用的 AkShare 调用，不替换其请求层。当前 Provider 对响应字段做契约检查和项目内字段映射；上游调用参数、动态同花顺请求头、会话及分页仍由 AkShare 处理。

上游依据固定到 `stock-data-analyse` 提交 `c26cabcf89443ec8f1445d0e5af86bf3d6dacf3`：[行业采集实现](https://github.com/fewhg3yhjt/stock-data-analyse/blob/c26cabcf89443ec8f1445d0e5af86bf3d6dacf3/warehouse/industry.py)、[资金流实现](https://github.com/fewhg3yhjt/stock-data-analyse/blob/c26cabcf89443ec8f1445d0e5af86bf3d6dacf3/fundflow/sources.py)。

## 能力映射与结果

| 上游能力 | 本项目 Provider | 请求范围 | 真实返回与派生校验 | 当前边界 |
|---|---|---|---|---|
| `stock_board_industry_name_ths()` | `AkShareBoardProvider.fetch_industry_list()` | 全行业目录 | HTTP 200；90 行，90 个唯一代码 | 行业目录不含概念归属 |
| `stock_board_industry_index_ths()` | `AkShareBoardProvider.fetch_industry_daily()` | 半导体，2026-09-01 至 2026-10-02 | HTTP 200；21 行，21 个唯一日期；实际末日为 2026-09-30 | 仅验证一个行业；成交量、成交额单位未确认 |
| `stock_fund_flow_industry(symbol="即时")` | `AkShareBoardProvider.fetch_fund_flow("industry", period="即时")` | 即时行业排行快照 | HTTP 200；90 行、90 个唯一板块名 | 无逐行交易日期；金额单位未确认 |
| `stock_fund_flow_concept(symbol="即时")` | `AkShareBoardProvider.fetch_fund_flow("concept", period="即时")` | 即时概念排行快照 | HTTP 200；387 行、387 个唯一板块名 | 无逐行交易日期；不是概念成分关系；金额单位未确认 |
| `query_all_stock()` + `query_stock_industry()` | `BaoStockIndustryMembershipProvider.fetch_snapshot()` | 旧探针：沪深全市场，参数分别为 `day=2026-09-30` 与 `date=2026-09-30` | 旧探针在 5,212 只在市证券中覆盖 5,210 只（99.9616%）；当前 Provider 已按相同的两次快照调用方式完成 Fixture 和离线重放 | 旧探针只留存合并后的分类结果，没有原始 SDK 行或 TCP 帧；不能据此声称当前 Provider 已完成独立的全量 Live Probe |

## 请求与响应证据

- 初次请求受本机隔离网络限制，记录为 `WinError 10013`；不能据此判定数据源不可用。网络权限允许后，小范围重试取得上述返回。
- AkShare 版本为 `1.18.83`。原始 HTTP 响应字节以 gzip 保存、按 SHA-256 命名；请求方法、URL、状态、响应头、内容编码、字节数、哈希和请求范围写入对应 `manifest.ndjson`。Cookie/`hexin-v` 这类敏感请求头值已遮蔽。
- THS 行业指数原始地址为 `https://d.10jqka.com.cn/v4/line/bk_881121/01/2026.js`；行业和概念资金流原始请求 URL、分页、返回状态和哈希见原始清单。
- BaoStock SDK 不暴露原始 TCP 帧。当前正式适配器提供可注入归档器来保存 SDK 解码行；本次没有用错误的单股请求替代全量快照行为。
- 2026-10-03 曾用 `query_stock_industry(code="sh600519")` 做单只实时探测；它不是已经验证的 `query_stock_industry(date=...)` 全量快照行为。其 SDK 解码结果保留在 `2026-10-03-baostock-csrc-provider.json` 及对应 Raw 清单中，但标记为 `superseded_request_variant`，不计入本迁移的 Provider 验证结论。

## 生成文件

- 原始探针清单：`raw/2026-10-02-sector-capabilities-network-retry/manifest.ndjson`、`raw/2026-10-03-csrc-provider-small-probe/manifest.ndjson`
- 原始网络失败记录：`raw/2026-10-02-sector-capabilities-v1/manifest.ndjson`
- 派生数据与哈希、行数、日期窗口、唯一性检查：`2026-10-02-sector-derived-validation.json`
- 可重复离线重放入口：`replay_sector_capability_archives.py`（校验原始 HTTP 哈希后重建 CSV，不联网）
- 可重复离线重放入口：`replay_sector_capability_archives.py`（校验原始 HTTP 哈希后重建 CSV，不联网）
- 完整派生 CSV：`2026-10-02-sector-derived/`
- BaoStock 新 Provider 小样本：`2026-10-03-baostock-csrc-provider.json`
- BaoStock 既有全市场覆盖基线：`2026-10-01-security-board-coverage.json`

所有新能力在 `config/providers.yaml` 与 `config/capabilities.yaml` 中均保持 `validation_only`。本次未写入生产数据目录，没有启用生产路由，也没有用资金流数据推断证券的概念成分。
