# 股票与 ETF 来源名单

证券名单继续使用现有来源采集、YAML 字段映射及留证流程。沪深部分修改原 `BaoStockIndustryMembershipProvider.fetch_snapshot`；北交所原来只有验证脚本，必要新增 `providers/exchanges/security.py`，迁移其官方目录请求，不新增任务管理层。

## 当前能力与验证范围

| 来源输入 | 能力 | 本次实际范围 | 执行状态 |
|---|---|---|---|
| `SDA-BOARD-005` | 沪深股票名单，支持主板、创业板、科创板及302代码段 | 原响应日期2026-09-30，5,224只股票 | 已实现、原证据回放通过；生产路由和调度未启用 |
| 同一输入，`config.include_etf=true` | 在同一原响应中保留沪深ETF，包括货币ETF | 1,692只ETF：沪市943、深市749；与股票合计6,916条 | 已实现、分类及映射回放通过；独立市场总数未核准 |
| `SECURITY-BSE-001` | 北交所官方当前证券目录，完整翻页 | 2026-10-07采集，18页348条；各页总数与唯一证券数一致 | 原脚本在线取数、正式适配器离线对照通过；生产路由和调度未启用 |

这些是各自来源和日期的名单，不是已经发布的当日全量证券。市场与资产类型的五组检查及整份前次有效名单回退已接入原任务构建流程，见 [任务覆盖与回退](../pipeline/collection-tasks.md)。定时更新、生产资格及在线正式发布按后续确认的阶段接通。

## 请求与分类

沪深输入保留原 BaoStock 登录/退出、`query_all_stock(day)`、`query_stock_industry(date)` 两次查询及SDK留证边界；没有替换传输方式。参数仍由 [providers.yaml](../../config/providers.yaml) 定义：`trade_date` 来自请求并需要已知交易日，`symbols` 是本地筛选，`include_etf` 来自配置，默认 `false`，保持原股票调用方式。全量股票与ETF名单显式配置 `include_etf: true`。

股票名单补正了旧规则遗漏的 `302132`。ETF 根据来源记录中的交易代码类别选择，沪市使用51/52/53/55/56/58开头的交易代码，深市使用158/159；名称不要求包含“ETF”。指数、LOF和联接基金不会因名字含ETF而混入。源头股票和ETF之外的507条记录保留原响应，并另保存排除行及原因，不能把它们当作已经验证的ETF。

分类核对中的例子有交易所依据：[中航成飞302132公告](https://disc.static.szse.cn/download/disc/disk03/finalpage/2025-03-19/dd95a920-8b43-4643-9405-93047fcddf93.PDF)、[上交所511600货币ETF公告](https://www.sse.com.cn/disclosure/announcement/general/jjzssgg/c/c_20260213_10809590.shtml)、[深交所159003货币ETF说明](https://investor.szse.cn/knowledge/fund/other/t20141209_538861.html)。原行业分类接口的既有范围与对照合同保持，不借这次名单修正改变其他接口。

北交所沿用原验证脚本的代理设置（`STOCK_DATA_HTTP_PROXY`，原本地代理默认值）、`trust_env=false`、同一Session及Cookie、User-Agent、Referer、GET页面、POST分页参数、20/30秒超时、禁止自动重定向、重定向后刷新页面并重试一次，以及页间等待。原逻辑来自 [验证脚本](../../provider_validation/tests/verify_security_board_coverage.py)。不在Provider内做来源回退。

分页增加总数、页号、行数、结束页、唯一证券及返回格式检查。现有基础留证模块增加可选请求体哈希匹配，仅该目录输入启用；缓存和回放同时区分不同POST页参数，保存脱敏后的表单参数，不只按相同地址选响应。其他接口的默认匹配方式保持。

## 字段和日期

[来源字段模板](../../config/datasets/security_snapshot.yaml) 与 [字段映射](../../config/normalization/security_snapshot.yaml) 共用 `stock_code`、`stock_name`、`exchange`、`status`、`trade_date`、`source`，补充 `asset_type`。旧股票候选没有该字段时仍按已有股票默认处理。最终名单仍使用 [security_master.yaml](../../config/datasets/security_master.yaml)。

北交所字段对应 `hqzqdm`、`hqzqjc`，资产类型为 `stock`，交易所为 `BSE`。接口没有提供可靠的停牌状态，因此保存 `status=unknown`，不把存在报价当作正在交易；该状态不阻止已经在名单中的证券进入后续范围。

北交所没有历史名单日期参数，拒绝单证券和历史区间查询。标准字段 `trade_date` 表示原响应在上海时区的采集日；源头行情日期 `hqjsrq` 原样保留，并在报告中另列 `quote_dates`。本次名单采集日为10月7日、行情日期为9月30日；10月8日回放仍保留10月7日，不用回放日期或行情日期改写名单日期。跨两个采集日的分页不能拼成一个快照。

## 文件位置及核验

- 原始接口探针：[北交所原响应清单](../../provider_validation/results/raw/security-catalog-bse-20261007/manifest.ndjson)。此处只有来源验证证据；原始响应在解析前留存。
- 分类调查与原脚本结果：[分类核对](../../provider_validation/results/security-catalog-20261007/classification-review.json)、[北交所原脚本结果](../../provider_validation/results/security-catalog-20261007/bse-original-result.json)。
- 本次运行的原响应：`data/raw/_tmp/<检查ID>/<单元哈希>/`。
- 来源候选：`data/task_workspace/security_snapshot/<请求范围>/<运行ID>/sources/<来源>/<输入ID>/`，含来源行、映射JSON、Parquet和质量记录。
- 检查索引和回归报告：`data/task_workspace/_checks/security-catalog/`；运行产物不提交Git。

本次运行仅回放已留证的原响应，生成待发布候选；没有写入 `data/canonical/`，也没有转正raw或打开自动调度。股票和ETF名单的原始SDK载荷可回放，BaoStock不暴露TCP原始帧；北交所HTTP响应字节与请求体哈希可逐页核验。

原脚本与正式适配器对照请求地址、方法、非秘密请求头、POST请求体哈希、状态、响应哈希和全部证券代码/名称。Cookie在归档中脱敏，无法从离线证据恢复实际值；另外以原生Session及适配器夹具验证Cookie传递、代理、超时和刷新重试。离线通过不等于正式适配器已完成持续在线运行验证。

相关离线回归127项通过，另2项名单日期边界检查通过。独立核对来源响应、请求体与派生Parquet哈希，以及YAML和代码版本；本机索引为 `data/task_workspace/_checks/security-catalog/verification.json`。
