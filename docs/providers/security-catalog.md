# 股票与 ETF 来源名单

证券名单继续使用现有来源采集、YAML 字段映射及留证流程。沪深部分修改原 `BaoStockIndustryMembershipProvider.fetch_snapshot`；北交所原来只有验证脚本，必要新增 `providers/exchanges/security.py`，迁移其官方目录请求，不新增任务管理层。

## 当前能力与验证范围

2026-10-09 既有 Provider 在线核验得到沪市股票2,320只、深市股票2,904只、深市ETF750只、沪市ETF943只、北交所股票349只，共7,266条。沪深股票、深市ETF及北交所股票与当次交易所目录逐代码匹配；沪市ETF目录944条，比BaoStock多 `512390`（中国低波ETF平安）。原SDK载荷没有该代码，差异不由本地映射或过滤产生。

原核对把这个目录差异解释为漏采，结论需修正。[基金管理人2026-09-30公告](https://paper.cnstock.com/html/2026-09/30/content_2274033.htm)明确该基金最后运作日为9月8日、9月9日进入清算；公告没有给出实际终止上市生效日。目录数量、清算状态和当前上市范围尚未按同一日期口径核准，不能仅凭943与944的差异认定漏采，也不能为凑数把清算基金当正常交易ETF补入。历史 [逐代码核对](../../provider_validation/results/security-activation-20261009/coverage-review.json) 和 [核对代码](../../provider_validation/results/security-activation-20261009/coverage-review-code.txt)保留原样，其“漏采”解释由本段更正。

当前全量目标由 `security_master` 配置限定为沪市A股、深市A股、北交所股票、沪市ETF、深市ETF五组，含主板、创业板及科创板。B股、LOF、债券和港股等不在当前目标中。来源名单保留暂停交易证券，北交所状态为未知；上市、清算与退出状态口径尚未统一核准。配置要求五组存在、此前证券无异常消失，不等于全市场100%覆盖证明。

没有正式名单发布或完成真实业务任务验收；在线来源资格仍待补齐。后续通过正式任务自动记录日期、范围、分组数量及最终检查，源头可行性证据仅作为历史只读输入。

以下表格保留先前实际验证范围，不能替代上述最新覆盖结论。

| 来源输入 | 能力 | 本次实际范围 | 执行状态 |
|---|---|---|---|
| `SDA-BOARD-005` | 沪深股票名单，支持主板、创业板、科创板及302代码段 | 原响应日期2026-09-30，5,224只股票 | 已实现、原证据回放通过；生产路由和调度未启用 |
| 同一输入，`config.include_etf=true` | 在同一原响应中保留沪深ETF，包括货币ETF | 1,692只ETF：沪市943、深市749；与股票合计6,916条 | 已实现、分类及映射回放通过；独立市场总数未核准 |
| `SECURITY-BSE-001` | 北交所官方当前证券目录，完整翻页 | 2026-10-07采集，18页348条；各页总数与唯一证券数一致 | 原脚本在线取数、正式适配器离线对照通过；生产路由和调度未启用 |

这些是各自来源和日期的名单，不是已经发布的当日全量证券。市场与资产类型的五组检查及整份前次有效名单回退已接入原任务构建流程，见 [任务覆盖与回退](../pipeline/collection-tasks.md)。[定时更新与资格检查](../pipeline/collection-tasks.md#证券清单定时更新) 已接通现有任务入口；同日期状态口径、资格和真实任务发布验收尚未完成。

## 请求与分类

沪深输入保留原 BaoStock 登录/退出、`query_all_stock(day)`、`query_stock_industry(date)` 两次查询及SDK留证边界；没有替换传输方式。参数仍由 [providers.yaml](../../config/providers.yaml) 定义：`trade_date` 来自请求并需要已知交易日，`symbols` 是本地筛选，`include_etf` 来自配置，默认 `false`，保持原股票调用方式。全量股票与ETF名单显式配置 `include_etf: true`。

股票名单补正了旧规则遗漏的 `302132`。ETF 根据来源记录中的交易代码类别选择，沪市使用51/52/53/55/56/58开头的交易代码，深市使用158/159；名称不要求包含“ETF”。指数、LOF和联接基金不会因名字含ETF而混入。源头股票和ETF之外的507条记录保留原响应，并另保存排除行及原因，不能把它们当作已经验证的ETF。

分类核对中的例子有交易所依据：[中航成飞302132公告](https://disc.static.szse.cn/download/disc/disk03/finalpage/2025-03-19/dd95a920-8b43-4643-9405-93047fcddf93.PDF)、[上交所511600货币ETF公告](https://www.sse.com.cn/disclosure/announcement/general/jjzssgg/c/c_20260213_10809590.shtml)、[深交所159003货币ETF说明](https://investor.szse.cn/knowledge/fund/other/t20141209_538861.html)。原行业分类接口的既有范围与对照合同保持，不借这次名单修正改变其他接口。

北交所沿用原验证脚本的代理设置（`STOCK_DATA_HTTP_PROXY`，原本地代理默认值）、`trust_env=false`、同一Session及Cookie、User-Agent、Referer、GET页面、POST分页参数、20/30秒超时、禁止自动重定向、重定向后刷新页面并重试一次，以及页间等待。原逻辑来自 [验证脚本](../../provider_validation/tests/verify_security_board_coverage.py)。不在Provider内做来源回退。

分页增加总数、页号、行数、结束页、唯一证券及返回格式检查。现有基础留证模块增加可选请求体哈希匹配，仅该目录输入启用；缓存和回放同时区分不同POST页参数，保存脱敏后的表单参数，不只按相同地址选响应。其他接口的默认匹配方式保持。

## 字段和日期

[来源字段模板](../../config/datasets/security_snapshot.yaml) 与 [字段映射](../../config/normalization/security_snapshot.yaml) 共用 `stock_code`、`stock_name`、`exchange`、`status`、`trade_date`、`source`，补充 `asset_type`。旧股票候选没有该字段时仍按已有股票默认处理。最终名单仍使用 [security_master.yaml](../../config/datasets/security_master.yaml)。

北交所字段对应 `hqzqdm`、`hqzqjc`，资产类型为 `stock`，交易所为 `BSE`。接口没有提供可靠的停牌状态，因此保存 `status=unknown`，不把存在报价当作正在交易；该状态不阻止已经在名单中的证券进入后续范围。

北交所没有历史名单日期参数，拒绝单证券和历史区间查询。标准字段 `trade_date` 表示原响应在上海时区的采集日；源头行情日期 `hqjsrq` 原样保留，并在报告中另列 `quote_dates`。此前目录采集日为10月7日、行情日期为9月30日；10月8日回放仍保留10月7日。最新10月9日目录与行情日期均为10月9日；不用回放日期或行情日期改写名单日期，跨采集日分页不能拼为同一快照。沪深最新核验明确请求 `trade_date=2026-10-09`，原响应记录与来源候选均保留该日。

## 文件位置及核验

- 原始接口探针：[北交所原响应清单](../../provider_validation/results/raw/security-catalog-bse-20261007/manifest.ndjson)。此处只有来源验证证据；原始响应在解析前留存。
- 分类调查与原脚本结果：[分类核对](../../provider_validation/results/security-catalog-20261007/classification-review.json)、[北交所原脚本结果](../../provider_validation/results/security-catalog-20261007/bse-original-result.json)。
- 当前旧检查的原响应：`data/raw/_tmp/<检查ID>/<单元哈希>/`；按日路径尚待代码修正，目标见 [存储说明](../storage/README.md)。
- 来源候选：`data/task_workspace/security_snapshot/<请求范围>/<运行ID>/sources/<来源>/<输入ID>/`，含来源行、映射JSON、Parquet和质量记录。
- 检查索引和回归报告：`data/task_workspace/_checks/security-catalog/`；运行产物不提交Git。

先前运行仅回放已留证的原响应。10月9日在线Provider候选和官方目录核验仍写在 `provider_validation/results/security-activation-20261009/`，属于已留存历史材料，不能作为正式业务任务完成证据。正式业务已进入开发，继续把联调及资格复核输出放在验证区偏离了本次确认的目录边界；后续此类响应、派生结果及核验报告归业务根目录或隔离实现测试根目录，已有证据只读引用。本阶段没有搬迁或删除历史文件。股票和ETF的SDK载荷可回放，BaoStock不暴露TCP原始帧；北交所HTTP字节与请求体哈希可逐页核验。

原脚本与正式适配器对照请求地址、方法、非秘密请求头、POST请求体哈希、状态、响应哈希和全部证券代码/名称。Cookie在归档中脱敏，无法从离线证据恢复实际值；另外以原生Session及适配器夹具验证Cookie传递、代理、超时和刷新重试。离线通过不等于正式适配器已完成持续在线运行验证。

相关离线回归127项通过，另2项名单日期边界检查通过。独立核对来源响应、请求体与派生Parquet哈希，以及YAML和代码版本；本机索引为 `data/task_workspace/_checks/security-catalog/verification.json`。
