# 按实际数据修正剩余输入

用户明确要求“以实际的数据为准，而不是看之前的定义”。本次据此修正来源、范围和名称，保留历史验证记录，把实际成功子接口接入现有候选采集流程。

| 输入 | 实际数据及对照 | 改动 |
|---|---|---|
| ASTOCK-014 | 原SDK返回375个概念名称和代码；41次响应包含401及重复页面 | 修改现有板块Provider，改为概念目录；无热度、炒作原因或成员关系，不认证目录完整性 |
| ASTOCK-037-profile | 600519，公司概况1条，原26个输出字段 | 必要巨潮来源适配器；保持原SDK、动态请求头及原字段顺序；注册资金原值保留，单位未认证的标准金额为空 |
| ASTOCK-037-events | 原实际请求2023-08-08，全市场动态75条 | 修改现有东财财务Provider；必须明确来源日期，全市场范围，不作600519个股F10 |
| ASTOCK-044 | BaoStock code_name=ST返回317条，原规则筛选198条 | 修改现有BaoStock Provider和SDK证据捕获；沪深活跃股票名称筛选，无北交所、报价或官方风险标志 |
| ASTOCK-087 | 原SDK查询2026-10-01“全部”公告，718条，9次请求 | 修改现有东财资讯Provider；全市场指定公告发布日期，完整来源分页，无股票过滤和正文 |

ASTOCK-037保留为历史复合验证记录，三个实际子接口独立配置，原失败的个股信息和主营构成不接入。配置共77条，包含3条F10子输入；64条可执行验证候选、2条未实现、8条不可执行记录、3条别名。8条不可执行记录包含旧F10复合记录，不能解释为8个来源失败。

## 配置与代码

`config/providers.yaml`管理来源、参数来源、范围、采集档案和调用方法。五套`config/datasets/{concept_directory,company_profile,company_events,st_name_list,dated_announcements}.yaml`定义字段和主键，对应`config/normalization/`文件管理字段映射。映射修改及字段投影已实际执行验证。采集档案保持禁用调度，生产路由资格不变。

扩展原`providers/akshare/boards.py`、`providers/baostock/industry.py`、`providers/eastmoney/{financial,news}.py`。巨潮此前没有适配器，因此只补必要的`providers/cninfo/{__init__,profile}.py`，不另建服务、管理器或配置系统。SDK内存缓存没有对应响应证据时不会生成候选。原Session、参数、代理、重试和解析保持；未在Provider内增加来源或日期回退。

## 实际证据和剩余问题

调查、修改前快照、授权及复用的317条BaoStock载荷见`provider_validation/results/actual-data-original-20261004/`。BaoStock是完整保存的SDK外部ResultSet解码表示，不是TCP线缆帧；317条来源行全部保存，198条筛选结果另存。

公告新探针在`actual-data-network-20261004/ASTOCK-087/`，解析前保存9个精确HTTP应用载荷，仍返回718条。`actual-data-live-20261004/`的三次沙箱失败为本地WinError10013权限限制，不能用作来源不可用证据。

两项仍缺可回放的原始响应，保持未实现，不把旧CSV或HTTP200当作正式Provider验证：

- ASTOCK-011：实际是SDK默认“预测年报每股收益”，旧CSV含3个年度的机构数及预测统计。原HTML因凭据类字面量检测仅保留哈希；原SDK新请求120秒未返回，由外部限时器结束。
- ASTOCK-037-business：旧600519样本有1条主营业务、产品和经营范围；原HTML同样只有哈希。原SDK补采也超过120秒，由外部限时器结束。

全证据目录再次检索，7条相关请求记录中没有可恢复的成功HTML，见`actual-data-original-20261004/ths-archive-search.json`。限时结束只说明探针未取得响应，不代表来源失效；没有改变SDK timeout、网址、协议、代理或日期。安全探针解析前留存响应；遇到凭据类字面量只保存脱敏表示、原字节哈希及明确关联，不保存原凭据。

## 验证

原验证入口新增`--verify-actual-data`，独立执行原SDK/原ST分支，与正式候选逐字段、逐请求比较。五个输入的原字段、日期和参数一致；17种状态、字段顺序、身份、日期、页数、行数、缺页及SDK错误夹具均拒绝输出并保留证据。YAML映射修改和投影通过。

最终对照和错误证据见`actual-data-final-20261004/comparison.json`。完整回归日志、XML、版本和哈希审计为同前缀文件，完整回归645项通过，无失败、错误或跳过，数量经XML核对。回放禁止外网，无生产数据写入；不认证当前全部来源在线、单位、独立全量覆盖、持续容量或正式路由资格。
