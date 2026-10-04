# 新闻输入转换与验证

当前共74项声明：27项已实现候选采集、37项尚未实现、7项阻塞、3项别名。此次仅迁移ASTOCK-034与ASTOCK-035，自动调度和正式路由均关闭。

华尔街见闻A股频道归档50条，原User-Agent、requests.request、环境代理、重定向、10/40秒超时和零重试保留。原无效频道默认值global修正为已验证的a-stock-channel；候选配置仅接受该频道，limit可配置1至100，metadata.cursor可传入。游标值保留，但后续页仅模拟检查，没有跨页真实完整性认证。标题空串、正文、标签、重要性及北京时间与原脚本一致；完整原始条目仍保留。

新闻联播2026-09-18归档14条标题和链接。原脚本三种标题位置、整期节目排除、HTML转义及协议补全保持；候选with_content默认False且只允许False，正文抓取没有来源证据。沿用request.trade_date参数名，但其含义是日历播出日，不要求交易日。周末和旧页面结构仅模拟验证。

字段与采集参数由现有config/providers.yaml、datasets、normalization配置；仅因架构原无对应来源适配器新增providers/wallstreetcn/news.py与providers/cctv/news.py，不创建服务或管理层。snapshot_at取原响应捕获时间，原脚本临时运行生成的fetched_at仅存原解析证据；source/source_url等元数据由采集报告保存。

验证证据：results/news-original-20261004保存调查与基线，news-final-20261004保存原脚本与Provider对照、字段投影、异常响应、重复记录、404、游标和周末模拟、注入会话与缓存，news-cli-20261004保存实际入口回放。news-final-20261004-tests.xml为380项回归，news-final-20261004-verification.json为最终哈希索引。原验证入口扩展--verify-news。

所有响应在解析前持久保存精确应用字节；重复使用2026-10-01来源归档，没有新增真实网络请求或生产写入。异常消息只记录类别，避免代理地址等敏感内容泄露。当前源可用性、其他频道、全部历史及央视正文均未认证，不自动加入正式路由。
