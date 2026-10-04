# 新浪复权因子候选输入

本批仅转换 ASTOCK-006，扩展既有 `providers/sina/daily.py` 的新浪日线适配器，不新建 Provider、管理层或调度系统。既有股票日线和期货日线方法保持；`client` 为可选关键字参数，不改变旧位置参数。

## 配置与来源

`config/providers.yaml` 声明参数 `code` 来自 `request.symbol`，`kind` 来自 `config.kind`（默认 qfq，可选 hfq）。仍引用 `market_after_close` 策略，调用间隔3秒、并发1；每次原脚本固定前复权后后复权两次请求，因此 `base_requests_per_fetch=2`。窗口参数禁用，源 SDK 此分支返回完整历史，不能虚构历史区间查询。

`config/datasets/adjustment_factor.yaml` 定义字段类型和主键；`config/normalization/adjustment_factor.yaml` 定义来源字段到标准字段的映射。`kind` 只决定候选输出哪组：两个精确原始响应、全部66条来源数据、选中33条解析与另外33条排除记录均保留。原系数字符串不经浮点转换，保留末尾零和全部精度。

日期字段 `factor_date` 是来源标签。`1900-01-01` 保留为来源基准标签，不当作真实交易日、除权事件或上市时间。经济计算口径尚未独立核验，标准数值 `factor_value` 为空，`raw_factor` 保留原值。本批不据此重算价格。

## 原行为和验证边界

保留成功脚本调用 `stock_zh_a_daily(symbol=..., adjust="qfq-factor")` 再 `hfq-factor` 的顺序、请求 URL、默认请求头、环境代理、无显式超时、SDK 解析和探测保守重试/主机暂停规则。原始响应保存后再检查 JavaScript 赋值内容：仅允许原格式 JSON 数据和观察到的尾部块注释，检查变量、字段顺序、条数、日期、重复和有限系数。该检查发生在原 SDK `eval` 之前，合法响应原字节原封不动交给 SDK。

原成功范围仅茅台 `sh600519`，qfq/hfq 各33条；本批复用持久化归档，没有重新访问来源。实际采集入口、原脚本与历史 CSV/响应逐字段对照、异常响应、YAML字段投影及映射、旧股票接口、注入会话/证据缓存均在离线或模拟模式执行。不能将这些检查报告成当前在线、全股票覆盖或完整经济口径验证。

修改前状态与源引用：`results/factor-original-20261004/`；开发失败/修正检查：`results/factor-smoke-20261004/` 和 `factor-tests-dev*-20261004*`；最终对照：`results/factor-final-20261004/`；真实命令入口回放：`results/factor-cli-20261004/`；完整回归为 `results/factor-final-v2-20261004-tests.xml`，哈希索引为 `factor-final-20261004-verification.json`。中间失败证据保留，不混作最终结果。

首次完整回归把临时目录放入深层证据目录，四个旧用例触发 Windows 路径限制；失败 XML 保留为 `factor-final-20261004-tests.xml`。较短目录重跑完整509项全部通过，定位检查见 `factor-path-20261004-tests.xml`。首次回归的完整临时夹具仍本地保存在 `factor-fulltests-20261004/`；四份失败报告和XML单独提交，其余大量重复旧证据不纳入本次代码提交。最终本批候选/异常/命令入口证据均单独提交。未调整生产存储实现来绕过测试路径问题。

自动调度、正式生产路由和生产目录写入保持关闭。转换后39项为候选实现、25项待转换、7项阻断、3项别名。提交按项目要求创建，当前工作区没有配置预期的 post-commit 自动推送钩子，未另行推送。
