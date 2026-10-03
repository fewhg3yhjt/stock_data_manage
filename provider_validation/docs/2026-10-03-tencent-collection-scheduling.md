# 腾讯输入采集配置与调度实现

## 默认配置

| 输入 | 范围 | 触发方式 | 当前执行资格 |
|---|---|---|---|
| 行情快照 ASTOCK-001 | 本地证券清单中当日已上市股票，沪、深、北三市；不混入基金、指数 | 每交易日15:10一次 | 原批量Provider存在；新输入迁移、传输等价性及在线批量验证未完成，调度阻断 |
| 前复权日线 ASTOCK-002-daily | 明确传入的证券集合，暂不默认全市场 | 每交易日16:20一次 | 已有候选输入；默认不自动运行 |
| 最近5分钟线 ASTOCK-002-5m | 明确传入的自选证券集合 | 交易时段每5分钟边界后5秒 | 已有候选输入；默认不自动运行 |
| 原生1分钟线 | 自选范围策略模板 | 交易时段每分钟边界后2秒 | 尚无独立验证的新输入契约绑定，不因5分钟验证通过而启用 |

上述策略的 `scheduling_enabled` 均为false。新调度路径不修改既有固定任务和实时分钟生产流程，也不增加服务、管理台或正式路由资格。

## 在哪里调整

`config/collection.yaml` 的 `collection_profiles` 管理采集周期。快照策略示例：

```yaml
tencent_market_snapshot:
  mode: after_close
  frequency:
    unit: day
    interval: 1
    at: "15:10"
  business_day: trading_day
  universe: all_stock
  scheduling_enabled: false
```

`unit: day` 表示按天，`interval: 1` 表示每天；`at` 是北京时间的采集时刻。大于一天的间隔必须同时给出 `anchor_date`，按日历日取模，再检查明确的交易日清单；不是每N个交易日。未覆盖的日期一律不启动，不凭星期推断交易日。

改为 `unit: minute`、`interval: 10` 即按交易时段每10分钟触发，仍保留 `universe: all_stock`。此时 `at` 不参与分钟计算，实际模式根据单位变为intraday。分钟周期从每段交易时段开始对齐，第一次触发在第一个间隔结束后；午休、隔夜不补发过期周期。`settle_delay_seconds` 是边界后等待秒数；`trigger_grace_seconds` 默认30秒，执行者须在这段触发窗口内检查，例如每10秒检查一次。该等待不代表来源Bar时间语义已验证，不改写来源时间戳。

`refresh_interval_seconds` 对新策略由频率派生，不能同时在YAML重复设置。旧通用模板尚未绑定可执行周期，保持原状态。未来管理台可写这些既有配置字段；当前每次命令调用重新读取配置，无后台热更新或UI。

`config/providers.yaml` 仍管理参数来源、接口及请求间隔，每次请求至少间隔3秒、并发1。采集周期和请求间隔分别控制业务刷新与来源访问速度。`datasets/` 和 `normalization/` 继续管理标准字段及来源映射；日线和5分钟线沿用既有实现。快照标准输入迁移未完成，因此没有虚设已实现的字段映射。

## 已实现的执行边界

`stock-data collect-due-inputs` 是既有命令行中的单次调度检查，默认只保存计划，不访问网络。提供 `--calendar-file`，自选/静态范围使用可重复的 `--symbol`；全市场快照范围使用 `--securities-file`，格式为现有SecurityRecord的JSON数组。证券清单与日历的原文件路径、哈希及实际解析范围会留证。全量仅指遍历传入的本地证券清单，不能证明该清单已完整覆盖当天市场。

```powershell
stock-data collect-due-inputs --now 2026-09-30T15:10:00+08:00 --calendar-file path/to/calendar.json --securities-file path/to/securities.json --output-root provider_validation/results/my-schedule-plan
```

执行需显式 `--execute`，并且策略启用、输入已有候选适配器、证券范围明确且通过容量检查。回放默认需要 `--replay-manifest`，在线执行需 `--mode live` 且使用当前时间。配置开关无法绕过快照迁移门禁。日线调度向原fetch_window传入当天start=end，复权仍为原qfq；不自动扩成历史补采。日线单日实时请求的原实现对照仍需小样本确认。

状态复用现有DuckDB的collection_attempt表，保存在候选输出目录内，不使用生产元数据库。日频任务按输入＋日期占用，改当天时间不会重复；分钟任务按输入＋边界占用。每次占用先持久化，然后调用原collect_input，成功保持候选VALIDATED，绝不标为PUBLISHED。同一周期失败或中断不自动重试，需要检查证据后人工处理；下个新周期可重新尝试。应固定使用同一个候选输出目录作为调度状态目录，运行期间数据库锁限制并发执行，不创建第二个调度实例。

自选数量超过限制时拒绝，不截取前N只。逐证券请求的成功路径预算放不进分钟周期时拒绝；单个来源请求/回退可能超时，执行期间也检查剩余预算，超时不会继续启动后续证券。200只×3秒约10分钟，不满足5分钟周期。未来全市场分钟快照也需要批量容量证据，不能只修改频率开关就宣称可持续运行。

## 验证与待完成项目

最终证据见 `results/input-scheduling-accepted-20261003/comparison.json` 及 `scheduling/verification.json`。四项既有输入继续与归档原实现对照；调度入口实际回放5分钟候选96行，并验证重复周期不重复执行。单证券快照以原响应SHA-256核对解码字段与原CSV，203只分批测试使用该响应合成的离线夹具，原始夹具和派生行均保存；不把它当作203只实时验证。

快照修正为原成功脚本的GBK解码，避免中文名称乱码；传输层、会话、请求头、重试与完整在线批量对照仍待完成。也未完成全市场在线覆盖、1分钟独立输入、分钟Bar完成时点及单位语义认证、日线单日实时迁移对照、生产发布、历史补采、分布覆盖、后台进程和管理台。所有候选报告明确production_writes=0、eligible_for_production_routing=false。

中间批次保留而不改写。verified/release批次在响应归档时触发Windows路径长度限制，表现为FileNotFoundError；pathcheck重现的响应临时文件绝对路径为263字符。这属于存储执行问题，不判为来源不可用。新候选运行目录改用短唯一ID，时间仍保留在响应清单和报告；增加深目录回放回归检查。最终accepted批次通过，其他中间结果不作为最终验收依据。
