# 项目开发进度

## 当前整改任务（2026-10-30，用户已确认）

新增明确指令已落实：删除代码指纹机制，按实际影响分层验证，并纠正将助手示例归为用户要求的表述。已按[版本与验证合同](provider-capability-verification-and-normalization.md#实现版本记录与验证范围)移除生成及运行阻断依赖；保留原响应、冻结配置和数据完整性校验，不新增替代管理机制。验收覆盖资格、HTTP/SDK缓存、候选续跑、日历导入及隔离业务流程；历史运行与既有报告不改写。

| 删除代码指纹阶段 | 实际范围及状态 | 验证与剩余事项 |
|---|---|---|
| 明确合同并删除实现 | 已修改7个现有源码文件，取消源码摘要/SDK源码快照生成、资格中的源码比对、HTTP/SDK缓存版本匹配及候选源码比对；不新建业务模块 | [107份运行源码复核](tmp/remove-code-fingerprint-20261030/review.json)，旧指纹字段及生成/比较引用为0；配置、范围、时效及数据完整性检查保留 |
| 模块验证 | [34项最终模块测试](tmp/remove-code-fingerprint-20261030/module-final.xml)通过 | 包括不同代码版本复用同一HTTP/SDK响应、日历导入、数据损坏拒绝及验收报告 |
| 采集与任务联调 | 首轮270项为261通过、9失败；本次删除的缩进错误已修复，[留存日线28项重跑](tmp/remove-code-fingerprint-20261030/retained-retest.xml)全通过；其余242项代码路径未变、未重复运行 | 最后删除名单报告源码摘要后，[禁止读取源码的名单发布与失败恢复](tmp/remove-code-fingerprint-20261030/publication-final.xml)通过；入口选测20通过、2项旧TCP标记断言失败，旧入口对照结果相同 |
| 三股实际响应回放 | 2026-10-28三股九组，独立根目录产出3条，续跑无操作，网络尝试0 | [任务回放](tmp/remove-code-fingerprint-20261030/daily-replay.json)，两份正式数据文件哈希未变；不授予资格或启用调度 |

扩大缓存选测17项为8通过、9失败，9项均在[删除前入口对照](tmp/remove-code-fingerprint-20261030/cache-baseline-all.xml)中失败；旧mock对`Session.send`的自动重定向参数预期与已有逐跳捕获不一致。本次不为让旧断言通过而改变传输合同；结果保留在[缓存JUnit](tmp/remove-code-fingerprint-20261030/cache.xml)。历史腾讯日线/5分钟回放各1项在[旧入口对照](tmp/remove-code-fingerprint-20261030/old-input-comparison.json)中也同样失败；另两项BaoStock旧TCP标记断言与已有实现不符，对照见[结果](tmp/remove-code-fingerprint-20261030/old-input-comparison-bao.json)。这些对照使用删除前的入口函数及当前依赖，不代表整套历史环境复现，也不据此判定来源不支持。

本次当前用例去重后实际覆盖343项：330通过、13失败、0跳过；当前1062项中719项未执行本轮完整检查。新增测试最初使用旧腾讯夹具的失败、缺失CSV及Windows深路径问题均保留首轮记录；补回2份Git原始CSV并采用短测试目录后，相应用例通过，不改变业务存储合同。详见[本次核验说明](tmp/remove-code-fingerprint-20261030/README.md)、[覆盖明细](tmp/remove-code-fingerprint-20261030/test-coverage.json)及[机器结果](tmp/remove-code-fingerprint-20261030/review.json)。删除范围已完成并符合设计；未修复上述旧接口/断言问题，未安装依赖、登记资格、启用调度或扩大业务范围，全套测试及M1/M2不据此通过。

## 先前已交付的整改批次

目标是让当前设计、开发状态和验收结论可独立复核。职责与判定合同见[验收基线](stock-data-test-design-acceptance-baseline.md#工程检查与阶段验收的职责合同)。本轮不修改行情字段语义、不登记来源资格、不采集或重做生产数据、不启用调度；中央流程拆分及环境重建只先完成依赖和差距核查。

| 任务 | 范围与依赖 | 状态 | 验收及证据 |
|---|---|---|---|
| 整改-01 文档职责收敛 | 重组现有主干，拆出历史重建与工程决策，保留独有历史内容及原响应 | 已完成，符合本轮职责合同 | [承接表](tmp/engineering-remediation-20261030/document-migration.json)保留10份原文哈希；新增2份、未删除整份原文档；README与CODE_STRUCTURE已同步；[17份主干链接检查](tmp/engineering-remediation-20261030/document-check.json)无缺失及锚点错配，未做图形渲染验收 |
| 整改-02 离线验收判定 | 已确认的组件检查合同，不扩大为全能验收平台 | 已实现并验证，符合设计 | [最终7项测试](tmp/engineering-remediation-20261030/acceptance-delivery.xml)全通过：正常、幂等失败、恢复失败、程序异常、CLI失败退出和重复运行保留前次故障证据；隔离故障目录按时间命名，修复深路径问题；项目关卡仍未通过 |
| 整改-03 名单调度缺陷 | 日历已有记录或未到期时不访问空更新结果 | 已修复并验证 | [调度47项](tmp/engineering-remediation-20261030/scheduler-retest.xml)全通过，含四类分支；首轮新增测试准备目录缺失的3项失败保留，不计为通过 |
| 整改-04 配置与来源清单 | 正式门槛单一维护；历史ID与文件证据分开验证 | 已实现并验证，符合设计 | 读取子YAML唯一日线合同；[目录79项复核](tmp/engineering-remediation-20261030/catalog-retest.xml)通过；历史响应按Git原字节恢复，原跳过的[2项名单回放](tmp/engineering-remediation-20261030/restored-catalog.xml)补跑通过，未重新采集 |
| 整改-05 历史代码指纹与环境评估（已被后续删除指令取代） | 盘点实际依赖、资格失效范围及可复建差距 | 已完成评估，未改失效规则或环境 | [43文件盘点](tmp/engineering-remediation-20261030/fingerprint-inventory.json)；[依赖反例](tmp/engineering-remediation-20261030/dependency-compatibility.json)：原SDK正则处理被当前Pandas/PyArrow组合阻断 |

审查基线为 `05bb3ae`，偏好记录提交为 `78208bc`。本轮验证输出保存在独立 `tmp/engineering-remediation-20261030/`；历史审查、原始响应及正式文件保持原位。各任务收尾分别更新实际结果、设计符合性与剩余事项。

## 上一批核验范围与未通过项

| 检查 | 分母、方法及实际结果 | 结论与证据 |
|---|---|---|
| 测试回归 | 当前收集1059项；完整JUnit按用例去重545项：544通过、1失败、0跳过；514项没有恢复证据后的完整重跑结果 | [覆盖明细](tmp/engineering-remediation-20261030/test-coverage.json)；首轮失败和中断记录保留，不报全套通过 |
| 原SDK新闻对照 | ASTOCK-031的1项，尚未进入项目候选标准化；当前AkShare 1.19.1在Pandas 3.0.6/PyArrow 25.0.0上处理Unicode正则失败 | [失败JUnit](tmp/engineering-remediation-20261030/input-retest.xml)、[最小反例](tmp/engineering-remediation-20261030/dependency-compatibility.json)；需另确认稳定环境与依赖合同，未安装或修改库 |
| 历史证据完整性 | 恢复25份历史Git原文并核19份响应；另恢复960份缺失回放文件、91,355,090字节；逐文件哈希一致 | [原链接恢复](tmp/engineering-remediation-20261030/historical-evidence-restoration.json)、[回放恢复](tmp/engineering-remediation-20261030/replay-fixture-restoration.json)；原路径留存，无覆盖和新采集 |
| 当前代码隔离日线回放 | 2026-10-28沪深北各1只、三口径共9组，产出3条；OHLC与旧正式记录一致，沪深按当前单位合同承接量额、北交所保持空；续跑无操作，网络尝试0 | [本轮任务实测](tmp/engineering-remediation-20261030/daily-replay.json)；不授予在线资格，不代表其他证券/日期 |
| 生产只读复核 | 2个分区数据哈希与审查前一致；9份响应哈希和36个OHLC逐值匹配 | [独立回读](tmp/engineering-remediation-20261030/production-readback.json)；只读既有7267条名单及3条日线，不推定当前日市场覆盖 |

本轮交付报告见[核验说明](tmp/engineering-remediation-20261030/README.md)和[机器汇总](tmp/engineering-remediation-20261030/delivery.json)。本次源代码修复、定向回归、文档整理和依赖评估已经完成；全套回归仍有上述环境阻断，M1/M2均未因此通过。中央执行器拆分、指纹依赖收窄、依赖锁定/环境重建、来源资格登记、全市场生产与历史重建没有实施。

## 当前业务交付状态

| 对象 | 已有结果 | 尚未交付或尚未证明 | 证据 |
|---|---|---|---|
| 证券名单 | 2026-10-09正式7267条（股票5573、ETF1694） | 当前日更新、指数/LOF及任意历史日完整市场覆盖不由此证明 | [发布验收](data/task_workspace/_checks/security-release-20261012/verification.json) |
| 股票三口径日线 | 2026-10-28三股、九组v7正式留存发布，查询回读一致 | 其他日期、全市场及持续生产尚未完成 | [正式验收](data/task_workspace/_checks/tencent-v7-formal-three-stock-2026-10-28/README.md) |
| v8单位合同 | 九份来源零联网回放与三条隔离聚合，当前合同覆盖内量额换算 | 未重做旧正式数据，未登记新资格；北交所等未覆盖量额仍空 | [留存验收](data/task_workspace/_checks/unit-contracts-20261030/README.md) |
| 旧全量日线 | 最后持久化摘要仍collecting且未发布 | 不代表进程存活或全量完成 | [自动摘要](data/task_workspace/2026-10-09/daily_bar/summary.json) |
| 分钟、快照与其他研究输入 | 有组件、配置及不同层级离线验证 | 常驻运行、时效、各数据产品正式闭环须分别验收 | [任务合同](docs/pipeline/collection-tasks.md)、[来源说明](docs/providers/README.md) |
| 历史重建 | 目标合同已保留并独立归档 | 事件连接、换版确认及多日期统一切换未完整实现 | [历史重建设计](docs/pipeline/daily-history-rebuild.md) |

代码已实现、限定范围已验证、数据已正式发布、当前来源有资格和持续调度已启用是不同判断。当前日线调度关闭；来源资格按接口、参数、配置、有效期与业务证据判断；历史样本不授权扩大范围，源码变化不再自动使资格失效。M1/M2总要求继续以[验收基线](stock-data-test-design-acceptance-baseline.md)为准。

## 已知问题与后续依赖

| 问题 | 后续动作与边界 |
|---|---|
| 离线工具误报关卡、名单调度分支、配置和清单测试 | 本轮已修复并按上表验证；不代表全部业务能力已验收 |
| 当前正式来源资格 | 本轮不登记；业务上线前按新代码/配置及实际范围复核 |
| ETF/LOF/指数适用性、全市场容量和连续交易日 | 独立业务验收，不由三股证据推定 |
| 代码指纹机制 | 已按本页当前任务删除生成及运行依赖；中央流程重构不在本次范围 |
| 环境可重建和CI | 当前指定解释器依赖齐全，但AkShare新闻对照在Pandas 3.0.6/PyArrow 25.0.0的正则处理中失败；锁定依赖、稳定版迁移和CI另明确环境合同，不擅自安装；不能据此宣称全套测试通过 |

## 已有证据与历史决策

当前职责见[总体设计](STOCK_ANALYSIS_V2_MARKET_DATA_CENTER_ARCHITECTURE_V1.md)，重要取舍和被替代合同见[工程决策](docs/engineering-decisions.md)。2026-10-09至30日的完整历史过程保存在Git提交 `78208bc`；原始响应和验收报告不删除、不改写。历史通过数只证明对应版本及样本，不计入本轮测试。

用户答复证据与独立判断偏好已在 `78208bc` 记录于[AGENTS](AGENTS.md#答复形式与独立判断)，当前整改范围后经用户明确确认。原工程审查基线为05bb3ae，见[脚本](tmp/engineering-review-20261030/review.py)和[结果](tmp/engineering-review-20261030/report.json)。

2026-10-30在`b199393`按用户明确要求记录防止过度设计、设计缺口先举例讨论及分层验证约束，见[协作约定](AGENTS.md#设计先行与领域建模)。该提交只更新规则，未改业务代码；后续用户明确要求删除代码指纹，现已按本页当前任务完成删除及对应验证，旧规则记录不能替代本次实现结果。
