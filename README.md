# Stock Data Manage

证券数据采集与管理项目。当前日线以“证券+交易日”一条聚合记录保存不复权及可用的前/后复权口径；不复权是发布必需数据，缺少复权值时保留未核验空槽，不复制价格。旧单口径数据保持原样。2026-10-28的沪深北三只样本已完成v7正式留存发布与查询验收。2026-10-30单位规则升级为v8并完成留存响应隔离验收。2026-11-04正式日线任务已运行：股票5,570/5,573条、ETF 1,717/1,717条已发布；指数62条未得到目标日记录，按既有覆盖门槛阻断，任务`complete_today=false`。两项BaoStock证券清单请求已分别在60秒处超时，未切换代理或来源；本次完整日线任务通过既有到期任务入口手动执行，不能证明`run-scheduler`进程连续运行。Windows登录启动任务已于2026-11-05移除，当前由操作员手动启动；未实现调度进程退出后的自动重启。固定日线业务任务按配置使用既有来源，并保留健康、原始响应、数据质量和正式发布检查。最新状态和证据见[项目进度与待办](PROJECT_PROGRESS.md#当前业务交付状态)。

| 需要了解什么 | 文档入口 |
|---|---|
| 总体架构、术语和模块边界 | [总体设计](STOCK_ANALYSIS_V2_MARKET_DATA_CENTER_ARCHITECTURE_V1.md) |
| 已确认的任务与数据对象设计及当前日线实现 | [逻辑架构和对象关系](STOCK_ANALYSIS_V2_MARKET_DATA_CENTER_ARCHITECTURE_V1.md#职责对象关系与阶段合同)、[日线聚合实现状态](stock-data-design-qfq-history-rebuild.md#实施现状2026-10-30)、[任务流程](docs/pipeline/collection-tasks.md#已确认设计与当前实现2026-10-28) |
| 当前交付状态、对象关系及执行合同 | [当前交付](PROJECT_PROGRESS.md#当前业务交付状态)、[核心流程框图](docs/pipeline/collection-tasks.md#已确认设计与当前实现2026-10-28) |
| 现有目录和代码负责什么 | [代码结构](CODE_STRUCTURE.md) |
| 输入参数、采集字段、映射及范围 | [源头接口说明](docs/providers/README.md)、[股票与ETF来源](docs/providers/security-catalog.md) |
| 原始暂存、检查、发布、重做和证券清单调度 | [任务流程](docs/pipeline/collection-tasks.md) |
| 日线整批准备、采集、处理与按类型发布 | [批次阶段合同](docs/pipeline/collection-tasks.md#日线批任务阶段合同2026-10-31用户确认)、[本轮进度与验证](PROJECT_PROGRESS.md#日线整批分阶段改造2026-10-31实施中) |
| 数据在哪里、文件名与内容示例、字段解读及归档 | [存储文件说明](docs/storage/README.md) |
| 在本机查看任务、原始证据和发布结果 | [只读运行台](docs/service/read-only-web.md) |
| 用 Docker 部署只读运行台并更新代码 | [Docker 部署说明](docs/service/container-deployment.md) |
| 来源资格、证据与归一化设计 | [来源专项设计](provider-capability-verification-and-normalization.md) |
| 日线记录、价格/点位类型及消费 | [多口径日线合同](stock-data-design-qfq-history-rebuild.md) |
| 历史变化、人工重建与跨日期切换（待实现） | [历史重建合同](docs/pipeline/daily-history-rebuild.md) |
| 为什么改变设计、旧合同由什么替代 | [工程决策](docs/engineering-decisions.md) |
| 怎么判断功能完成和数据可用 | [验收基线](stock-data-test-design-acceptance-baseline.md) |
| 开发协作与文档维护要求 | [AGENTS.md](AGENTS.md) |

[2026-10-03历史能力矩阵](PROVIDER_CAPABILITY_MATRIX.md)只用于追溯旧样本和证据，不作为当前能力入口。

## 目录与文档导航

目录和文件的详细职责见 [代码目录与文件职责](CODE_STRUCTURE.md)，文档归属规则见 [项目协作要求中的文档管理](AGENTS.md#文档管理)。

| 目录或文件 | 用途 |
|---|---|
| `src/` | 按 providers、routing、quality、pipeline、storage、service、worker 等职责组织的正式实现 |
| `config/` | 来源参数、采集频率、路由、数据集字段模板及归一化映射配置 |
| `tests/` | 实现的自动化测试 |
| `docs/` | 按模块职责归档的正式说明；各子目录随文档产生建立 |
| `provider_validation/` | 正式实现前的来源可行性验证脚本、原始返回、覆盖报告及结论；历史研究材料已归档至 `results/legacy/tmp_test/`，业务开发后只读引用已有证据，入口见 [Provider 验证目录说明](provider_validation/README.md) |
| `AGENTS.md` | 协作、验证及文档管理约定 |
| `data/` | 统一运行根目录：`raw/`、`task_workspace/`、`canonical/`、`task_archive/`、`metadata/` 和 `hot/`；按需生成，不提交 Git，详见 [存储与归档说明](docs/storage/README.md) |
| `tmp/` | 本地临时工作、正式实现的隔离回放、联调与故障验收；按独立运行根目录分层输出，不能冒充真实生产数据 |
| `CODE_STRUCTURE.md` | 当前代码和文档目录的职责索引 |

正式文档的归属如下。当前已建立源头接口、任务流程和存储文档；证券清单调度并入任务流程。其他模块目录按需要建立，现有总体及专项设计保持原位，入口见本文开头。

| 文档目录 | 内容 |
|---|---|
| `docs/providers/` | 源头采集接口、调用参数、来源字段、采集限制及接入状态 |
| `docs/routing/` | 来源选择、补缺与回退 |
| `docs/quality/` | 标准字段、归一化与质量检查 |
| `docs/pipeline/` | 数据构建、合并、重做与发布 |
| `docs/storage/` | 数据存储与归档 |
| `docs/service/` | 面向业务的数据查询接口 |
| `docs/worker/` | 任务调度、执行与恢复 |

正式说明表引用当前代码、YAML 和验证证据；修改说明表不会修改运行配置。能力已实现、已验证的范围、生产路由资格与调度启用状态分别记录。

正式代码直接放在 `src/` 下，模块按职责分目录，不再套一层项目同名目录。Python 导入名仍为 `stock_data_manage`，由 `pyproject.toml` 将该包映射到 `src/`；`stock_data_manage.providers` 对应 `src/providers/`。根目录 `config/` 保存 YAML，`src/config/` 保存读取和校验配置的代码。

来源可行性证据位于 `provider_validation/results/`。通用输入入口 `collect-input` 默认将原响应保存到 `data/raw/`，来源解析和标准化结果保存到 `data/task_workspace/`，任务元数据库保存到 `data/metadata/metadata.duckdb`。证券主数据通过 `collect-task` 更新到 `data/canonical/security_master/current/security_master.duckdb` 这一份当前清单；完整来源成功时写入状态及字段变更历史，失败时保留现状。日线任务使用目标日以前最近一份成功完整的清单；当天刷新失败不会把现有证券改成无效，也不会阻止仍有可用清单的日线继续执行。`run-scheduler`是每30秒读取YAML周期的前台Python轮询进程，当前由操作员手动启动；它不是Windows登录任务或项目内服务，看门狗也未实现。登录触发任务已于2026-11-05从本机Windows任务计划程序移除。手动启动命令及停止行为见[任务流程](docs/pipeline/collection-tasks.md)。离线回放须显式指定独立的 `--data-root`，业务任务输出禁止放入 `provider_validation/`。显式 `--output-root` 保留隔离验证布局；历史报告中的旧路径及哈希不改写。

2026-10-09确认的开发边界进一步限定：`provider_validation/` 仅服务正式实现前的接口可行性确认；进入业务开发后，业务原始响应、Provider联调、来源复核及端到端验收都使用 `data/` 或 `tmp/` 下独立运行布局，不向该验证区新增产物。历史证据只读保留，业务代码位于 `src/`、实现测试位于 `tests/`。

已按确认的简化目录修改现有代码：raw为 `<日期>/<来源>/<接口>/`，请求以文件名区分；工作区为 `<日期>/<数据集>/`，自动保存任务参数和摘要；日线正式数据按日期发布，证券名单则维护唯一当前态数据库，历史日期清单不再新增。来源失败不会修改当前名单，完整刷新成功才更新状态和变更记录。旧业务目录已迁移并保留152个原文件的审计副本，10月9日五来源名单作为历史证据保留。接口、参数、单位或配置合同变化时按实际影响复核；不再因源码文件变化自动阻断。详见 [存储规则](docs/storage/README.md) 与 [项目进度](PROJECT_PROGRESS.md)。

## 日线当前实现范围（2026-10-31）

股票、ETF和已纳入当前证券清单的指数日线均按YAML任务及固定输入配置逐证券采集，再聚合为“证券+交易日”记录；实际完成范围和发布状态以各任务摘要为准。`run-scheduler`由操作员手动启动，每30秒轮询并同步分派到期业务任务；当前未配置Windows登录触发或项目内进程看门狗，进程退出后需再次手动启动。2026-11-04正式日线入口已运行：股票5,570/5,573、ETF1,717/1,717分区发布；指数495/557，因62条缺失超过现有覆盖门槛而阻断，整批未完成。详见[聚合设计和实施现状](stock-data-design-qfq-history-rebuild.md)、[任务流程](docs/pipeline/collection-tasks.md)及[项目进度](PROJECT_PROGRESS.md)。

本次已完成[配置驱动的日线展示整改](docs/service/read-only-web.md#展示字段合同与整改实现2026-10-30已确认并验收)：列映射以[展示YAML](config/presentation.yaml)为准，来源映射与单位换算仍归原配置；[本轮实际结果与截图](tmp/presentation-20261029/README.md)限定三股单日，不扩展为各类型生产验收。

## 开发与验收入口

2026-10-31已依据用户截图及成交量单位确认，补齐纬达光电目标日三口径量额，当前页面读取新的v9隔离根。具体值、精度边界及实际范围见[北交所补证](docs/service/read-only-web.md#北交所量额补证与展示2026-10-31)，不改写旧发布或授予全市场资格。

当前交付、任务依赖、阻断和实际证据统一见[项目进度](PROJECT_PROGRESS.md)；本页不另维护组件完成长列表。源头候选成功、隔离回放、正式发布、当前资格及常驻运行须分别判断。

本次三股单日的[采集→处理→隔离发布→页面完整验收](tmp/daily-flow-20261029/README.md)包含逐阶段数据、测试及浏览器截图；[执行与复现命令](docs/pipeline/collection-tasks.md#本次验收的执行与复现入口)沿用现有业务入口。证据保留在本机隔离目录，不将当前样本扩展为全市场或持续生产结论。

运行合成组件自检（仅写独立tmp根目录，不访问来源）：

```powershell
& 'D:\Program Files\Python\python.exe' -m stock_data_manage.cli acceptance-offline --root tmp/offline-components --output tmp/offline-components/report.json
```

该报告只检查20个模拟工作日的单股存储重放、一处发布恢复和200只观察池预算。逐项检查成功不等于项目Gate C/D/E全部通过；源码与静态检查、真实来源、全资产回放、完整故障矩阵、实测时效及连续交易日均需各自证据。组件失败时CLI返回非零码。

工程决策见[决策记录](docs/engineering-decisions.md)，未来历史重建见[独立合同](docs/pipeline/daily-history-rebuild.md)。历史能力矩阵仅为指定日期证据索引。

## 本地验证

首次使用先安装当前项目：

```powershell
python -m pip install -e .
```

安装登记现有源码及 `stock-data` 命令入口。执行命令、测试和来源验证脚本时使用同一个 Python 环境；不再依靠将 `src/` 添加到 `PYTHONPATH` 启动。目录迁移后，已有开发环境也需重新执行上述安装命令。需要安装测试依赖时使用 `python -m pip install -e ".[test]"`。

只读查看现有任务、原始响应和发布结果时，安装 Web 可选依赖并启动本机运行台：

```powershell
python -m pip install -e ".[web]"
stock-data-web
```

浏览器访问 `http://127.0.0.1:8765`。“数据查询”可按数据类型、证券代码或名称、日期区间预览来源记录，并查看对应原始响应。该页面只读取现有 `data/` 产物，不启动、停止或修改采集任务；状态解释、日期口径、原始响应脱敏及后续云端部署边界见[只读运行台说明](docs/service/read-only-web.md)。

```powershell
& 'D:\Program Files\Python\python.exe' -m pytest
```

数据集合同位于 `config/datasets/`，来源映射位于 `config/normalization/`，接口参数位于 `config/providers.yaml`，采集策略位于 `config/capabilities.yaml`。各来源的历史样本、当前实现、生产资格和调度状态分别查看[来源说明](docs/providers/README.md)与[项目进度](PROJECT_PROGRESS.md)，不由适配器存在或安装成功推定可生产。当前指定解释器与每次测试前的依赖核对要求见[项目环境](AGENTS.md#项目python环境与测试执行)；本轮全套回归的依赖兼容阻断见项目进度。
