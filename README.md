# Stock Data Manage

证券数据采集与管理项目。当前股票日线以“证券+交易日”一条聚合记录保存不复权、前复权和后复权三组数据，旧单口径数据保持原样。2026-10-28的沪深北三只样本已完成v7正式留存发布与只读查询验收。2026-10-30单位规则升级为v8并完成留存响应隔离验收；旧正式文件保留，当前资格和单位适用性依接口、配置及业务证据核验，未启用在线采集或调度。指数名单及其他资产的正式能力仍待核验。现有运行台只读，任务操作与覆盖质量页面尚待开发。最新状态和验收证据见[项目进度与待办](PROJECT_PROGRESS.md#当前业务交付状态)。

| 需要了解什么 | 文档入口 |
|---|---|
| 总体架构、术语和模块边界 | [总体设计](STOCK_ANALYSIS_V2_MARKET_DATA_CENTER_ARCHITECTURE_V1.md) |
| 已确认的任务与数据对象设计及当前日线实现 | [逻辑架构和对象关系](STOCK_ANALYSIS_V2_MARKET_DATA_CENTER_ARCHITECTURE_V1.md#职责对象关系与阶段合同)、[日线聚合实现状态](stock-data-design-qfq-history-rebuild.md#实施现状2026-10-30)、[任务流程](docs/pipeline/collection-tasks.md#已确认设计与当前实现2026-10-28) |
| 当前交付状态、对象关系及执行合同 | [当前交付](PROJECT_PROGRESS.md#当前业务交付状态)、[核心流程框图](docs/pipeline/collection-tasks.md#已确认设计与当前实现2026-10-28) |
| 现有目录和代码负责什么 | [代码结构](CODE_STRUCTURE.md) |
| 输入参数、采集字段、映射及范围 | [源头接口说明](docs/providers/README.md)、[股票与ETF来源](docs/providers/security-catalog.md) |
| 原始暂存、检查、发布、重做和证券清单调度 | [任务流程](docs/pipeline/collection-tasks.md) |
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

来源可行性证据位于 `provider_validation/results/`。通用输入入口 `collect-input` 默认将原响应保存到 `data/raw/`，来源解析和标准化结果保存到 `data/task_workspace/`，元数据库保存到 `data/metadata/metadata.duckdb`。来源输入先生成候选；`collect-task` 对证券主数据和单交易日日线接通暂存、检查、重做及发布，正式执行默认使用配置中的 `data/`。本机已保留2026-10-09证券名单与旧日线样本发布；当前三口径股票日线的小范围正式状态见上方进度入口。后续任务仍须按接口合同、配置、有效期和请求范围核验资格；v7曾授予精确留存资格；v8单位规则升级后须重新复核，在线与后台日线调度未启用。离线回放须显式指定独立的 `--data-root`，业务任务输出禁止放入 `provider_validation/`。详见 [任务流程](docs/pipeline/collection-tasks.md) 和 [存储与归档说明](docs/storage/README.md)。显式 `--output-root` 保留隔离验证布局；历史报告中的旧路径及哈希不改写。

2026-10-09确认的开发边界进一步限定：`provider_validation/` 仅服务正式实现前的接口可行性确认；进入业务开发后，业务原始响应、Provider联调、来源复核及端到端验收都使用 `data/` 或 `tmp/` 下独立运行布局，不向该验证区新增产物。历史证据只读保留，业务代码位于 `src/`、实现测试位于 `tests/`。

已按确认的简化目录修改现有代码：raw为 `<日期>/<来源>/<接口>/`，请求以文件名区分；工作区为 `<日期>/<数据集>/`，自动保存任务参数和摘要；正式数据按日期发布，最新名单由元数据索引选择。当前目录接口按原捕获日落盘；回退仅引用旧发布，不改写旧日期文件。旧业务目录已迁移并保留152个原文件的审计副本，10月9日五来源名单已完成正式留存流程发布。接口、参数、单位或配置合同变化时按实际影响复核；不再因源码文件变化自动阻断。详见 [存储规则](docs/storage/README.md) 与 [项目进度](PROJECT_PROGRESS.md)。

## 日线当前实现范围（2026-10-30）

股票日线由 YAML 声明任务和采集策略，分别保留三口径响应，再聚合成“证券+交易日”一条记录；页面默认显示不复权，可切换已存口径。当前已使用真实v7响应完成2026-10-28三只股票的隔离重做、恢复和正式留存发布，规范日线查询逐组回读通过；历史正式数据的量额仍为空；v8已按来源、证券类型/代码段和实际序列换算覆盖范围内的量额，完成留存回放及隔离聚合验收，尚未重做正式数据或登记新的生产资格，持续调度关闭。ETF、LOF、指数的适用口径和聚合发布仍待验收。旧单口径数据及其他正式文件保持原样。详见[聚合设计和实施现状](stock-data-design-qfq-history-rebuild.md)及[项目进度](PROJECT_PROGRESS.md)。

## 开发与验收入口

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
