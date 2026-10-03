import fs from "node:fs/promises";
import path from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

const repo = process.cwd();
const visualDir = process.env.CODEX_VISUALIZATION_DIR;
if (!visualDir) throw new Error("Set CODEX_VISUALIZATION_DIR to the current conversation visualization directory.");
const require = createRequire(path.join(visualDir, "artifact-tool-loader.cjs"));
const artifactToolEntry = require.resolve("@oai/artifact-tool");
const { SpreadsheetFile, Workbook } = await import(pathToFileURL(artifactToolEntry).href);

const csvPath = path.join(repo, "provider_validation", "coverage", "interface-coverage.csv");
const xlsxPath = path.join(repo, "provider_validation", "coverage", "interface-coverage.xlsx");
const csvText = (await fs.readFile(csvPath, "utf8")).replace(/^\uFEFF/, "");
const imported = await Workbook.fromCSV(csvText, { sheetName: "导入数据" });
const importedRange = imported.worksheets.getItem("导入数据").getUsedRange();
const cleanCell = (value) => String(value ?? "").replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\uFFFE\uFFFF]/g, "");
const matrix = importedRange.values.map((row) => row.map(cleanCell));
const headers = matrix[0].map((v) => String(v ?? ""));
const rows = matrix.slice(1).map((r) => headers.map((_, i) => String(r[i] ?? "")));
const col = (name) => headers.indexOf(name);
const included = rows.filter((r) => r[col("是否计入87项")] === "是");
const countBy = (status) => included.filter((r) => r[col("接口取数结果")] === status).length;
const supplementary = rows.filter((r) => r[col("清单范围")].includes("补充"));
const reportSummaryPath = path.join(repo, "provider_validation", "results", "interface-coverage-summary.json");
const reportSummary = JSON.parse(await fs.readFile(reportSummaryPath, "utf8"));
const liveProbeIds = new Set();
const liveProbeRoot = path.join(repo, "provider_validation", "results", "live-probes");
for (const entry of await fs.readdir(liveProbeRoot, { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  const runDir = path.join(liveProbeRoot, entry.name);
  try {
    const policy = JSON.parse(await fs.readFile(path.join(runDir, "probe-run-policy.json"), "utf8"));
    if (policy.policy_version !== "rate-limited-live-probe-v1") continue;
    const summary = (await fs.readFile(path.join(runDir, "_summary.csv"), "utf8")).replace(/^\uFEFF/, "");
    for (const match of summary.matchAll(/^(\d+),/gm)) liveProbeIds.add(Number(match[1]));
  } catch {
    // An incomplete batch is not counted as a completed capability probe.
  }
}

const workbook = Workbook.create();
const overview = workbook.worksheets.add("覆盖概览");
const detail = workbook.worksheets.add("逐接口明细");
const glossary = workbook.worksheets.add("状态释义");
overview.showGridLines = false;
detail.showGridLines = false;
glossary.showGridLines = false;
overview.tabColor = "#1F4E78";
detail.tabColor = "#5B9BD5";
glossary.tabColor = "#70AD47";

overview.getRange("A2").values = [["接口测试覆盖度报告"]];
overview.getRange("A2:D2").format = { font: { name: "Arial", size: 16, bold: true, color: "#17365D" } };
overview.getRange("A3").values = [[`数据基准：${new Date().toISOString().slice(0, 10)}；“能力声明”与“实测范围”分列，证据状态代码及中文解释见明细和“状态释义”`]];
overview.getRange("A3:D3").format = { font: { name: "Arial", size: 10, color: "#666666" } };
overview.getRange("A5:C5").values = [["统计项", "数量", "口径说明"]];
overview.getRange("A6:C14").values = [
  ["a-stock-data纳入分母能力", 87, "正式计数范围；不包含说明项和板块补充项"],
  ["说明/参数变体", 2, "另列，不计入87项分母"],
  ["stock-data-analyse板块接口", supplementary.length, "行业目录、行业指数、行业/概念资金流、BaoStock两项查询"],
  ["通过", countBy("通过"), "已留存真实接口原始响应并通过已记录范围核验"],
  ["部分通过", countBy("部分通过"), "有解析输出或历史合并结果；证据/覆盖范围仍有明确限制"],
  ["未通过", countBy("未通过"), "失败、不可用或返回不完整；见逐项错误和结果证据"],
  ["未验证", countBy("未验证"), "需要凭据或缺少可复查的实时返回"],
  ["a-stock-data低频实时探测接口数", liveProbeIds.size, "按逐接口摘要去重，含1项因全市场分页超出采样范围而停止的部分记录；BaoStock SDK探针另见明细"],
  ["原始响应哈希核对", reportSummary.retained_raw_response_bodies_hash_checked, `${reportSummary.retained_raw_response_bodies_hash_checked}个报告引用的原始响应文件完成SHA-256核对；${reportSummary.retained_raw_response_hash_failures}个不匹配`],
];
overview.getRange("A5:C14").format.wrapText = true;
overview.getRange("A5:C5").format = { fill: "#1F4E78", font: { name: "Arial", size: 10, bold: true, color: "#FFFFFF" }, verticalAlignment: "center" };
overview.getRange("A6:C14").format = { font: { name: "Arial", size: 10, color: "#202020" }, verticalAlignment: "center", wrapText: true };
overview.getRange("A5:C14").format.borders = { preset: "insideHorizontal", style: "thin", color: "#D9E2F3" };
overview.getRange("A5:C5").format.rowHeight = 24;
overview.getRange("A6:C14").format.rowHeight = 32;
overview.getRange("A16").values = [["证据口径与限制"]];
overview.getRange("A16:D16").format = { font: { name: "Arial", size: 11, bold: true, color: "#17365D" } };
overview.getRange("A17:D20").values = [
  ["1", "补抓脚本结果", "本轮脚本执行状态与原始响应审计分开显示；解析CSV不是原始HTTP响应。", "见逐接口明细"],
  ["2", "能力声明与实测", "文档声明的支持范围不等于实测范围；逐周期、日期和回退结果请看“实测范围/日期/结果”。", "见逐接口明细"],
  ["3", "证据状态", "英文机器状态代码旁有中文解释；它描述证据形式，不替代接口取数结论。", "另有“状态释义”页"],
  ["4", "BaoStock证监会行业", "只保留历史合并解析数据，缺原始SDK行/TCP帧；当前Provider未做相同参数新全量Live Probe。", "部分通过"],
];
overview.getRange("A17:D20").format = { font: { name: "Arial", size: 10, color: "#202020" }, verticalAlignment: "center", wrapText: true };
overview.getRange("A17:D20").format.rowHeight = 38;
overview.getRange("A17:D20").format.borders = { preset: "insideHorizontal", style: "thin", color: "#E7E6E6" };
overview.getRange("A:A").format.columnWidth = 24;
overview.getRange("B:B").format.columnWidth = 16;
overview.getRange("C:C").format.columnWidth = 72;
overview.getRange("D:D").format.columnWidth = 24;
overview.getRange("A2:D20").format.verticalAlignment = "center";

detail.getRangeByIndexes(0, 0, matrix.length, headers.length).values = matrix;
const lastColumn = (n) => {
  let out = "";
  while (n > 0) {
    const rem = (n - 1) % 26;
    out = String.fromCharCode(65 + rem) + out;
    n = Math.floor((n - 1) / 26);
  }
  return out;
};
const lastCol = lastColumn(headers.length);
const lastRow = matrix.length;
const table = detail.tables.add(`A1:${lastCol}${lastRow}`, true, "InterfaceCoverageTable");
table.style = "TableStyleMedium2";
table.showFilterButton = true;
detail.getRange(`A1:${lastCol}1`).format = { fill: "#1F4E78", font: { name: "Arial", size: 10, bold: true, color: "#FFFFFF" }, wrapText: true, verticalAlignment: "center" };
detail.getRange(`A1:${lastCol}1`).format.rowHeight = 36;
detail.getRange(`A2:${lastCol}${lastRow}`).format = { font: { name: "Arial", size: 9, color: "#202020" }, wrapText: true, verticalAlignment: "center" };
detail.getRange(`A2:${lastCol}${lastRow}`).format.rowHeight = 60;
const klineDataIndex = rows.findIndex((r) => r[col("接口ID")] === "ASTOCK-002");
if (klineDataIndex >= 0) detail.getRange(`A${klineDataIndex + 2}:${lastCol}${klineDataIndex + 2}`).format.rowHeight = 120;
const widths = {
  "接口ID": 16, "清单范围": 30, "是否计入87项": 14, "项目/来源": 30, "类别": 30,
  "接口/能力名称": 34, "接口地址/协议": 48, "接口/调用符号": 42, "接口说明": 44,
  "能力说明（文档声明）/返回字段": 60, "调用方式/参数范围": 48, "来源代码文件/行号": 60,
  "代码SHA-256": 58, "测试代码文件/行号": 54, "项目Provider对照代码": 60,
  "接口取数结果": 16, "本轮补抓脚本结果": 24, "证据完整度": 48, "实测范围/日期/结果": 68,
  "验证备注/数据核验结论": 62, "原始响应Manifest": 54, "原始响应文件/哈希": 58,
  "解析结果/输出文件": 60, "结果文件SHA-256/行数": 65, "返回示例": 68,
  "逐接口结果记录": 58, "来源版本/提交": 50, "证据状态代码": 42, "证据状态说明": 58,
};
for (let i = 0; i < headers.length; i++) {
  const letter = lastColumn(i + 1);
  detail.getRange(`${letter}:${letter}`).format.columnWidth = widths[headers[i]] ?? 36;
}
const statusCol = lastColumn(col("接口取数结果") + 1);
const statusRange = detail.getRange(`${statusCol}2:${statusCol}${lastRow}`);
statusRange.conditionalFormats.add("cellIs", { operator: "equal", formula: "通过", format: { fill: "#E2F0D9", font: { color: "#375623", bold: true } } });
statusRange.conditionalFormats.add("cellIs", { operator: "equal", formula: "部分通过", format: { fill: "#DDEBF7", font: { color: "#1F4E78", bold: true } } });
statusRange.conditionalFormats.add("cellIs", { operator: "equal", formula: "未通过", format: { fill: "#FCE4D6", font: { color: "#9C0006", bold: true } } });
statusRange.conditionalFormats.add("cellIs", { operator: "equal", formula: "未验证", format: { fill: "#FFF2CC", font: { color: "#7F6000", bold: true } } });
detail.freezePanes.freezeRows(1);
detail.freezePanes.freezeColumns(6);

const evidenceGlossary = [
  ["证据状态代码", "中文解释", "阅读时注意"],
  ["live_raw", "实时原始响应已归档", "仅说明有实时原始响应；是否覆盖所有参数/周期看“实测范围/日期/结果”。"],
  ["parsed_only", "仅保存解析结果", "没有对应的原始响应，不能独立复核源站原始返回。"],
  ["historical_parsed_only", "仅有历史解析/合并结果", "未做本轮实时验证；历史数据可复核不等于当前接口可用。"],
  ["local_cache_unverified", "存在本地缓存，实时接口未验证", "缓存内容不构成当前在线接口可用的证据。"],
  ["failed", "本轮探针或数据校验失败", "看逐项错误和失败记录，区分传输失败与业务数据校验失败。"],
  ["unavailable", "接口当前不可用或受到访问阻断", "表示本轮环境下未取得可用数据，不直接断言源站永久不可用。"],
  ["credential_required", "需要凭据，未能验证", "不是接口失败结论。"],
  ["shared_endpoint_variant_not_separately_live_verified", "共用接口的该参数变体未单独实时验证", "其他参数变体通过不能代替此变体验证。"],
  ["undetermined", "证据状态未能判定", "缺少足够信息来归入其他状态。"],
];
glossary.getRange("A1:C1").values = [evidenceGlossary[0]];
glossary.getRange(`A2:C${evidenceGlossary.length}`).values = evidenceGlossary.slice(1);
glossary.tables.add(`A1:C${evidenceGlossary.length}`, true, "EvidenceStatusGlossary").style = "TableStyleMedium4";
glossary.getRange(`A1:C${evidenceGlossary.length}`).format.wrapText = true;
glossary.getRange("A:A").format.columnWidth = 48;
glossary.getRange("B:B").format.columnWidth = 34;
glossary.getRange("C:C").format.columnWidth = 88;
glossary.getRange(`A1:C${evidenceGlossary.length}`).format.rowHeight = 38;
glossary.freezePanes.freezeRows(1);

workbook.recalculate();
const previewDir = visualDir;
const overviewPreview = await workbook.render({ sheetName: "覆盖概览", range: "A1:D20", scale: 1, format: "png" });
await fs.writeFile(path.join(previewDir, "interface-coverage-overview.png"), new Uint8Array(await overviewPreview.arrayBuffer()));
const detailPreview = await workbook.render({ sheetName: "逐接口明细", range: "A1:H12", scale: 1, format: "png" });
await fs.writeFile(path.join(previewDir, "interface-coverage-detail.png"), new Uint8Array(await detailPreview.arrayBuffer()));
const glossaryPreview = await workbook.render({ sheetName: "状态释义", range: "A1:C10", scale: 1, format: "png" });
await fs.writeFile(path.join(previewDir, "interface-coverage-status-glossary.png"), new Uint8Array(await glossaryPreview.arrayBuffer()));
const inspect = await workbook.inspect({ kind: "workbook,sheet,table", maxChars: 5000, tableMaxRows: 4, tableMaxCols: 8, tableMaxCellChars: 120 });
await fs.writeFile(path.join(previewDir, "interface-coverage-inspect.ndjson"), inspect.ndjson ?? String(inspect));
const xlsx = await SpreadsheetFile.exportXlsx(workbook);
await xlsx.save(xlsxPath);
console.log(JSON.stringify({ xlsxPath, rows: rows.length, headers: headers.length, statusCounts: { pass: countBy("通过"), partial: countBy("部分通过"), fail: countBy("未通过"), unverified: countBy("未验证") }, inspect: inspect.ndjson }, null, 2));
