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

const workbook = Workbook.create();
const overview = workbook.worksheets.add("覆盖概览");
const detail = workbook.worksheets.add("逐接口明细");
overview.showGridLines = false;
detail.showGridLines = false;
overview.tabColor = "#1F4E78";
detail.tabColor = "#5B9BD5";

overview.getRange("A2").values = [["接口测试覆盖度报告"]];
overview.getRange("A2:D2").format = { font: { name: "Arial", size: 16, bold: true, color: "#17365D" } };
overview.getRange("A3").values = [["数据基准：2026-10-03；逐接口结果、代码、调用范围和证据路径见“逐接口明细”"]];
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
  ["本轮补抓输出", 61, "61项执行结果全部匹配到逐接口行；执行状态单独列示"],
  ["原始响应哈希核对", 114, "114个归档响应文件与SHA-256文件名一致；0个不匹配"],
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
  ["2", "范围边界", "通过仅代表该行写明的接口、日期、参数、样本和校验；不自动代表全市场或长期稳定。", "见逐接口明细"],
  ["3", "板块数据", "行业/概念资金流是快照，不能代表证券-概念成分关系；金额单位尚未确认。", "6项不计入87项"],
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
detail.getRange(`A2:${lastCol}${lastRow}`).format.rowHeight = 46;
const widths = {
  "接口ID": 16, "清单范围": 30, "是否计入87项": 14, "项目/来源": 30, "类别": 30,
  "接口/能力名称": 34, "接口地址/协议": 48, "接口/调用符号": 42, "接口说明": 44,
  "接口内容/主要字段": 52, "调用方式/参数范围": 48, "来源代码文件/行号": 60,
  "代码SHA-256": 58, "测试代码文件/行号": 54, "项目Provider对照代码": 60,
  "接口取数结果": 16, "本轮补抓脚本结果": 24, "证据完整度": 48, "验证范围/日期": 48,
  "验证备注/数据核验结论": 62, "原始响应Manifest": 54, "原始响应文件/哈希": 58,
  "解析结果/输出文件": 60, "结果文件SHA-256/行数": 65, "返回示例": 68,
  "逐接口结果记录": 58, "来源版本/提交": 50, "证据状态": 24,
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

workbook.recalculate();
const previewDir = visualDir;
const overviewPreview = await workbook.render({ sheetName: "覆盖概览", range: "A1:D20", scale: 1, format: "png" });
await fs.writeFile(path.join(previewDir, "interface-coverage-overview.png"), new Uint8Array(await overviewPreview.arrayBuffer()));
const detailPreview = await workbook.render({ sheetName: "逐接口明细", range: "A1:H12", scale: 1, format: "png" });
await fs.writeFile(path.join(previewDir, "interface-coverage-detail.png"), new Uint8Array(await detailPreview.arrayBuffer()));
const inspect = await workbook.inspect({ kind: "workbook,sheet,table", maxChars: 5000, tableMaxRows: 4, tableMaxCols: 8, tableMaxCellChars: 120 });
await fs.writeFile(path.join(previewDir, "interface-coverage-inspect.ndjson"), inspect.ndjson ?? String(inspect));
const xlsx = await SpreadsheetFile.exportXlsx(workbook);
await xlsx.save(xlsxPath);
console.log(JSON.stringify({ xlsxPath, rows: rows.length, headers: headers.length, statusCounts: { pass: countBy("通过"), partial: countBy("部分通过"), fail: countBy("未通过"), unverified: countBy("未验证") }, inspect: inspect.ndjson }, null, 2));
