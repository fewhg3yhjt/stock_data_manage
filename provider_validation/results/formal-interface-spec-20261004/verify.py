"""Independently check current configuration, linked evidence and saved XLSX XML."""
from __future__ import annotations

import gzip
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile

import yaml

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).parent
DOC = ROOT / "docs/providers"
SPEC = DOC / "源头采集接口说明.json"
XLSX = DOC / "源头采集接口说明.xlsx"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    spec = load(SPEC)
    config = yaml.safe_load((ROOT / "config/providers.yaml").read_text(encoding="utf-8"))["input_capabilities"]
    overview, params, fields, excluded = spec["sheets"]
    active = {k for k, v in config.items() if v["implementation_status"] == "implemented_validation_only"}
    assert {r[0] for r in overview["rows"]} == active
    assert len(overview["rows"]) == len(active) == 64
    assert {r[0] for r in excluded["rows"]} == set(config) - active
    assert len(config) == len(overview["rows"]) + len(excluded["rows"]) == 77
    assert {e["input_id"] for e in spec["evidence"]} == active
    for source in spec["sources"]:
        assert digest(ROOT / source["path"]) == source["sha256"], source["path"]
    expected_params = {(k, p) for k in active for p in config[k]["parameters"]}
    assert {(r[0], r[2]) for r in params["rows"] if r[2] != "无调用参数"} == expected_params
    for row in params["rows"]:
        if row[2] != "无调用参数":
            assert row[6] == config[row[0]]["parameters"][row[2]]["source"]
    for ident in active:
        dataset = yaml.safe_load((ROOT / "config/datasets" / f"{config[ident]['dataset']}.yaml").read_text(encoding="utf-8"))
        assert {r[3] for r in fields["rows"] if r[0] == ident} == set(dataset["fields"])
    capital = next(r for r in fields["rows"] if r[0] == "ASTOCK-037-profile" and r[3] == "registered_capital")
    assert capital[8] == "未独立核准" and capital[12] == "标准输出置空；来源值另行留证"
    assert next(r for r in excluded["rows"] if r[0] == "ASTOCK-037")[3] == "原复合记录（已拆分）"
    assert sum(r[3] == "别名（不重复计数）" for r in excluded["rows"]) == 3
    for ident in ("ASTOCK-011", "ASTOCK-037-business"):
        assert "尚不能认定来源永久不可用" in next(r[5] for r in excluded["rows"] if r[0] == ident)
    rows_by_id = {r[0]: r for r in overview["rows"]}
    assert rows_by_id["ASTOCK-087"][18] == '{"date":"2026-10-01"}'
    assert rows_by_id["ASTOCK-037-events"][18] == '{"date":"2023-08-08"}'
    assert [rows_by_id[k][19] for k in ("ASTOCK-014", "ASTOCK-037-profile", "ASTOCK-037-events", "ASTOCK-044", "ASTOCK-087")] == [375, 1, 75, 198, 718]
    assert rows_by_id["ASTOCK-001"][11] == "每1天，15:10"
    assert all(r[8] == "未授予生产路由资格" and r[9] == "未启用" for r in overview["rows"])

    bodies = set()
    bodyless = 0
    for item in spec["evidence"]:
        assert digest(ROOT / item["report"]) == item["report_sha256"]
        assert digest(ROOT / item["audit_index"]) == item["audit_index_sha256"]
        assert digest(ROOT / item["derived_output"]["path"]) == item["derived_output"]["sha256"]
        for manifest in item["manifests"]:
            assert digest(ROOT / manifest["path"]) == manifest["sha256"]
        report = load(ROOT / item["report"])
        raw_path = ROOT / item["manifests"][0]["path"]
        for response in report.get("responses", []):
            storage = response.get("body_storage")
            if not storage:
                bodyless += 1
                continue
            path = raw_path.parent / storage
            content = gzip.decompress(path.read_bytes()) if path.suffix == ".gz" else path.read_bytes()
            assert hashlib.sha256(content).hexdigest() == response["body_sha256"], str(path)
            bodies.add(str(path))

    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with ZipFile(XLSX) as archive:
        assert archive.testzip() is None
        book = ET.fromstring(archive.read("xl/workbook.xml"))
        assert [s.attrib["name"] for s in book.findall("x:sheets/x:sheet", ns)] == [s["name"] for s in spec["sheets"]]
        shared = ET.fromstring(archive.read("xl/sharedStrings.xml"))
        strings = ["".join(n.itertext()) for n in shared]
        formula_count = 0
        for index, sheet in enumerate(spec["sheets"], 1):
            tree = ET.fromstring(archive.read(f"xl/worksheets/sheet{index}.xml"))
            cells = {c.attrib["r"]: c for c in tree.findall("x:sheetData/x:row/x:c", ns)}
            ids = []
            for row in range(10, 10 + len(sheet["rows"])):
                cell = cells[f"A{row}"]
                value = cell.find("x:v", ns).text
                ids.append(strings[int(value)] if cell.attrib.get("t") == "s" else value)
            assert ids == [r[0] for r in sheet["rows"]]
            for col, header in enumerate(sheet["headers"], 1):
                if header in {"验证报告", "模板路径", "映射路径"}:
                    for record in sheet["rows"]:
                        assert (ROOT / record[col - 1]).is_file()
            assert int(cells["B4"].find("x:v", ns).text) == len(sheet["rows"])
            pane = tree.find("x:sheetViews/x:sheetView/x:pane", ns)
            assert pane.attrib["state"] == "frozen"
            assert float(pane.attrib["xSplit"]) == 2 and float(pane.attrib["ySplit"]) == 9
            assert not [c for c in cells.values() if c.attrib.get("t") == "e"]
            formulas = [c.find("x:f", ns).text for c in cells.values() if c.find("x:f", ns) is not None]
            for formula in formulas:
                if formula.startswith("HYPERLINK("):
                    target = re.match(r'HYPERLINK\("([^"]+)"', formula).group(1)
                    assert (DOC / target).resolve().is_file(), target
            formula_count += len(formulas)
            table = ET.fromstring(archive.read(f"xl/tables/table{index}.xml"))
            assert table.find("x:autoFilter", ns) is not None
    links = 0
    for path in (ROOT / "README.md", DOC / "README.md"):
        for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", path.read_text(encoding="utf-8")):
            if "://" in target:
                continue
            file, _, anchor = target.partition("#")
            linked = path.parent / file
            if linked.resolve() == (OUT / "verification.json").resolve():
                # This check writes the linked result after all assertions pass.
                links += 1
                continue
            assert linked.is_file(), target
            if anchor == "文档管理":
                assert "## 文档管理" in linked.read_text(encoding="utf-8")
            links += 1
    result = {"validation_time_utc": datetime.now(timezone.utc).isoformat(), "result": "passed", "mode": "offline_documentation_check", "network_requests": 0, "production_writes": 0, "configured_records": len(config), "hash_linked_reports": len(spec["evidence"]), "raw_response_bodies_hash_checked": len(bodies), "response_events_without_body": bodyless, "sheet_rows": {s["name"]: len(s["rows"]) for s in spec["sheets"]}, "xlsx_formulas_checked": formula_count, "markdown_links_checked": links, "xlsx_zip_xml": "passed", "freeze_and_filters": "passed", "native_excel_application_test": "not_performed", "visual_review": "four worksheets reviewed with Artifact Tool renders", "artifacts": [{"path": str(p.relative_to(ROOT)).replace("\\", "/"), "sha256": digest(p)} for p in (SPEC, XLSX, Path(__file__), ROOT / "provider_validation/tests/build_interface_coverage.py", ROOT / "provider_validation/tests/build_interface_coverage_workbook.mjs")]}
    (OUT / "verification.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
