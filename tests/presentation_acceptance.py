"""Reproduce the approved display acceptance using retained responses, never production writes."""
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from decimal import Decimal
from unittest.mock import patch
import json
import shutil
import sys
import argparse

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--root", type=Path, default=ROOT / "tmp/presentation-20261029")
parser.add_argument("--task-id", default="three-stock-display-20261029")
args = parser.parse_args()
BASE = args.root.resolve()
if not BASE.is_relative_to(ROOT / "tmp") or BASE == ROOT / "tmp":
    raise ValueError("Acceptance root must be an independent directory under project tmp")
ORIGINAL = ROOT / "tmp/daily-flow-20261029"
BASE.mkdir(parents=True, exist_ok=True)


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def save(name, value):
    path = BASE / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


save("run-environment.json", {"executable": sys.executable, "python": sys.version,
    "checked_at": datetime.now(timezone.utc), "packages": {name: metadata.version(name) for name in
        ("fastapi", "httpx", "pyarrow", "duckdb", "PyYAML", "requests", "uvicorn")}})
assert sys.version_info >= (3, 11)
from fastapi.testclient import TestClient
from stock_data_manage.pipeline.inputs import collect_task
from stock_data_manage.providers.transport import RequestPacer
from stock_data_manage.service.web import create_app
from stock_data_manage.storage.integrity import Manifest, file_hash
from stock_data_manage.storage.parquet import CanonicalPartitionStore
from stock_data_manage.storage.raw import RawObjectStore

index = read(ORIGINAL / "source-index.json")
units = []
for symbol in ("sh600519", "sz000001", "bj920001"):
    units.append({"key": symbol, "symbol": symbol, "input_id": "ASTOCK-002-daily",
        "context": {"request": {"symbol": symbol, "start_date": "2026-10-29", "end_date": "2026-10-29"}},
        "replay_manifests": {a: index[symbol + "-" + a]["raw_manifest"] for a in ("none", "forward", "backward")}})
definition = {"task_id": args.task_id, "task": "stock_daily", "trade_date": "2026-10-29", "units": units}
save("task.json", definition)
with patch("requests.adapters.HTTPAdapter.send", side_effect=AssertionError("Acceptance attempted HTTP")):
    state = collect_task(definition=definition, config_root=ROOT / "config", data_root=BASE / "run", mode="replay",
        collector_options={"pacer": RequestPacer(wait=lambda _: None)})
    assert state["status"] == "published", state.get("error")
    save("state.json", state)
    resume = collect_task(definition=definition, config_root=ROOT / "config", data_root=BASE / "run", mode="replay")
    assert resume["no_op"]

master = ROOT / "data/canonical/2026-10-09/security_master"
assert Manifest.load(master / "manifest.json").verify(master / "data.parquet")
target = BASE / "run/canonical/2026-10-09/security_master"
target.mkdir(parents=True, exist_ok=True)
for name in ("data.parquet", "manifest.json"):
    if (target / name).exists():
        assert file_hash(target / name) == file_hash(master / name)
    else:
        shutil.copy2(master / name, target / name)
save("master-snapshot.json", {"source": str(master), "destination": str(target), "date": "2026-10-09",
    "row_count": Manifest.load(target / "manifest.json").row_count, "original_bytes_preserved": True})

records = CanonicalPartitionStore(BASE / "run/canonical").read_daily("stock", "2026-10-29")
assert len(records) == 3
for record in records:
    symbol = {"XSHG":"sh", "XSHE":"sz", "BSE":"bj"}[record.instrument_id.split(":")[0]] + record.instrument_id.split(":")[1]
    for adjustment, quote in record.quotes.items():
        check = read(index[symbol + "-" + adjustment]["semantic_check"])
        for field, expected in check["target_prices"].items():
            assert getattr(quote, field) == Decimal(expected)
        source_row = check["source_target_row"]
        assert quote.volume == Decimal(source_row[5]) * 100
        assert quote.amount == Decimal(source_row[8]) * 10000
        RawObjectStore.verify_manifest(Path(quote.raw_object_path))
        assert set(quote.flags) == {"source_volume_rounded_to_100_shares", "source_amount_rounded_to_100_CNY"}

names = {"sh600519":"贵州茅台", "sz000001":"平安银行", "bj920001":"纬达光电"}
with TestClient(create_app(config_root=ROOT / "config", data_root=BASE / "run")) as client:
    for adjustment in ("none", "forward", "backward"):
        response = client.get("/api/daily", params={"start_date":"2026-10-29", "end_date":"2026-10-29", "adjustment":adjustment})
        assert response.status_code == 200
        result = response.json()
        assert result["total"] == 3 and len(result["columns"]) == 13
        assert all(row["security_name"] == names[row["symbol"]] and row["name_source_date"] == "2026-10-09" for row in result["items"])
        bj = next(row for row in result["items"] if row["symbol"] == "bj920001")
        assert bj["field_metadata"]["volume"]["status"] == "available"
        assert Decimal(bj["values"]["volume"]) == Decimal("1182900")
        assert Decimal(bj["values"]["amount"]) == Decimal("11671400")
        save(f"checks/api-{adjustment}.json", result)
    filters = []
    for query, expected in (({},9), ({"asset_type":"stock"},9), ({"security":"贵州茅台"},3)):
        response = client.get("/api/data", params={"dataset":"daily_bar", "start_date":"2026-10-29", "end_date":"2026-10-29", **query})
        assert response.status_code == 200
        result = response.json()
        assert result["total"] == expected and len(result["columns"]) == 12
        filters.append({"query":query,"expected":expected,"actual":result["total"]})
        if not query:
            save("checks/source-api.json", result)
            for row in result["items"]:
                assert row["security_name"] == names[row["symbol"]]
                expected_units = {"volume":"lot", "amount":"ten_thousand_CNY"}
                assert all(row["field_metadata"][field]["unit"] == expected_units.get(field, "") for field in ("volume", "amount"))
                assert "9" in row["values"] and "source_bar_key" in row["values"]
    save("filter-results.json", filters)
guard = {name: file_hash(ROOT / name) == digest for name, digest in read(ORIGINAL / "original-production.json").items()}
assert all(guard.values())
save("results.json", {"passed":True,"published_rows":3,"quote_groups":9,"raw_price_comparisons":36,
    "quantity_comparisons":18,"daily_columns":13,"source_columns":12,"provider_requests":0,
    "resume_no_op":True,"production_files_unchanged":guard,"names_snapshot":"2026-10-09","source_filters":filters})
print(json.dumps(read(BASE / "results.json"), ensure_ascii=False))
