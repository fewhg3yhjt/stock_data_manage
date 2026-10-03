"""Offline inventory of existing providers and input migration gaps; no provider calls."""
from pathlib import Path
from datetime import datetime, timezone
import ast
import collections
import gzip
import hashlib
import json
import yaml

ROOT=Path(__file__).resolve().parents[3]
OUT=Path(__file__).parent

def reference(path):
    path=Path(path)
    return {"path":path.relative_to(ROOT).as_posix(),"sha256":hashlib.sha256(path.read_bytes()).hexdigest()}

factory_path=ROOT/"src/stock_data_manage/routing/factory.py"
tree=ast.parse(factory_path.read_text(encoding="utf-8"))
function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=="build_input_provider")
methods=ast.literal_eval(next(n.value for n in function.body if isinstance(n,ast.Assign)
    and any(isinstance(t,ast.Name) and t.id=="expected_methods" for t in n.targets)))
config=ROOT/"config/providers.yaml"
inputs=yaml.safe_load(config.read_text(encoding="utf-8"))["input_capabilities"]
modules={
 "tencent.bulk_snapshot":"providers/tencent/snapshot.py",
 "tencent.native_1m":"providers/tencent/minute.py",
 "tencent.forward_history":"providers/tencent/daily.py",
 "eastmoney.limit_up_pool":"providers/eastmoney/limit_pool.py",
 "sina.trading_calendar":"providers/sina/calendar.py",
 "akshare.industry_index_daily":"providers/akshare/boards.py",
 "akshare.industry_fund_flow":"providers/akshare/boards.py",
 "akshare.concept_fund_flow":"providers/akshare/boards.py",
 "eastmoney.shareholder_count":"providers/eastmoney/shareholder.py",
 "eastmoney.dividend_event":"providers/eastmoney/dividend.py",
 "eastmoney.stock_fund_flow":"providers/eastmoney/fund_flow.py",
 "baostock.industry_membership":"providers/baostock/industry.py",
}
checked={}
records={}
rows=[]
for ident,contract in inputs.items():
    module_name=modules.get(contract.get("runtime_endpoint"))
    module=ROOT/"src/stock_data_manage"/module_name if module_name else None
    row={"input_id":ident,"display_name":contract["display_name"],"provider":contract["provider"],
        "endpoint":contract["endpoint"],"declared_status":contract["implementation_status"],
        "generic_entry_supported":ident in methods,"dataset":contract.get("dataset"),
        "runtime_method":contract.get("runtime_method"),"runtime_endpoint":contract.get("runtime_endpoint"),
        "existing_module":reference(module) if module else None,"evidence":[]}
    for ident_ref in contract["evidence_refs"]:
        path=ROOT/"provider_validation/results/interface-records"/(ident_ref+".json")
        record=records.setdefault(ident_ref,json.loads(path.read_text(encoding="utf-8")))
        row["evidence"].append({**reference(path),"interface_id":ident_ref,
            "validation_result":record["validation_result"],"tested_scope":record["scope"],
            "finding":record["finding"],"validation_time_utc":record["validation_time_utc"],
            "manifest_ref":record.get("manifest_ref")})
        for kind in ("response_artifacts","derived_artifacts","source_code_artifacts"):
            for item in record.get(kind,[]):
                if item["path"] not in checked:
                    artifact=ROOT/item["path"]
                    data=gzip.decompress(artifact.read_bytes()) if kind=="response_artifacts" else artifact.read_bytes()
                    digest=hashlib.sha256(data).hexdigest()
                    assert digest==item["sha256"],item["path"]
                    checked[item["path"]]={"path":item["path"],"sha256":digest,"kind":kind}
        manifest=record.get("manifest_ref")
        if manifest and (ROOT/manifest).is_file() and manifest not in checked:
            checked[manifest]={**reference(ROOT/manifest),"kind":"manifest_reference"}
    rows.append(row)
counts=dict(collections.Counter(c["implementation_status"] for c in inputs.values()))
assert len(inputs)==74 and len(records)==73
assert counts=={"migration_pending":5,"implemented_validation_only":10,"unimplemented":50,"blocked":7,"alias":2}
old_only=[r["input_id"] for r in rows if r["declared_status"]=="implemented_validation_only" and not r["generic_entry_supported"]]
assert old_only==["SDA-BOARD-001","SDA-BOARD-002","SDA-BOARD-003","SDA-BOARD-004","SDA-BOARD-005","SDA-BOARD-006"]
assert set(methods)=={"ASTOCK-002-daily","ASTOCK-002-5m","ASTOCK-045","ASTOCK-070"}
report={"record_type":"input_transition_readiness_review","validation_time_utc":datetime.now(timezone.utc).isoformat(),
 "review_date_local":"2026-10-04","mode":"offline_code_and_archive_audit","network_requests":0,"production_writes":0,
 "eligible_for_production_routing":False,"implemented_conversion_this_review":False,
 "configured_input_count":74,"successful_source_interface_count":73,"status_counts":counts,
 "generic_entry_input_ids":list(methods),"old_validated_not_generic_entry":old_only,
 "code_references":[reference(ROOT/p) for p in ["src/stock_data_manage/config/loader.py","src/stock_data_manage/routing/factory.py",
    "src/stock_data_manage/pipeline/inputs.py","src/stock_data_manage/cli.py","src/stock_data_manage/worker/scheduler.py"]],
 "configuration":reference(config),"audit_source":reference(Path(__file__)),"hash_checked_artifacts":list(checked.values()),
 "inputs":rows,"recommended_next_batch":["SDA-BOARD-001","SDA-BOARD-002","SDA-BOARD-003","SDA-BOARD-004"],
 "next_batch_existing_provider":"src/stock_data_manage/providers/akshare/boards.py",
 "next_batch_limits":["modify existing provider and generic input modules; no parallel provider or manager",
    "preserve source SDK transport, field and return contracts; use YAML mappings",
    "industry directory differs from security membership; concept flow differs from concept members",
    "reuse archived responses first; candidate only; schedules and production routing remain disabled",
    "unconfirmed units stay unconfirmed, never infer source data date from replay time"],
 "deferred":["BaoStock SDK evidence integration and security snapshot vs membership contracts",
    "Tencent snapshot input migration and live batch equivalence","EastMoney shareholder/fund-flow/dividend migration comparisons",
    "generalized non-symbol/date/board collection scheduling","unit/finality/freshness/routing gates","production publication and console"]}
target=OUT/"review.json"
if target.exists():
    raise SystemExit("existing investigation output must not be overwritten; copy audit to a fresh review directory")
target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8",newline="\n")
print(json.dumps({"result":"passed","configured_inputs":74,"entry_inputs":len(methods),"old_path_only":len(old_only),
    "checked_artifacts":len(checked),"output":str(target.relative_to(ROOT))}))
