from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .domain import Adjustment, AssetType, Dataset, Exchange
from .providers.sina import SinaDailyProvider, SinaMinuteProvider
from .providers.tencent import TencentDailyProvider, TencentMinuteProvider
from .providers.transport import UrlLibTransport
from .providers.probes import probe_daily_capability, probe_minute_capability
from .routing.capabilities import ProviderCapability
from .storage.metadata import MetadataStore
from .worker.acceptance import run_offline_acceptance
from .worker.recovery import RecoveryScanner


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="stock-data")
    subcommands = parser.add_subparsers(dest="command", required=True)

    probe = subcommands.add_parser("probe-daily", help="run one explicit provider capability probe")
    probe.add_argument("--provider", choices=("sina", "tencent"), required=True)
    probe.add_argument("--symbol", required=True)
    probe.add_argument("--trade-date", type=date.fromisoformat, required=True)
    probe.add_argument("--ttl-days", type=int, default=7)
    probe.add_argument("--metadata", type=Path)
    probe.add_argument("--market", default="XSHG")
    probe.add_argument("--asset-type", default="stock")
    probe.add_argument("--code-prefix", default="")
    probe.add_argument("--adjustment", choices=("none", "forward"), default="none")

    minute_probe = subcommands.add_parser(
        "probe-minute", help="run one explicit minute provider capability probe"
    )
    minute_probe.add_argument("--provider", choices=("sina", "tencent"), required=True)
    minute_probe.add_argument("--symbol", required=True)
    minute_probe.add_argument("--as-of", type=_parse_datetime, required=True)
    minute_probe.add_argument("--frequency", type=int, choices=(1, 5), required=True)
    minute_probe.add_argument("--ttl-days", type=int, default=7)
    minute_probe.add_argument("--metadata", type=Path)
    minute_probe.add_argument("--market", default="XSHG")
    minute_probe.add_argument("--asset-type", default="stock")
    minute_probe.add_argument("--code-prefix", default="")

    recover = subcommands.add_parser("recover", help="repair canonical files and metadata after interruption")
    recover.add_argument("--canonical-root", type=Path)
    recover.add_argument("--metadata", type=Path)
    recover.add_argument("--config-root", type=Path, default=Path("config"))
    recover.add_argument("--data-root", type=Path)
    recover.add_argument("--archive-task", type=Path, action="append", default=[],
                         help="archive an explicitly selected, verified published task")

    acceptance = subcommands.add_parser(
        "acceptance-offline", help="run deterministic offline M1 acceptance evidence"
    )
    acceptance.add_argument("--root", type=Path, required=True)
    acceptance.add_argument("--end-date", type=date.fromisoformat, default=date(2026, 9, 11))
    acceptance.add_argument("--output", type=Path)

    collect = subcommands.add_parser("collect-input", help="collect or replay one configured input into candidate files")
    collect.add_argument("--input", required=True)
    collect.add_argument("--config-root", type=Path, default=Path("config"))
    collect.add_argument("--output-root", type=Path, help="explicit isolated validation output; historical evidence layout")
    collect.add_argument("--data-root", type=Path, help="override configured runtime data root, keeping all storage layers together")
    collect.add_argument("--mode", choices=("replay", "live"), default="replay")
    collect.add_argument("--replay-manifest", type=Path)
    collect.add_argument("--evidence-root", type=Path)
    collect.add_argument("--symbol")
    collect.add_argument("--start-date", type=date.fromisoformat)
    collect.add_argument("--end-date", type=date.fromisoformat)
    collect.add_argument("--trade-date", type=date.fromisoformat)
    collect.add_argument("--count", type=int)
    collect.add_argument("--fields", help="comma-separated declared output fields, including required fields")
    collect.add_argument("--calendar-file", type=Path, help="explicit saved calendar rows for date-snapshot validation")
    collect.add_argument("--context-file", type=Path, help="JSON parameter namespaces; explicit CLI flags override matching values")

    task = subcommands.add_parser("collect-task", help="execute a durable security-master or daily task in isolated storage")
    task.add_argument("--task-file", type=Path, required=True)
    task.add_argument("--config-root", type=Path, default=Path("config"))
    task.add_argument("--data-root", type=Path, required=True)
    task.add_argument("--redo", choices=("resume", "full", "selected"), default="resume")
    task.add_argument("--symbol", action="append", default=[])
    task.add_argument("--mode", choices=("replay", "live"), default="replay")

    due = subcommands.add_parser("collect-due-inputs", help="plan one scheduler tick, or explicitly execute candidate collection")
    due.add_argument("--config-root", type=Path, default=Path("config"))
    due.add_argument("--output-root", type=Path, help="explicit isolated validation output; historical evidence layout")
    due.add_argument("--data-root", type=Path, help="override configured runtime data root")
    due.add_argument("--now", type=_parse_datetime, help="aware schedule time; defaults to current UTC time")
    due.add_argument("--calendar-file", type=Path, required=True)
    due.add_argument("--securities-file", type=Path, help="saved SecurityRecord array for all_stock scope")
    due.add_argument("--symbol", action="append", default=[], help="explicit watchlist/static symbol; repeat for multiple symbols")
    due.add_argument("--execute", action="store_true", help="execute enabled, implemented candidate inputs; default only saves a plan")
    due.add_argument("--mode", choices=("replay", "live"), default="replay")
    due.add_argument("--replay-manifest", type=Path)
    due.add_argument("--evidence-root", type=Path)

    args = parser.parse_args(argv)
    if args.command == "collect-task":
        from .pipeline.inputs import collect_task
        try:
            definition = json.loads(args.task_file.read_text(encoding="utf-8"))
            report = collect_task(definition=definition, config_root=args.config_root, data_root=args.data_root,
                                  redo=args.redo, symbols=args.symbol, mode=args.mode)
        except (ValueError, KeyError, TypeError, OSError) as exc:
            parser.error(str(exc))
        print(json.dumps(report, ensure_ascii=False, default=_json_default, indent=2))
        return 0 if report["status"] == "published" else 2
    if args.command == "collect-due-inputs":
        from .pipeline.inputs import collect_due_inputs
        from .service.instruments import SecurityRecord
        try:
            rows = json.loads(args.calendar_file.read_text(encoding="utf-8"))
            if not isinstance(rows, list):
                raise ValueError("calendar file must contain date rows")
            for row in rows:
                if "is_trading_day" in row and type(row["is_trading_day"]) is not bool:
                    raise ValueError("is_trading_day must be a boolean")
            days = [date.fromisoformat(row["trade_date"]) for row in rows if row.get("is_trading_day", True)]
            records = []
            paths = [args.calendar_file]
            if args.securities_file:
                security_rows = json.loads(args.securities_file.read_text(encoding="utf-8"))
                if not isinstance(security_rows, list):
                    raise ValueError("securities file must contain SecurityRecord rows")
                for row in security_rows:
                    values = {**row, "exchange": Exchange(row["exchange"]), "asset_type": AssetType(row["asset_type"])}
                    for field in ("list_date", "delist_date", "publish_date", "calculation_start_date"):
                        if values.get(field) is not None:
                            values[field] = date.fromisoformat(values[field])
                    records.append(SecurityRecord(**values))
                paths.append(args.securities_file)
            report = collect_due_inputs(now=args.now or datetime.now(timezone.utc), config_root=args.config_root,
                output_root=args.output_root, trading_dates=days, securities=records, symbols=args.symbol,
                execute=args.execute, mode=args.mode, replay_manifest=args.replay_manifest,
                evidence_root=args.evidence_root, dependency_paths=paths,
                **({"data_root": args.data_root} if args.data_root is not None else {}))
        except (ValueError, TypeError, KeyError) as exc:
            parser.error(str(exc))
        print(json.dumps(report, ensure_ascii=False, default=_json_default, indent=2))
        return 2 if any(job["status"] in {"blocked", "failed"} or
                        (job["status"] == "already_attempted" and job.get("reason") != "validated")
                        for job in report["jobs"]) else 0
    if args.command == "collect-input":
        from .pipeline.inputs import collect_input
        request_context = {k: v for k, v in {"symbol": args.symbol, "start_date": args.start_date,
                           "end_date": args.end_date, "trade_date": args.trade_date}.items() if v is not None}
        try:
            context = json.loads(args.context_file.read_text(encoding="utf-8")) if args.context_file else {}
            if (not isinstance(context, dict) or set(context) - {"request", "config", "dependency", "metadata", "calendar", "credential_ref"}
                    or any(not isinstance(value, dict) for value in context.values())):
                raise ValueError("context file must contain supported parameter namespace objects")
            context.setdefault("request", {}).update(request_context)
            context.setdefault("config", {}).update({"count": args.count} if args.count is not None else {})
            if args.context_file:
                context.setdefault("metadata", {})["context_path"] = str(args.context_file.resolve())
            if "trading_dates" in context.get("calendar", {}):
                context["calendar"]["trading_dates"] = [date.fromisoformat(str(day)) for day in context["calendar"]["trading_dates"]]
            if args.calendar_file:
                saved_days = json.loads(args.calendar_file.read_text(encoding="utf-8"))
                if not isinstance(saved_days, list):
                    raise ValueError("calendar file must contain an explicit date-row array")
                context["calendar"] = {"trading_dates": [date.fromisoformat(str(row["trade_date"])) for row in saved_days]}
                context.setdefault("metadata", {})["calendar_path"] = str(args.calendar_file.resolve())
            report = collect_input(input_id=args.input, context=context,
                config_root=args.config_root, output_root=args.output_root, mode=args.mode,
                replay_manifest=args.replay_manifest, evidence_root=args.evidence_root,
                fields=[name.strip() for name in args.fields.split(",")] if args.fields else None,
                **({"data_root": args.data_root} if args.data_root is not None else {}))
        except ValueError as exc:
            parser.error(str(exc))
        print(json.dumps(report, ensure_ascii=False, default=_json_default, indent=2))
        return 0 if report["status"] == "candidate_complete" else 2
    if args.command == "probe-daily":
        if args.adjustment == "forward" and args.provider != "tencent":
            parser.error("forward daily probe currently requires --provider tencent")
        if args.adjustment == "forward" and args.asset_type == "index":
            parser.error("index daily bars cannot use forward adjustment")
        adjustment = Adjustment(args.adjustment)
        provider = (
            SinaDailyProvider(UrlLibTransport())
            if args.provider == "sina"
            else TencentDailyProvider(UrlLibTransport(), adjustment=adjustment)
        )
        now = datetime.now(timezone.utc)
        evidence = probe_daily_capability(
            provider,
            symbol=args.symbol,
            trade_date=args.trade_date,
            now=now,
            ttl=timedelta(days=args.ttl_days),
        )
        if args.metadata is not None:
            with MetadataStore(args.metadata) as metadata:
                metadata.save_probe_evidence(
                    evidence,
                    dataset="daily_bar",
                    market=args.market,
                    asset_type=args.asset_type,
                    code_prefix=args.code_prefix,
                    adjustment=args.adjustment,
                )
        print(json.dumps(asdict(evidence), ensure_ascii=False, default=_json_default, indent=2))
        return 0 if evidence.eligible_for_selection else 2

    if args.command == "probe-minute":
        transport = UrlLibTransport()
        capability = _minute_probe_capability(args.provider, args.frequency, args.as_of, args.ttl_days)
        if args.provider == "tencent":
            provider = TencentMinuteProvider(transport, capability)
        else:
            provider = SinaMinuteProvider(transport, capability)
        evidence = probe_minute_capability(
            provider,
            symbol=args.symbol,
            as_of=args.as_of,
            ttl=timedelta(days=args.ttl_days),
        )
        if args.metadata is not None:
            with MetadataStore(args.metadata) as metadata:
                dataset = "minute_bar_1m" if args.frequency == 1 else "minute_bar_5m"
                metadata.save_probe_evidence(
                    evidence,
                    dataset=dataset,
                    market=args.market,
                    asset_type=args.asset_type,
                    code_prefix=args.code_prefix,
                )
        print(json.dumps(asdict(evidence), ensure_ascii=False, default=_json_default, indent=2))
        return 0 if evidence.eligible_for_selection else 2

    if args.command == "acceptance-offline":
        report = run_offline_acceptance(args.root, end_date=args.end_date)
        payload = json.dumps(report.to_dict(), ensure_ascii=False, indent=2)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(payload + "\n", encoding="utf-8")
        print(payload)
        return 0

    from .config.loader import load_storage_paths
    from .worker.recovery import archive_published_task, recover_collection_tasks
    # Preserve the existing explicit recovery contract outside a project checkout.
    paths = load_storage_paths(args.config_root, data_root=args.data_root) if (
        not args.canonical_root or not args.metadata or args.archive_task or args.data_root) else None
    canonical_root = args.canonical_root or paths["canonical_root"]
    try:
        recovered_tasks = recover_collection_tasks(config_root=args.config_root, data_root=args.data_root) if (
            paths and not args.metadata and not args.canonical_root) else ()
        with MetadataStore(args.metadata or paths["metadata_path"]) as metadata:
            report = RecoveryScanner(canonical_root, metadata).recover()
            archived = [str(archive_published_task(task, workspace_root=paths["workspace_root"],
                archive_root=paths["archive_root"], canonical_root=canonical_root,
                raw_root=paths["raw_root"], metadata=metadata)) for task in args.archive_task]
    except ValueError as exc:
        parser.error(str(exc))
    output = asdict(report)
    if recovered_tasks:
        output["recovered_tasks"] = recovered_tasks
    if args.archive_task:
        output["archived_tasks"] = archived
    print(json.dumps(output, ensure_ascii=False, default=_json_default, indent=2))
    return 0 if not report.invalid_final_partitions else 2


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"cannot encode {type(value).__name__}")


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _minute_probe_capability(
    provider: str, frequency: int, as_of: datetime, ttl_days: int
) -> ProviderCapability:
    endpoint = "native_1m" if provider == "tencent" else "native_5m"
    dataset = Dataset.MINUTE_BAR_1M if frequency == 1 else Dataset.MINUTE_BAR_5M
    return ProviderCapability(
        provider=provider,
        endpoint=endpoint,
        version=f"{provider}-minute-probe-v1",
        datasets=frozenset({dataset}),
        exchanges=frozenset({Exchange.XSHG, Exchange.XSHE, Exchange.BSE}),
        asset_types=frozenset({AssetType.STOCK, AssetType.ETF, AssetType.LOF, AssetType.INDEX}),
        frequencies=frozenset({frequency}),
        adjustments=frozenset({Adjustment.NONE}),
        priority=100,
        validated_at=as_of - timedelta(minutes=1),
        validation_expires_at=as_of + timedelta(days=ttl_days),
        max_symbols_per_request=1,
    )


if __name__ == "__main__":
    raise SystemExit(main())
