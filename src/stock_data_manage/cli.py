from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .acceptance import run_offline_acceptance
from .capability import ProviderCapability
from .capability_probe import probe_daily_capability, probe_minute_capability
from .domain import Adjustment, AssetType, Dataset, Exchange
from .http_providers import (
    SinaDailyProvider,
    SinaMinuteProvider,
    TencentDailyProvider,
    TencentMinuteProvider,
    UrlLibTransport,
)
from .metadata import MetadataStore
from .recovery import RecoveryScanner


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

    recover = subcommands.add_parser("recover", help="repair canonical files and metadata after interruption")
    recover.add_argument("--canonical-root", type=Path, required=True)
    recover.add_argument("--metadata", type=Path, required=True)

    acceptance = subcommands.add_parser(
        "acceptance-offline", help="run deterministic offline M1 acceptance evidence"
    )
    acceptance.add_argument("--root", type=Path, required=True)
    acceptance.add_argument("--end-date", type=date.fromisoformat, default=date(2026, 9, 11))
    acceptance.add_argument("--output", type=Path)

    args = parser.parse_args(argv)
    if args.command == "probe-daily":
        provider = (
            SinaDailyProvider(UrlLibTransport())
            if args.provider == "sina"
            else TencentDailyProvider(UrlLibTransport())
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

    with MetadataStore(args.metadata) as metadata:
        report = RecoveryScanner(args.canonical_root, metadata).recover()
    print(json.dumps(asdict(report), ensure_ascii=False, default=_json_default, indent=2))
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
