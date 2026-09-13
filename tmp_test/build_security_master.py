"""Merge the measured equity and ETF/LOF universes into one security master."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--equity-run",
        type=Path,
        default=BASE_DIR / "market_day_probe" / "run_20260912_184212",
    )
    parser.add_argument("--fund-run", type=Path, help="默认读取 etf_full_probe/LATEST.txt")
    parser.add_argument(
        "--eastmoney-lof-run",
        type=Path,
        default=BASE_DIR / "etf_daily_probe" / "run_20260913_132826",
        help="东财LOF快照运行目录；存在时加入多源证券并集",
    )
    return parser.parse_args()


def market_name(symbol: str) -> str:
    return {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}[symbol[:2]]


def eastmoney_symbol(symbol: str) -> str:
    prefix = "1." if symbol.startswith("sh") else "0."
    return prefix + symbol[2:]


def instrument_id(symbol: str, instrument_type: str) -> str:
    return f"CN.{market_name(symbol)}.{instrument_type.upper()}.{symbol[2:]}"


def equity_union(run_dir: Path) -> pd.DataFrame:
    bao = pd.read_parquet(run_dir / "baostock" / "universe.parquet")
    bao_part = pd.DataFrame(
        {
            "code": bao["code"].astype(str).str.replace(".", "", regex=False).str.lower(),
            "name": bao["code_name"].astype(str),
            "active": bao["tradeStatus"].astype(str).eq("1"),
            "sources": "baostock",
        }
    )
    sina = pd.read_parquet(run_dir / "sina" / "stocks.parquet")
    code_col, name_col = sina.columns[:2]
    sina_part = pd.DataFrame(
        {
            "code": sina[code_col].astype(str).str.lower(),
            "name": sina[name_col].astype(str),
            "active": True,
            "sources": "sina",
        }
    )
    values = pd.concat([bao_part, sina_part], ignore_index=True)
    values = values[values["code"].str.match(r"^(sh|sz|bj)\d{6}$", na=False)]
    records = []
    for code, group in values.groupby("code", sort=True):
        records.append(
            {
                "code": code,
                "name": group["name"].iloc[0],
                "instrument_type": "equity",
                "active": bool(group["active"].any()),
                "universe_sources": ",".join(sorted(set(group["sources"]))),
            }
        )
    return pd.DataFrame(records)


def main() -> int:
    args = parse_args()
    fund_run = args.fund_run
    if fund_run is None:
        fund_run = Path((BASE_DIR / "etf_full_probe" / "LATEST.txt").read_text(encoding="utf-8").strip())
    equity = equity_union(args.equity_run.resolve())
    funds = pd.read_parquet(fund_run.resolve() / "instrument_master.parquet")
    funds["active"] = True
    lof_snapshot = args.eastmoney_lof_run.resolve() / "eastmoney" / "snapshot.csv"
    if lof_snapshot.exists():
        raw_lof = pd.read_csv(lof_snapshot, dtype={"f12": str}, low_memory=False)
        market_prefix = raw_lof["f13"].astype(str).map({"1": "sh", "1.0": "sh", "0": "sz", "0.0": "sz"})
        em_lof = pd.DataFrame(
            {
                "code": market_prefix.fillna("") + raw_lof["f12"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6),
                "name": raw_lof["f14"].astype(str),
                "instrument_type": "lof",
                "market": market_prefix,
                "sources": "eastmoney",
                "source_categories": "LOF基金",
                "active": pd.to_numeric(raw_lof["f2"], errors="coerce").notna(),
            }
        )
        funds = pd.concat([funds, em_lof], ignore_index=True)
        merged_funds: list[dict] = []
        for code, group in funds.groupby("code", sort=True):
            names = [value for value in group["name"].astype(str).tolist() if value and value != "nan"]
            merged_funds.append(
                {
                    "code": code,
                    "name": names[0] if names else "",
                    "instrument_type": "etf" if "etf" in set(group["instrument_type"]) else "lof",
                    "market": code[:2],
                    "sources": ",".join(sorted({item for value in group["sources"].astype(str) for item in value.split(",")})),
                    "source_categories": ",".join(sorted({item for value in group["source_categories"].astype(str) for item in value.split(",")})),
                    "active": bool(group["active"].any()),
                }
            )
        funds = pd.DataFrame(merged_funds)
    funds = funds.rename(columns={"sources": "universe_sources"})
    combined = pd.concat(
        [
            equity[["code", "name", "instrument_type", "active", "universe_sources"]],
            funds[["code", "name", "instrument_type", "active", "universe_sources"]],
        ],
        ignore_index=True,
    )
    combined["market"] = combined["code"].map(market_name)
    combined["instrument_id"] = [instrument_id(code, kind) for code, kind in zip(combined["code"], combined["instrument_type"])]
    combined["source_symbols"] = combined["code"].map(
        lambda code: json.dumps(
            {
                "baostock": code[:2] + "." + code[2:],
                "eastmoney": eastmoney_symbol(code),
                "tencent": code,
                "sina": code,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    combined = combined[
        ["instrument_id", "market", "code", "name", "instrument_type", "active", "universe_sources", "source_symbols"]
    ].sort_values(["instrument_type", "market", "code"])
    if combined["instrument_id"].duplicated().any():
        raise RuntimeError("合并证券主表出现重复 instrument_id")
    out_dir = fund_run.resolve()
    if lof_snapshot.exists():
        fund_output = funds.rename(columns={"universe_sources": "sources"})
        fund_output.to_csv(out_dir / "instrument_master_all_sources.csv", index=False, encoding="utf-8-sig")
        fund_output.to_parquet(out_dir / "instrument_master_all_sources.parquet", index=False)
        combined.to_csv(out_dir / "security_master_all_sources.csv", index=False, encoding="utf-8-sig")
        combined.to_parquet(out_dir / "security_master_all_sources.parquet", index=False)
    else:
        combined.to_csv(out_dir / "security_master.csv", index=False, encoding="utf-8-sig")
        combined.to_parquet(out_dir / "security_master.parquet", index=False)
    summary = {
        "rows": len(combined),
        "by_type": combined["instrument_type"].value_counts().to_dict(),
        "by_market": combined["market"].value_counts().to_dict(),
        "equity_run": str(args.equity_run.resolve()),
        "fund_run": str(fund_run.resolve()),
    }
    summary_name = "security_master_all_sources_summary.json" if lof_snapshot.exists() else "security_master_summary.json"
    (out_dir / summary_name).write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
