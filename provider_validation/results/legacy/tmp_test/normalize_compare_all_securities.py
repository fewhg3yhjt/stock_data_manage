"""Normalize and compare all currently saved equities, ETFs and LOFs."""

from __future__ import annotations

import hashlib
import itertools
import json
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from normalize_compare_daily import COMPARE_TOLERANCES, NORMALIZER_VERSION, to_numeric


BASE_DIR = Path(__file__).resolve().parent
EQUITY_RUN = BASE_DIR / "market_day_probe" / "run_20260912_184212"
FUND_RUN = BASE_DIR / "etf_full_probe" / "run_20260912_220930"
ETF_SNAPSHOT_RUN = BASE_DIR / "etf_daily_probe" / "run_20260912_221825"
LOF_SNAPSHOT_RUN = BASE_DIR / "etf_daily_probe" / "run_20260913_132826"
LOF_TENCENT_EXTRA_RUN = BASE_DIR / "etf_daily_probe" / "run_20260913_133214"
ETF_HISTORY_SAMPLE_RUN = BASE_DIR / "etf_daily_probe" / "run_20260912_222058"
OUTPUT_DIR = BASE_DIR / "security_comparison" / "trade_date_20260911"
CONTRACT_PATH = BASE_DIR / "market_data_contract.yaml"


def market_of(symbol: str) -> str:
    return {"sh": "XSHG", "sz": "XSHE", "bj": "BSE"}[symbol[:2]]


def make_instrument_id(symbol: str, kind: str) -> str:
    return f"CN.{market_of(symbol)}.{kind.upper()}.{symbol[2:]}"


def target_template(rows: int, columns: list[str]) -> pd.DataFrame:
    index = pd.RangeIndex(rows)
    return pd.DataFrame({column: pd.Series(pd.NA, index=index, dtype="object") for column in columns})


def apply_context(
    out: pd.DataFrame,
    symbols: pd.Series,
    kinds: pd.Series,
    source: str,
    adapter: str,
    fetched_at: str,
    status: str,
) -> None:
    out["instrument_id"] = [make_instrument_id(symbol, kind) for symbol, kind in zip(symbols, kinds)]
    out["instrument_type"] = kinds.values
    out["trade_date"] = "2026-09-11"
    out["adjustment"] = "none"
    out["source_family"] = source
    out["source_adapter"] = adapter
    out["source_symbol"] = symbols.values
    out["fetched_at"] = fetched_at
    out["run_id"] = "all_security_comparison_20260911"
    out["record_status"] = status
    out["trade_status"] = "unknown"
    out["quality_flags"] = [[] for _ in range(len(out))]
    out["normalizer_version"] = NORMALIZER_VERSION


def finish(out: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    core = ["instrument_id", "trade_date", "adjustment", "open", "high", "low", "close", "pre_close", "volume_shares", "amount_cny"]
    out["row_hash"] = out.apply(
        lambda row: hashlib.sha256(
            "|".join("" if pd.isna(row[item]) else str(row[item]) for item in core).encode("utf-8")
        ).hexdigest(),
        axis=1,
    )
    result = out[columns].copy().sort_values("instrument_id").reset_index(drop=True)
    if result.duplicated(["instrument_id", "trade_date", "adjustment"]).any():
        raise RuntimeError("来源内出现重复证券主键")
    return result


def master_types() -> dict[str, str]:
    master = pd.read_parquet(FUND_RUN / "instrument_master_all_sources.parquet")
    return dict(zip(master["code"].astype(str), master["instrument_type"].astype(str)))


def normalize_tencent(columns: list[str], kind_map: dict[str, str]) -> pd.DataFrame:
    raw = pd.concat(
        [
            pd.read_parquet(ETF_SNAPSHOT_RUN / "tencent" / "daily.parquet"),
            pd.read_parquet(LOF_SNAPSHOT_RUN / "tencent" / "daily.parquet"),
            pd.read_parquet(LOF_TENCENT_EXTRA_RUN / "tencent" / "daily.parquet"),
        ],
        ignore_index=True,
    )
    symbols = raw["source_symbol"].astype(str).str.lower()
    kinds = symbols.map(kind_map)
    out = target_template(len(raw), columns)
    apply_context(out, symbols, kinds, "tencent", "raw_http", "2026-09-12T22:20:06+08:00", "provisional")
    for source, target in (("open", "open"), ("high", "high"), ("low", "low"), ("close", "close"), ("pre_close", "pre_close"), ("pct_change", "change_pct")):
        out[target] = to_numeric(raw[source])
    out["volume_shares"] = to_numeric(raw["volume_raw_lot"]) * 100
    out["amount_cny"] = to_numeric(raw["amount_raw_wan"]) * 10000
    out["source_timestamp"] = raw["quote_time"].astype(str)
    out["quality_flags"] = [["snapshot_provisional", "volume_lot_x100", "amount_wan_x10000"] for _ in range(len(out))]
    return finish(out, columns)


def normalize_sina_file(path: Path, kind: str, columns: list[str]) -> pd.DataFrame:
    raw = pd.read_parquet(path).copy()
    raw.columns = ["symbol", "name", "close", "change_amount", "change_pct", "buy", "sell", "pre_close", "open", "high", "low", "volume_shares", "amount_cny"]
    symbols = raw["symbol"].astype(str).str.lower()
    kinds = pd.Series(kind, index=raw.index)
    out = target_template(len(raw), columns)
    apply_context(out, symbols, kinds, "sina", "fund_etf_category_sina", "2026-09-12T22:10:25+08:00", "provisional")
    for field in ("open", "high", "low", "close", "pre_close", "volume_shares", "amount_cny", "change_amount", "change_pct"):
        out[field] = to_numeric(raw[field])
    out["quality_flags"] = [["snapshot_provisional"] for _ in range(len(out))]
    return finish(out, columns)


def normalize_sina(columns: list[str]) -> pd.DataFrame:
    return pd.concat(
        [
            normalize_sina_file(FUND_RUN / "sina_etf" / "snapshot_raw.parquet", "etf", columns),
            normalize_sina_file(FUND_RUN / "sina_lof" / "snapshot_raw.parquet", "lof", columns),
        ],
        ignore_index=True,
    ).sort_values("instrument_id").reset_index(drop=True)


def normalize_eastmoney(columns: list[str], kind_map: dict[str, str]) -> pd.DataFrame:
    raw = pd.concat(
        [
            pd.read_parquet(ETF_SNAPSHOT_RUN / "eastmoney" / "snapshot.parquet"),
            pd.read_csv(LOF_SNAPSHOT_RUN / "eastmoney" / "snapshot.csv", dtype={"f12": str}, low_memory=False),
        ],
        ignore_index=True,
    )
    prefix = raw["f13"].astype(str).map({"1": "sh", "1.0": "sh", "0": "sz", "0.0": "sz"}).fillna("")
    symbols = prefix + raw["f12"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    kinds = symbols.map(kind_map).fillna("etf")
    out = target_template(len(raw), columns)
    apply_context(out, symbols, kinds, "eastmoney", "paced_raw_http", "2026-09-12T22:10:25+08:00", "provisional")
    mapping = {
        "open": "f17",
        "high": "f15",
        "low": "f16",
        "close": "f2",
        "pre_close": "f18",
        "volume_shares": "f5",
        "amount_cny": "f6",
        "change_amount": "f4",
        "change_pct": "f3",
        "amplitude_pct": "f7",
        "turnover_rate_pct": "f8",
    }
    for target, source in mapping.items():
        out[target] = to_numeric(raw[source])
    out["volume_shares"] = out["volume_shares"] * 100
    out["quality_flags"] = [["snapshot_provisional", "volume_lot_x100"] for _ in range(len(out))]
    return finish(out, columns)


def normalize_baostock_sample(columns: list[str]) -> pd.DataFrame:
    raw = pd.read_parquet(ETF_HISTORY_SAMPLE_RUN / "baostock" / "daily_rows.parquet")
    symbols = raw["code"].astype(str).str.lower().str.replace(".", "", regex=False)
    kinds = raw["instrument_type"].astype(str)
    out = target_template(len(raw), columns)
    apply_context(out, symbols, kinds, "baostock", "baostock_python", "2026-09-12T22:21:20+08:00", "confirmed")
    mapping = {
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "pre_close": "preclose",
        "volume_shares": "volume",
        "amount_cny": "amount",
        "change_pct": "pctChg",
        "turnover_rate_pct": "turn",
    }
    for target, source in mapping.items():
        out[target] = to_numeric(raw[source])
    out["trade_status"] = raw["tradestatus"].astype(str).map({"1": "trading", "0": "suspended"}).fillna("unknown")
    return finish(out, columns)


def load_equity(source: str) -> pd.DataFrame:
    return pd.read_parquet(EQUITY_RUN / source / "normalized" / "daily_bar.parquet")


def save_source(frame: pd.DataFrame, source: str) -> None:
    directory = OUTPUT_DIR / source
    directory.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(directory / "daily_bar.parquet", index=False)
    csv_frame = frame.copy()
    csv_frame["quality_flags"] = csv_frame["quality_flags"].map(
        lambda value: json.dumps(value.tolist() if hasattr(value, "tolist") else value, ensure_ascii=False)
    )
    csv_frame.to_csv(directory / "daily_bar.csv", index=False, encoding="utf-8-sig")


def compare(frames: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    coverage_rows: list[dict[str, Any]] = []
    field_rows: list[dict[str, Any]] = []
    keys = ["instrument_id", "trade_date", "adjustment"]
    for source, frame in frames.items():
        for kind in ("equity", "etf", "lof"):
            selected = frame[frame["instrument_type"].eq(kind)]
            valid_close = pd.to_numeric(selected["close"], errors="coerce").gt(0)
            valid_trade = pd.to_numeric(selected["open"], errors="coerce").gt(0)
            coverage_rows.append(
                {
                    "source": source,
                    "instrument_type": kind,
                    "rows": len(selected),
                    "valid_close_rows": int(valid_close.sum()),
                    "valid_trade_rows": int(valid_trade.sum()),
                }
            )
    for left_name, right_name in itertools.combinations(frames, 2):
        for kind in ("equity", "etf", "lof"):
            left = frames[left_name][frames[left_name]["instrument_type"].eq(kind)]
            right = frames[right_name][frames[right_name]["instrument_type"].eq(kind)]
            joined = left[keys + list(COMPARE_TOLERANCES)].merge(
                right[keys + list(COMPARE_TOLERANCES)], on=keys, how="inner", suffixes=("_left", "_right")
            )
            active_mask = pd.to_numeric(joined["open_left"], errors="coerce").gt(0) & pd.to_numeric(
                joined["open_right"], errors="coerce"
            ).gt(0)
            for scope, scoped in (("all", joined), ("active", joined[active_mask])):
                for field, tolerance in COMPARE_TOLERANCES.items():
                    a = pd.to_numeric(scoped[f"{field}_left"], errors="coerce")
                    b = pd.to_numeric(scoped[f"{field}_right"], errors="coerce")
                    valid = a.notna() & b.notna()
                    diff = (a[valid] - b[valid]).abs()
                    field_rows.append(
                        {
                            "left_source": left_name,
                            "right_source": right_name,
                            "instrument_type": kind,
                            "scope": scope,
                            "field": field,
                            "common_rows": len(scoped),
                            "compared_rows": len(diff),
                            "exact_rate": float(diff.le(1e-9).mean()) if len(diff) else None,
                            "within_tolerance_rate": float(diff.le(tolerance).mean()) if len(diff) else None,
                            "over_tolerance_rows": int(diff.gt(tolerance).sum()) if len(diff) else 0,
                            "max_abs_diff": float(diff.max()) if len(diff) else None,
                            "tolerance": tolerance,
                        }
                    )
    return pd.DataFrame(coverage_rows), pd.DataFrame(field_rows)


def write_report(coverage: pd.DataFrame, fields: pd.DataFrame) -> None:
    lines = [
        "# 全证券统一格式覆盖与一致性报告",
        "",
        "交易日：2026-09-11。范围为股票、ETF、LOF；指数及板块另列能力矩阵。",
        "",
        "## 当前统一文件覆盖",
        "",
        "| 来源 | 股票 | ETF | LOF | 合计 | 有效收盘价 | 有效开盘价 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    pivot = coverage.pivot(index="source", columns="instrument_type", values="rows").fillna(0).astype(int)
    valid_pivot = coverage.pivot(index="source", columns="instrument_type", values="valid_close_rows").fillna(0).astype(int)
    trade_pivot = coverage.pivot(index="source", columns="instrument_type", values="valid_trade_rows").fillna(0).astype(int)
    for source in ("baostock", "tencent", "sina", "eastmoney"):
        row = pivot.loc[source]
        total = int(row.sum())
        valid_total = int(valid_pivot.loc[source].sum())
        trade_total = int(trade_pivot.loc[source].sum())
        lines.append(f"| {source} | {row.get('equity', 0):,} | {row.get('etf', 0):,} | {row.get('lof', 0):,} | {total:,} | {valid_total:,} | {trade_total:,} |")
    lines += [
        "",
        "BaoStock的ETF为4只历史样本，不是全量；东财股票为100只北交所首批样本。缺失值表示来源尚未取得该类全量数据，不等于接口永久不支持。",
        "",
        "## ETF核心字段两两比较",
        "",
        "| 来源对 | 共同ETF | 字段 | 容差内一致率 | 超容差 | 最大绝对差 |",
        "|---|---:|---|---:|---:|---:|",
    ]
    selected = fields[(fields["instrument_type"] == "etf") & (fields["scope"] == "active") & fields["field"].isin(["open", "high", "low", "close", "pre_close", "volume_shares", "amount_cny"])]
    selected = selected[selected["compared_rows"] > 0]
    for row in selected.to_dict("records"):
        rate = f"{row['within_tolerance_rate']:.2%}"
        maximum = "-" if pd.isna(row["max_abs_diff"]) else f"{row['max_abs_diff']:.6g}"
        lines.append(
            f"| {row['left_source']} vs {row['right_source']} | {int(row['common_rows']):,} | {row['field']} | {rate} | {int(row['over_tolerance_rows']):,} | {maximum} |"
        )
    lines += [
        "",
        "## LOF有效交易记录两两比较",
        "",
        "| 来源对 | 共同有效LOF | 字段 | 容差内一致率 | 超容差 | 最大绝对差 |",
        "|---|---:|---|---:|---:|---:|",
    ]
    lof_selected = fields[(fields["instrument_type"] == "lof") & (fields["scope"] == "active") & fields["field"].isin(["open", "high", "low", "close", "pre_close", "volume_shares", "amount_cny"])]
    lof_selected = lof_selected[lof_selected["compared_rows"] > 0]
    for row in lof_selected.to_dict("records"):
        rate = f"{row['within_tolerance_rate']:.2%}"
        maximum = "-" if pd.isna(row["max_abs_diff"]) else f"{row['max_abs_diff']:.6g}"
        lines.append(
            f"| {row['left_source']} vs {row['right_source']} | {int(row['common_rows']):,} | {row['field']} | {rate} | {int(row['over_tolerance_rows']):,} | {maximum} |"
        )
    lines += [
        "",
        "## 当前结论",
        "",
        "- ETF已经进入统一目标格式并独立统计，不再混在股票结果中。",
        "- LOF多源并集为425只；腾讯覆盖425只，新浪382只，东财390只，可对重合部分做跨来源一致性确认。",
        "- 腾讯与新浪有33只共同LOF无当日成交：腾讯close填昨收，新浪close为0；这些记录不进入有效交易一致率。",
        "- BaoStock需要对ETF/LOF执行逐只历史日线补抓后，才能宣称该来源覆盖所有证券。",
        "- 指数、行业板块、概念板块不是可交易证券，继续使用独立instrument_type；仅在有两个来源覆盖同一标的时才计算一致率。",
    ]
    (OUTPUT_DIR / "ALL_SECURITY_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    contract = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    columns = list(contract["targets"]["daily_bar"]["fields"])
    kinds = master_types()
    fund_frames = {
        "baostock": normalize_baostock_sample(columns),
        "tencent": normalize_tencent(columns, kinds),
        "sina": normalize_sina(columns),
        "eastmoney": normalize_eastmoney(columns, kinds),
    }
    frames: dict[str, pd.DataFrame] = {}
    for source in fund_frames:
        frame = pd.concat([load_equity(source), fund_frames[source]], ignore_index=True)
        if frame.duplicated(["instrument_id", "trade_date", "adjustment"]).any():
            raise RuntimeError(f"{source} 股票与基金合并后出现重复主键")
        frame = frame.sort_values("instrument_id").reset_index(drop=True)
        save_source(frame, source)
        frames[source] = frame
    coverage, fields = compare(frames)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    coverage.to_csv(OUTPUT_DIR / "coverage.csv", index=False, encoding="utf-8-sig")
    fields.to_csv(OUTPUT_DIR / "field_comparison.csv", index=False, encoding="utf-8-sig")
    write_report(coverage, fields)
    print(json.dumps({"output_dir": str(OUTPUT_DIR), "coverage": coverage.to_dict("records")}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
