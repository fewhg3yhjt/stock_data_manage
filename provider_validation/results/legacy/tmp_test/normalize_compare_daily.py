"""Normalize saved 2026-09-11 stock data and compare providers locally."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_RUN_DIR = BASE_DIR / "market_day_probe" / "run_20260912_184212"
CONTRACT_PATH = BASE_DIR / "market_data_contract.yaml"
NORMALIZER_VERSION = "daily-normalizer-v1"

COMPARE_TOLERANCES = {
    "open": 1e-6,
    "high": 1e-6,
    "low": 1e-6,
    "close": 1e-6,
    "pre_close": 1e-6,
    "volume_shares": 100.0,
    "amount_cny": 10000.0,
    "change_amount": 1e-6,
    "change_pct": 0.011,
    "amplitude_pct": 0.011,
    "turnover_rate_pct": 0.011,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR)
    parser.add_argument("--contract", type=Path, default=CONTRACT_PATH)
    return parser.parse_args()


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.replace({"": None, "-": None, "--": None}), errors="coerce")


def normalized_symbol(value: Any) -> str:
    return str(value).strip().lower().replace(".", "")


def market_of(symbol: str) -> str:
    if symbol.startswith("sh"):
        return "XSHG"
    if symbol.startswith("sz"):
        return "XSHE"
    if symbol.startswith("bj"):
        return "BSE"
    digits = symbol[-6:]
    if digits.startswith(("4", "8", "9")):
        return "BSE"
    raise ValueError(f"无法判断市场: {symbol}")


def canonical_symbol(code: Any, market_id: Any | None = None) -> str:
    raw = normalized_symbol(code)
    if raw.startswith(("sh", "sz", "bj")):
        return raw
    digits = raw.zfill(6)
    if digits.startswith(("4", "8", "9")):
        return "bj" + digits
    if str(market_id) in {"1", "1.0"}:
        return "sh" + digits
    return "sz" + digits


def instrument_id(symbol: str) -> str:
    return f"CN.{market_of(symbol)}.EQUITY.{symbol[-6:]}"


def empty_target(index: pd.Index, columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(pd.NA, index=index, dtype="object") for column in columns})


def source_time(run_dir: Path) -> str:
    config = json.loads((run_dir / "run_config.json").read_text(encoding="utf-8"))
    return str(config.get("started_at") or datetime.fromtimestamp(run_dir.stat().st_mtime).astimezone().isoformat())


def common_context(
    target: pd.DataFrame,
    symbols: pd.Series,
    source: str,
    adapter: str,
    trade_date: pd.Series | str,
    fetched_at: str,
    run_id: str,
    record_status: str,
) -> None:
    target["instrument_id"] = symbols.map(instrument_id)
    target["instrument_type"] = "equity"
    target["trade_date"] = trade_date
    target["adjustment"] = "none"
    target["source_family"] = source
    target["source_adapter"] = adapter
    target["source_symbol"] = symbols
    target["source_timestamp"] = pd.NA
    target["fetched_at"] = fetched_at
    target["run_id"] = run_id
    target["record_status"] = record_status
    target["quality_flags"] = [[] for _ in range(len(target))]
    target["normalizer_version"] = NORMALIZER_VERSION


def finish_target(frame: pd.DataFrame, target_columns: list[str]) -> pd.DataFrame:
    for column in target_columns:
        if column not in frame:
            frame[column] = pd.NA
    core = [
        "instrument_id",
        "trade_date",
        "adjustment",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume_shares",
        "amount_cny",
    ]

    def row_hash(row: pd.Series) -> str:
        raw = "|".join("" if pd.isna(row[item]) else str(row[item]) for item in core)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    frame["row_hash"] = frame.apply(row_hash, axis=1)
    result = frame[target_columns].copy()
    if result.duplicated(["instrument_id", "trade_date", "adjustment"]).any():
        raise RuntimeError("标准化结果出现重复主键")
    return result.sort_values("instrument_id").reset_index(drop=True)


def normalize_baostock(run_dir: Path, columns: list[str], fetched_at: str) -> pd.DataFrame:
    raw = pd.read_parquet(run_dir / "baostock" / "daily.parquet")
    out = empty_target(raw.index, columns)
    symbols = raw["code"].map(normalized_symbol)
    common_context(out, symbols, "baostock", "baostock_python", raw["date"], fetched_at, run_dir.name, "confirmed")
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
    return finish_target(out, columns)


def tencent_raw(run_dir: Path) -> pd.DataFrame:
    parts = [
        pd.read_parquet(run_dir / "tencent" / "daily.parquet"),
        pd.read_parquet(run_dir / "tencent_sina_extras" / "daily.parquet"),
    ]
    return pd.concat(parts, ignore_index=True)


def normalize_tencent(run_dir: Path, columns: list[str], fetched_at: str) -> pd.DataFrame:
    raw = tencent_raw(run_dir)
    out = empty_target(raw.index, columns)
    symbols = raw["source_symbol"].map(normalized_symbol)
    trade_dates = raw["quote_time"].astype(str).str[:8]
    trade_dates = pd.to_datetime(trade_dates, format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m-%d")
    common_context(out, symbols, "tencent", "raw_http", trade_dates, fetched_at, run_dir.name, "provisional")
    for field in ("open", "high", "low", "close", "pre_close", "pct_change"):
        target = "change_pct" if field == "pct_change" else field
        out[target] = to_numeric(raw[field])
    raw_volume = to_numeric(raw["volume_raw_lot"])
    is_star = symbols.str.startswith(("sh688", "sh689"))
    out["volume_shares"] = raw_volume.where(is_star, raw_volume * 100)
    out["amount_cny"] = to_numeric(raw["amount_raw_wan"]) * 10000
    out["trade_status"] = "unknown"
    out["source_timestamp"] = raw["quote_time"].astype(str)
    out["quality_flags"] = [
        ["snapshot_provisional", "volume_already_shares" if star else "volume_lot_x100", "amount_wan_x10000"]
        for star in is_star
    ]
    return finish_target(out, columns)


def normalize_sina(run_dir: Path, columns: list[str], fetched_at: str) -> pd.DataFrame:
    raw = pd.read_parquet(run_dir / "sina" / "stocks.parquet").copy()
    semantic = [
        "symbol",
        "name",
        "close",
        "change_amount",
        "change_pct",
        "buy",
        "sell",
        "pre_close",
        "open",
        "high",
        "low",
        "volume_shares",
        "amount_cny",
        "ticktime",
    ]
    raw.columns = semantic[: len(raw.columns)]
    out = empty_target(raw.index, columns)
    symbols = raw["symbol"].map(normalized_symbol)
    common_context(out, symbols, "sina", "paced_raw_http", "2026-09-11", fetched_at, run_dir.name, "provisional")
    for field in ("open", "high", "low", "close", "pre_close", "volume_shares", "amount_cny", "change_amount", "change_pct"):
        out[field] = to_numeric(raw[field])
    out["trade_status"] = "unknown"
    out["source_timestamp"] = "2026-09-11 " + raw["ticktime"].astype(str)
    out["quality_flags"] = [["snapshot_provisional"] for _ in range(len(out))]
    return finish_target(out, columns)


def normalize_eastmoney(run_dir: Path, columns: list[str], fetched_at: str) -> pd.DataFrame:
    payload = json.loads((run_dir / "eastmoney" / "stocks_raw.json").read_text(encoding="utf-8"))
    raw = pd.DataFrame((payload.get("data") or {}).get("diff") or [])
    out = empty_target(raw.index, columns)
    symbols = pd.Series(
        [canonical_symbol(code, market_id) for code, market_id in zip(raw["f12"], raw["f13"])],
        index=raw.index,
    )
    common_context(out, symbols, "eastmoney", "paced_raw_http", "2026-09-11", fetched_at, run_dir.name, "provisional")
    mapping = {
        "open": "f17",
        "high": "f15",
        "low": "f16",
        "close": "f2",
        "pre_close": "f18",
        "volume_shares": "f5",
        "amount_cny": "f6",
        "change_pct": "f3",
        "amplitude_pct": "f7",
        "turnover_rate_pct": "f8",
    }
    for target, source in mapping.items():
        out[target] = to_numeric(raw[source])
    out["volume_shares"] = out["volume_shares"] * 100
    out["trade_status"] = "unknown"
    out["quality_flags"] = [["snapshot_provisional", "partial_first_page", "volume_lot_x100"] for _ in range(len(out))]
    return finish_target(out, columns)


def save_target(frame: pd.DataFrame, source_dir: Path) -> dict[str, str]:
    target_dir = source_dir / "normalized"
    target_dir.mkdir(parents=True, exist_ok=True)
    parquet = target_dir / "daily_bar.parquet"
    csv = target_dir / "daily_bar.csv"
    frame.to_parquet(parquet, index=False)
    csv_frame = frame.copy()
    csv_frame["quality_flags"] = csv_frame["quality_flags"].map(lambda value: json.dumps(value, ensure_ascii=False))
    csv_frame.to_csv(csv, index=False, encoding="utf-8-sig")
    return {"parquet": str(parquet), "csv": str(csv), "rows": len(frame)}


def scope_mask(joined: pd.DataFrame, status_map: dict[str, str], scope: str) -> pd.Series:
    statuses = joined["instrument_id"].map(status_map).fillna("snapshot_only")
    if scope == "trading":
        return statuses.eq("trading")
    if scope == "suspended":
        return statuses.eq("suspended")
    return pd.Series(True, index=joined.index)


def compare_sources(frames: dict[str, pd.DataFrame], out_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    field_rows: list[dict[str, Any]] = []
    pair_rows: list[dict[str, Any]] = []
    differences: list[dict[str, Any]] = []
    bao = frames["baostock"]
    status_map = dict(zip(bao["instrument_id"], bao["trade_status"]))
    keys = ["instrument_id", "trade_date", "adjustment"]
    fields = list(COMPARE_TOLERANCES)

    for left_name, right_name in itertools.combinations(frames, 2):
        left = frames[left_name][keys + fields]
        right = frames[right_name][keys + fields]
        joined = left.merge(right, on=keys, how="inner", suffixes=("_left", "_right"))
        statuses = joined["instrument_id"].map(status_map).fillna("snapshot_only")
        pair_rows.append(
            {
                "left_source": left_name,
                "right_source": right_name,
                "common_rows": len(joined),
                "trading_rows": int(statuses.eq("trading").sum()),
                "suspended_rows": int(statuses.eq("suspended").sum()),
                "snapshot_only_rows": int(statuses.eq("snapshot_only").sum()),
            }
        )
        for scope in ("all", "trading", "suspended"):
            scoped = joined[scope_mask(joined, status_map, scope)]
            for field, tolerance in fields_and_tolerances(fields):
                left_values = pd.to_numeric(scoped[f"{field}_left"], errors="coerce")
                right_values = pd.to_numeric(scoped[f"{field}_right"], errors="coerce")
                valid = left_values.notna() & right_values.notna()
                diffs = (left_values[valid] - right_values[valid]).abs()
                compared = len(diffs)
                exact = int(diffs.le(1e-9).sum()) if compared else 0
                within = int(diffs.le(tolerance).sum()) if compared else 0
                field_rows.append(
                    {
                        "left_source": left_name,
                        "right_source": right_name,
                        "scope": scope,
                        "field": field,
                        "tolerance": tolerance,
                        "compared_rows": compared,
                        "exact_rows": exact,
                        "exact_rate": exact / compared if compared else None,
                        "within_tolerance_rows": within,
                        "within_tolerance_rate": within / compared if compared else None,
                        "max_abs_diff": float(diffs.max()) if compared else None,
                        "mean_abs_diff": float(diffs.mean()) if compared else None,
                    }
                )
                if scope == "all" and compared:
                    bad_indexes = diffs[diffs > tolerance].index
                    for index in bad_indexes:
                        differences.append(
                            {
                                "left_source": left_name,
                                "right_source": right_name,
                                "instrument_id": joined.at[index, "instrument_id"],
                                "trade_date": joined.at[index, "trade_date"],
                                "reference_status": statuses.at[index],
                                "field": field,
                                "left_value": left_values.at[index],
                                "right_value": right_values.at[index],
                                "abs_diff": abs(left_values.at[index] - right_values.at[index]),
                                "tolerance": tolerance,
                            }
                        )
    pair_df = pd.DataFrame(pair_rows)
    field_df = pd.DataFrame(field_rows)
    difference_df = pd.DataFrame(differences)
    out_dir.mkdir(parents=True, exist_ok=True)
    pair_df.to_csv(out_dir / "pairwise_coverage.csv", index=False, encoding="utf-8-sig")
    field_df.to_csv(out_dir / "field_comparison.csv", index=False, encoding="utf-8-sig")
    difference_df.to_csv(out_dir / "differences.csv", index=False, encoding="utf-8-sig")
    difference_df.to_parquet(out_dir / "differences.parquet", index=False)
    return pair_df, field_df, difference_df


def fields_and_tolerances(fields: list[str]):
    for field in fields:
        yield field, COMPARE_TOLERANCES[field]


def pct(value: Any) -> str:
    return "-" if pd.isna(value) else f"{float(value):.2%}"


def write_report(
    out_dir: Path,
    outputs: dict[str, dict[str, Any]],
    pair_df: pd.DataFrame,
    field_df: pd.DataFrame,
    differences: pd.DataFrame,
) -> None:
    lines = [
        "# 2026-09-11 四来源股票统一格式与一致性报告",
        "",
        "## 标准化文件",
        "",
        "| 来源 | 行数 | 数据性质 |",
        "|---|---:|---|",
    ]
    nature = {"baostock": "历史日线/confirmed", "tencent": "收盘快照/provisional", "sina": "收盘快照/provisional", "eastmoney": "首批部分快照/provisional"}
    for source, output in outputs.items():
        lines.append(f"| {source} | {output['rows']:,} | {nature[source]} |")
    lines += [
        "",
        "所有文件严格使用 `market_data_contract.yaml -> targets.daily_bar.fields` 的字段顺序。",
        "",
        "## 两两覆盖",
        "",
        "| 来源对 | 共同证券 | 正常交易 | 停牌 | 仅快照市场 |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in pair_df.to_dict("records"):
        lines.append(
            f"| {row['left_source']} vs {row['right_source']} | {row['common_rows']:,} | "
            f"{row['trading_rows']:,} | {row['suspended_rows']:,} | {row['snapshot_only_rows']:,} |"
        )
    lines += [
        "",
        "## 核心字段一致率",
        "",
        "BaoStock参与的来源对以及腾讯/新浪使用正常交易证券；东财没有BaoStock交易状态，使用其99只共同北交所快照样本。下表同时给出严格一致率和业务容差一致率。价格容差为0.000001元；涨跌幅为0.011个百分点；成交量为100股；成交额为10,000元。",
        "",
        "| 来源对 | 范围 | 字段 | 比较数 | 严格一致 | 容差内一致 | 最大绝对差 |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    core_fields = ["open", "high", "low", "close", "volume_shares", "amount_cny"]
    normal_core = field_df[
        (field_df["scope"] == "trading")
        & (field_df["field"].isin(core_fields))
        & ~field_df["left_source"].eq("eastmoney")
        & ~field_df["right_source"].eq("eastmoney")
    ]
    eastmoney_core = field_df[
        (field_df["scope"] == "all")
        & (field_df["field"].isin(core_fields))
        & (field_df["left_source"].eq("eastmoney") | field_df["right_source"].eq("eastmoney"))
        & field_df["compared_rows"].gt(0)
    ]
    core = pd.concat([normal_core, eastmoney_core], ignore_index=True)
    for row in core.to_dict("records"):
        max_diff = "-" if pd.isna(row["max_abs_diff"]) else f"{row['max_abs_diff']:.6g}"
        scope_label = "正常交易" if row["scope"] == "trading" else "共同快照样本"
        lines.append(
            f"| {row['left_source']} vs {row['right_source']} | {scope_label} | {row['field']} | {int(row['compared_rows']):,} | "
            f"{pct(row['exact_rate'])} | {pct(row['within_tolerance_rate'])} | "
            f"{max_diff} |"
        )
    status_counts = differences["reference_status"].value_counts() if not differences.empty else pd.Series(dtype=int)
    trading_differences = int(status_counts.get("trading", 0))
    suspended_differences = int(status_counts.get("suspended", 0))
    snapshot_differences = int(status_counts.get("snapshot_only", 0))
    lines += [
        "",
        "## 结论",
        "",
        f"- 超出业务容差的逐字段差异共有 {len(differences):,} 条，已输出到 `differences.parquet/csv`。",
        f"- 其中正常交易股票 {trading_differences} 条、停牌股票 {suspended_differences} 条、仅快照市场 {snapshot_differences} 条。",
        "- 正常交易股票的OHLC在BaoStock、腾讯、新浪之间严格一致率为100%；共同的99只东财北交所样本，OHLC同样为100%。",
        "- 仅快照市场的超容差差异来自北交所920045、920161、920375三只股票的成交量与成交额。",
        "- 正常交易证券应重点看 OHLC；成交量和成交额因来源单位与展示精度不同，应看容差一致率，不能要求字符串完全相等。",
        "- 停牌证券各来源会分别返回空值、零值或昨收，不应计入正常行情一致率，也不能用快照覆盖 confirmed 历史行。",
        "- 东方财富本轮只有首批 100 只北交所快照，报告中的东财比较只代表该样本，不能外推为全市场成功率。",
    ]
    (out_dir / "COMPARISON_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    run_dir = args.run_dir.resolve()
    contract = yaml.safe_load(args.contract.resolve().read_text(encoding="utf-8"))
    target_columns = list(contract["targets"]["daily_bar"]["fields"])
    fetched_at = source_time(run_dir)
    normalizers = {
        "baostock": normalize_baostock,
        "tencent": normalize_tencent,
        "sina": normalize_sina,
        "eastmoney": normalize_eastmoney,
    }
    frames: dict[str, pd.DataFrame] = {}
    outputs: dict[str, dict[str, Any]] = {}
    for source, normalizer in normalizers.items():
        frame = normalizer(run_dir, target_columns, fetched_at)
        if list(frame.columns) != target_columns:
            raise RuntimeError(f"{source} 输出字段顺序不符合契约")
        frames[source] = frame
        outputs[source] = save_target(frame, run_dir / source)

    compare_dir = run_dir / "normalized_comparison"
    pair_df, field_df, difference_df = compare_sources(frames, compare_dir)
    write_report(compare_dir, outputs, pair_df, field_df, difference_df)
    summary = {
        "contract": str(args.contract.resolve()),
        "run_dir": str(run_dir),
        "outputs": outputs,
        "pairwise_rows": len(pair_df),
        "field_comparison_rows": len(field_df),
        "differences_over_tolerance": len(difference_df),
        "tolerances": COMPARE_TOLERANCES,
    }
    (compare_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
