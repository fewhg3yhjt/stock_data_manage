"""Full-universe ETF daily snapshot and resumable historical-date probe.

The ETF master is produced by ``etf_universe_probe.py``.  Snapshot mode uses
bulk endpoints.  History mode deliberately runs one symbol at a time, keeps a
checkpoint after every symbol, and enforces the per-source minimum intervals.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests

from full_market_day_probe import fetch_tencent, request_session
from etf_universe_probe import EASTMONEY_LOF_FS, fetch_eastmoney_snapshot


BASE_DIR = Path(__file__).resolve().parent
ETF_ROOT = BASE_DIR / "etf_full_probe"
OUTPUT_ROOT = BASE_DIR / "etf_daily_probe"
LOCK_PATH = OUTPUT_ROOT / ".etf_daily_probe.lock"
SOURCE_INTERVALS = {"baostock": 0.5, "eastmoney": 3.0, "tencent": 0.5, "sina": 3.0}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("snapshot", "history", "all"), default="snapshot")
    parser.add_argument("--sources", default="baostock,eastmoney,tencent,sina")
    parser.add_argument("--master", type=Path, help="ETF 主清单 parquet/csv；默认读取 etf_full_probe/LATEST.txt")
    parser.add_argument("--include-lof", action="store_true", help="除 ETF 外也处理 LOF")
    parser.add_argument("--only-lof", action="store_true", help="仅处理 LOF，不重复抓取 ETF")
    parser.add_argument("--codes", help="逗号分隔的精确来源代码，例如 sh510300,sz159919")
    parser.add_argument("--start", help="历史起日 YYYY-MM-DD；默认最近工作日")
    parser.add_argument("--end", help="历史止日 YYYY-MM-DD；默认等于起日")
    parser.add_argument("--history-limit", type=int, default=0, help="每来源最多跑多少只；0=全部")
    parser.add_argument(
        "--sina-history-mode",
        choices=("fund-full", "general-1023"),
        default="fund-full",
        help="新浪 ETF 历史接口；默认专用全历史接口",
    )
    parser.add_argument("--output-dir", type=Path, help="指定/复用输出目录，支持断点续跑")
    return parser.parse_args()


def previous_workday() -> str:
    value = date.today()
    while value.weekday() >= 5:
        value -= timedelta(days=1)
    return value.isoformat()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def save_frame(frame: pd.DataFrame, stem: Path) -> dict[str, str]:
    stem.parent.mkdir(parents=True, exist_ok=True)
    csv_path = stem.with_suffix(".csv")
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    outputs = {"csv": str(csv_path)}
    try:
        parquet_path = stem.with_suffix(".parquet")
        frame.to_parquet(parquet_path, index=False)
        outputs["parquet"] = str(parquet_path)
    except Exception as exc:
        outputs["parquet_error"] = f"{type(exc).__name__}: {exc}"
    return outputs


@contextmanager
def single_instance_lock(path: Path):
    import msvcrt

    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    try:
        handle.seek(0)
        if handle.read(1) == b"":
            handle.seek(0)
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise RuntimeError("已有 ETF 日线任务运行，拒绝重复启动") from exc
        yield
    finally:
        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            handle.close()


def configure_logging(out_dir: Path) -> logging.Logger:
    logger = logging.getLogger("etf_daily_probe")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    file_handler = logging.FileHandler(out_dir / "run.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger


def resolve_master(path: Path | None) -> tuple[Path, pd.DataFrame]:
    if path is None:
        latest_file = ETF_ROOT / "LATEST.txt"
        if not latest_file.exists():
            raise FileNotFoundError("尚无 ETF 主清单，请先运行 etf_universe_probe.py")
        latest = Path(latest_file.read_text(encoding="utf-8").strip())
        path = latest / "instrument_master.parquet"
    path = path.resolve()
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path, dtype={"code": str})
    else:
        frame = pd.read_parquet(path)
    required = {"code", "name", "instrument_type", "market"}
    if not required.issubset(frame.columns):
        raise ValueError(f"ETF 主清单缺列: {sorted(required - set(frame.columns))}")
    return path, frame


def snapshot_existing(
    master_path: Path,
    source: str,
    out_dir: Path,
    instrument_types: set[str],
) -> dict[str, Any]:
    universe_run = master_path.parent
    candidates: dict[str, list[Path]] = {
        "eastmoney": [universe_run / "eastmoney_etf" / "snapshot_raw.csv"],
        "sina": [
            *([universe_run / "sina_etf" / "snapshot_raw.csv"] if "etf" in instrument_types else []),
            *([universe_run / "sina_lof" / "snapshot_raw.csv"] if "lof" in instrument_types else []),
        ],
    }
    source_paths = candidates[source]
    missing = [path for path in source_paths if not path.exists()]
    if missing:
        return {"status": "MISSING", "rows": 0, "reason": f"未找到 {missing}"}
    frame = pd.concat([pd.read_csv(path, low_memory=False) for path in source_paths], ignore_index=True)
    outputs = save_frame(frame, out_dir / source / "snapshot")
    return {
        "status": "OK",
        "rows": len(frame),
        "reused_from": [str(path) for path in source_paths],
        "outputs": outputs,
    }


def baostock_records(result_set) -> list[dict[str, Any]]:
    fields = result_set.fields if isinstance(result_set.fields, list) else result_set.fields.split(",")
    rows: list[dict[str, Any]] = []
    while result_set.error_code == "0" and result_set.next():
        rows.append(dict(zip(fields, result_set.get_row_data())))
    return rows


def fetch_history_baostock(bs, symbol: str, start: str, end: str) -> tuple[list[dict], dict]:
    request_code = symbol[:2] + "." + symbol[2:]
    result = bs.query_history_k_data_plus(
        request_code,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
        start_date=start,
        end_date=end,
        frequency="d",
        adjustflag="3",
    )
    if result.error_code != "0":
        raise RuntimeError(f"{result.error_code}: {result.error_msg}")
    return baostock_records(result), {"request_code": request_code, "adjustflag": "3"}


def fetch_history_eastmoney(http: requests.Session, symbol: str, start: str, end: str) -> tuple[list[dict], dict]:
    secid = ("1." if symbol.startswith("sh") else "0.") + symbol[2:]
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "0",
        "beg": start.replace("-", ""),
        "end": end.replace("-", ""),
        "lmt": "1000000",
    }
    response = http.get("https://push2his.eastmoney.com/api/qt/stock/kline/get", params=params, timeout=(10, 30))
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data") or {}
    fields = ["date", "open", "close", "high", "low", "volume", "amount", "amplitude", "pct_chg", "chg", "turnover"]
    records = [dict(zip(fields, item.split(","))) for item in (data.get("klines") or [])]
    return records, {"request": params, "source_name": data.get("name")}


def fetch_history_tencent(http: requests.Session, symbol: str, start: str, end: str) -> tuple[list[dict], dict]:
    params = {"param": f"{symbol},day,,,1024"}
    response = http.get("https://web.ifzq.gtimg.cn/appstock/app/kline/kline", params=params, timeout=(10, 30))
    response.raise_for_status()
    payload = response.json()
    bars = (((payload.get("data") or {}).get(symbol) or {}).get("day") or [])
    fields = ["date", "open", "close", "high", "low", "volume"]
    records = [dict(zip(fields, item[:6])) for item in bars if len(item) >= 6 and start <= str(item[0]) <= end]
    return records, {"request": params, "returned_bars": len(bars), "date_filter": [start, end]}


def fetch_history_sina(http: requests.Session, symbol: str, start: str, end: str) -> tuple[list[dict], dict]:
    params = {"symbol": symbol, "scale": "240", "ma": "no", "datalen": "1023"}
    response = http.get(
        "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData",
        params=params,
        timeout=(10, 30),
    )
    response.raise_for_status()
    body = response.text.strip()
    if not body or body.startswith("<"):
        raise ValueError("新浪返回 HTML 或空内容")
    payload = response.json()
    records = []
    for item in payload:
        day = str(item.get("day", ""))[:10]
        if start <= day <= end:
            records.append({"date": day, **{key: value for key, value in item.items() if key != "day"}})
    return records, {"request": params, "returned_bars": len(payload), "date_filter": [start, end]}


def fetch_history_sina_fund_full(http: requests.Session, symbol: str, start: str, end: str) -> tuple[list[dict], dict]:
    import py_mini_racer
    from akshare.stock.cons import hk_js_decode

    url = f"https://finance.sina.com.cn/realstock/company/{symbol}/hisdata_klc2/klc_kl.js"
    response = http.get(url, timeout=(10, 30))
    response.raise_for_status()
    text = response.text
    if "=" not in text or ";" not in text:
        raise ValueError("新浪 ETF 全历史接口返回格式异常")
    encrypted = text.split("=", 1)[1].split(";", 1)[0].replace('"', "")
    decoder = py_mini_racer.MiniRacer()
    decoder.eval(hk_js_decode)
    decoded = decoder.call("d", encrypted) or []
    all_records = pd.DataFrame(decoded)
    if all_records.empty:
        return [], {"url": url, "returned_bars": 0, "date_filter": [start, end], "history_mode": "fund-full"}
    all_records["date"] = pd.to_datetime(all_records["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    selected = all_records[(all_records["date"] >= start) & (all_records["date"] <= end)].copy()
    return selected.to_dict("records"), {
        "url": url,
        "returned_bars": len(all_records),
        "available_start": str(all_records["date"].min()),
        "available_end": str(all_records["date"].max()),
        "date_filter": [start, end],
        "history_mode": "fund-full",
    }


def load_checkpoint(path: Path) -> tuple[list[dict], set[str]]:
    if not path.exists():
        return [], set()
    entries: list[dict] = []
    completed: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        entries.append(item)
        if item.get("status") in {"OK", "EMPTY"}:
            completed.add(str(item.get("code")))
    return entries, completed


def history_provider(
    source: str,
    universe: pd.DataFrame,
    out_dir: Path,
    start: str,
    end: str,
    limit: int,
    sina_history_mode: str,
    logger: logging.Logger,
) -> dict[str, Any]:
    source_dir = out_dir / source
    source_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = source_dir / "history_checkpoint.jsonl"
    _, completed = load_checkpoint(checkpoint)
    selected = universe.head(limit) if limit > 0 else universe
    selected = selected.reset_index(drop=True)
    interval = SOURCE_INTERVALS[source]
    http = request_session() if source != "baostock" else None
    bs = None
    if source == "baostock":
        import baostock as baostock

        bs = baostock
        login = bs.login()
        if login.error_code != "0":
            raise ConnectionError(f"BaoStock 登录失败: {login.error_code} {login.error_msg}")

    fetchers: dict[str, Callable[[str], tuple[list[dict], dict]]] = {
        "baostock": lambda symbol: fetch_history_baostock(bs, symbol, start, end),
        "eastmoney": lambda symbol: fetch_history_eastmoney(http, symbol, start, end),
        "tencent": lambda symbol: fetch_history_tencent(http, symbol, start, end),
        "sina": lambda symbol: (
            fetch_history_sina_fund_full(http, symbol, start, end)
            if sina_history_mode == "fund-full"
            else fetch_history_sina(http, symbol, start, end)
        ),
    }
    attempted = succeeded = empty = errors = 0
    consecutive_connection_errors = 0
    try:
        with checkpoint.open("a", encoding="utf-8", buffering=1) as handle:
            for position, row in selected.iterrows():
                symbol = str(row["code"])
                if symbol in completed:
                    continue
                request_started = time.monotonic()
                attempted += 1
                entry: dict[str, Any] = {
                    "code": symbol,
                    "name": str(row["name"]),
                    "instrument_type": str(row["instrument_type"]),
                    "source": source,
                }
                try:
                    records, metadata = fetchers[source](symbol)
                    data_file = ""
                    if records:
                        symbol_frame = pd.DataFrame(records)
                        symbol_frame.insert(0, "source", source)
                        symbol_frame.insert(0, "instrument_type", str(row["instrument_type"]))
                        symbol_frame.insert(0, "name", str(row["name"]))
                        symbol_frame.insert(0, "code", symbol)
                        data_path = source_dir / "symbols" / f"{symbol}.parquet"
                        data_path.parent.mkdir(parents=True, exist_ok=True)
                        symbol_frame.to_parquet(data_path, index=False)
                        data_file = str(data_path.relative_to(source_dir))
                    entry.update(
                        {
                            "status": "OK" if records else "EMPTY",
                            "row_count": len(records),
                            "data_file": data_file,
                            "metadata": metadata,
                        }
                    )
                    if records:
                        succeeded += 1
                    else:
                        empty += 1
                    consecutive_connection_errors = 0
                except Exception as exc:
                    message = f"{type(exc).__name__}: {exc}"
                    entry.update({"status": "ERROR", "error": message})
                    errors += 1
                    lowered = message.lower()
                    if any(marker in lowered for marker in ("connection", "timeout", "remote", "返回 html")):
                        consecutive_connection_errors += 1
                    else:
                        consecutive_connection_errors = 0
                handle.write(json.dumps(entry, ensure_ascii=False) + "\n")

                done = position + 1
                if done % 100 == 0 or done == len(selected):
                    logger.info("%s 历史进度 %d/%d，成功 %d，空 %d，错误 %d", source, done, len(selected), succeeded, empty, errors)
                if consecutive_connection_errors >= 5:
                    logger.warning("%s 连续 5 次连接失败，触发熔断", source)
                    break
                spent = time.monotonic() - request_started
                if done < len(selected) and spent < interval:
                    time.sleep(interval - spent)
    finally:
        if bs is not None:
            bs.logout()

    entries, completed = load_checkpoint(checkpoint)
    status_rows: list[dict] = []
    daily_row_count = 0
    for item in entries:
        # Backward compatibility: migrate checkpoints produced by the first
        # small-sample version without holding the whole market in memory.
        legacy_records = item.get("records") or []
        if legacy_records and not item.get("data_file"):
            symbol = str(item.get("code"))
            legacy_frame = pd.DataFrame(
                [
                    {
                        "code": symbol,
                        "name": item.get("name"),
                        "instrument_type": item.get("instrument_type"),
                        "source": source,
                        **record,
                    }
                    for record in legacy_records
                ]
            )
            data_path = source_dir / "symbols" / f"{symbol}.parquet"
            data_path.parent.mkdir(parents=True, exist_ok=True)
            legacy_frame.to_parquet(data_path, index=False)
            item["data_file"] = str(data_path.relative_to(source_dir))
            item["row_count"] = len(legacy_frame)
        daily_row_count += int(item.get("row_count") or len(legacy_records))
        status_rows.append({key: value for key, value in item.items() if key not in {"records", "metadata"}})
    outputs = {
        "status": save_frame(pd.DataFrame(status_rows), source_dir / "history_status"),
        "partition_dir": str(source_dir / "symbols"),
    }
    return {
        "status": "PARTIAL" if len(completed) < len(selected) else "OK",
        "requested": len(selected),
        "completed_symbols": len(completed),
        "daily_rows": daily_row_count,
        "start": start,
        "end": end,
        "sina_history_mode": sina_history_mode if source == "sina" else None,
        "minimum_interval_seconds": interval,
        "outputs": outputs,
    }


def main() -> int:
    args = parse_args()
    selected_sources = [item.strip().lower() for item in args.sources.split(",") if item.strip()]
    unknown = set(selected_sources) - set(SOURCE_INTERVALS)
    if unknown:
        raise ValueError(f"不支持的数据源: {sorted(unknown)}")
    master_path, master = resolve_master(args.master)
    if args.only_lof and args.include_lof:
        raise ValueError("--only-lof 与 --include-lof 不能同时使用")
    allowed_types = {"lof"} if args.only_lof else ({"etf", "lof"} if args.include_lof else {"etf"})
    universe = master[master["instrument_type"].isin(allowed_types)].copy().sort_values("code").reset_index(drop=True)
    if args.codes:
        requested_codes = {item.strip().lower() for item in args.codes.split(",") if item.strip()}
        universe = universe[universe["code"].isin(requested_codes)].copy().reset_index(drop=True)
        missing_codes = requested_codes - set(universe["code"])
        if missing_codes:
            raise ValueError(f"ETF 主清单中没有这些代码: {sorted(missing_codes)}")
    start = args.start or previous_workday()
    end = args.end or start
    if end < start:
        raise ValueError("历史止日不能早于起日")

    stamp = datetime.now().strftime("run_%Y%m%d_%H%M%S")
    out_dir = args.output_dir.resolve() if args.output_dir else OUTPUT_ROOT / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    logger = configure_logging(out_dir)
    summary: dict[str, Any] = {
        "started_at": datetime.now().astimezone().isoformat(),
        "phase": args.phase,
        "master": str(master_path),
        "instrument_types": sorted(allowed_types),
        "universe_rows": len(universe),
        "sources": {},
        "intervals": SOURCE_INTERVALS,
        "start": start,
        "end": end,
    }
    write_json(out_dir / "run_config.json", summary)

    with single_instance_lock(LOCK_PATH):
        if args.phase in {"snapshot", "all"}:
            for source in selected_sources:
                if source == "tencent":
                    started = time.monotonic()
                    try:
                        result = fetch_tencent(universe, out_dir / "tencent", logger, batch_interval=3.0)
                        summary["sources"]["tencent_snapshot"] = {"status": "OK", "elapsed_seconds": round(time.monotonic() - started, 3), **result}
                    except Exception as exc:
                        summary["sources"]["tencent_snapshot"] = {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}
                elif source == "eastmoney" and allowed_types == {"lof"}:
                    raw, meta = fetch_eastmoney_snapshot(
                        out_dir / "eastmoney",
                        fs=EASTMONEY_LOF_FS,
                    )
                    outputs = save_frame(raw, out_dir / "eastmoney" / "snapshot")
                    summary["sources"]["eastmoney_snapshot"] = {**meta, "outputs": outputs}
                elif source in {"eastmoney", "sina"}:
                    summary["sources"][f"{source}_snapshot"] = snapshot_existing(
                        master_path,
                        source,
                        out_dir,
                        allowed_types,
                    )
                else:
                    summary["sources"]["baostock_snapshot"] = {"status": "UNSUPPORTED", "rows": 0, "reason": "BaoStock 无 ETF 全市场实时快照接口；使用历史日线接口补齐"}
                write_json(out_dir / "summary.json", summary)

        if args.phase in {"history", "all"}:
            for source in selected_sources:
                started = time.monotonic()
                try:
                    result = history_provider(
                        source,
                        universe,
                        out_dir,
                        start,
                        end,
                        args.history_limit,
                        args.sina_history_mode,
                        logger,
                    )
                    summary["sources"][f"{source}_history"] = {"elapsed_seconds": round(time.monotonic() - started, 3), **result}
                except Exception as exc:
                    summary["sources"][f"{source}_history"] = {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}
                write_json(out_dir / "summary.json", summary)

    summary["completed_at"] = datetime.now().astimezone().isoformat()
    write_json(out_dir / "summary.json", summary)
    (OUTPUT_ROOT / "LATEST.txt").write_text(str(out_dir), encoding="utf-8")
    print(json.dumps({"output_dir": str(out_dir), **summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
