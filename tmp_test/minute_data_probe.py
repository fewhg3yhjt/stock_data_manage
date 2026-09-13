"""Safely validate minute-bar flows on a fixed market/instrument matrix."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests

from full_market_day_probe import request_session


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_ROOT = BASE_DIR / "minute_data_probe"
LOCK_PATH = OUTPUT_ROOT / ".minute_data_probe.lock"
SOURCE_INTERVALS = {"baostock": 0.5, "eastmoney": 3.0, "tencent": 0.5, "sina": 3.0}
SUPPORTED_PERIODS = {"1", "5", "15", "30", "60"}


@dataclass(frozen=True)
class MinuteCase:
    case_id: str
    source_symbol: str
    instrument_type: str
    board_group: str
    eastmoney_secid: str | None


CASES = [
    MinuteCase("sh_main", "sh600519", "equity", "main_sh", "1.600519"),
    MinuteCase("sz_main", "sz000001", "equity", "main_sz", "0.000001"),
    MinuteCase("chinext", "sz300750", "equity", "chinext", "0.300750"),
    MinuteCase("star", "sh688001", "equity", "star", "1.688001"),
    MinuteCase("bse", "bj920000", "equity", "bse", "0.920000"),
    MinuteCase("sh_etf", "sh510300", "etf", "etf_sh", "1.510300"),
    MinuteCase("sz_etf", "sz159919", "etf", "etf_sz", "0.159919"),
    MinuteCase("sh_lof", "sh501018", "lof", "lof_sh", "1.501018"),
    MinuteCase("sh_index", "sh000001", "index", "index", "1.000001"),
    MinuteCase("industry_board", "BK0475", "industry_board", "industry", "90.BK0475"),
    MinuteCase("concept_board", "BK1753", "concept_board", "concept", "90.BK1753"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", default="baostock,eastmoney,tencent,sina")
    parser.add_argument("--periods", default="1,5", help="逗号分隔: 1,5,15,30,60")
    parser.add_argument("--cases", help="逗号分隔 case_id；默认全部代表类型")
    parser.add_argument("--count", type=int, default=120, help="腾讯/新浪请求条数，默认 120")
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


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
            raise RuntimeError("已有分钟线探测任务运行，拒绝重复启动") from exc
        yield
    finally:
        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            handle.close()


def configure_logging(out_dir: Path) -> logging.Logger:
    logger = logging.getLogger("minute_data_probe")
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


def normalize_numeric(frame: pd.DataFrame, text_fields: set[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in result.columns:
        if column not in text_fields:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    return result


def frame_range(frame: pd.DataFrame) -> tuple[str | None, str | None]:
    if frame.empty:
        return None, None
    for column in ("datetime", "date", "time", "day"):
        if column in frame:
            values = frame[column].dropna().astype(str)
            if not values.empty:
                return values.iloc[0], values.iloc[-1]
    return None, None


def baostock_rows(result_set) -> list[dict]:
    fields = result_set.fields if isinstance(result_set.fields, list) else result_set.fields.split(",")
    rows: list[dict] = []
    while result_set.error_code == "0" and result_set.next():
        rows.append(dict(zip(fields, result_set.get_row_data())))
    return rows


def fetch_baostock(bs, case: MinuteCase, period: str) -> tuple[pd.DataFrame, dict]:
    if period == "1":
        raise NotImplementedError("BaoStock 不提供 1 分钟周期")
    if case.instrument_type in {"industry_board", "concept_board"}:
        raise NotImplementedError("BaoStock 不提供来源专属板块分钟线")
    if case.source_symbol.startswith("bj"):
        raise NotImplementedError("BaoStock 不支持北交所代码")
    end = date.today()
    start = end - timedelta(days=10)
    request_code = case.source_symbol[:2] + "." + case.source_symbol[2:]
    result = bs.query_history_k_data_plus(
        request_code,
        "date,time,code,open,high,low,close,volume,amount,adjustflag",
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        frequency=period,
        adjustflag="3",
    )
    if result.error_code != "0":
        raise RuntimeError(f"{result.error_code}: {result.error_msg}")
    records = baostock_rows(result)
    frame = pd.DataFrame(records, columns=result.fields)
    if not frame.empty:
        frame.insert(0, "datetime", frame["date"].astype(str) + " " + frame["time"].astype(str))
        frame = normalize_numeric(frame, {"datetime", "date", "time", "code"})
    return frame, {"code": request_code, "frequency": period, "start": start.isoformat(), "end": end.isoformat(), "adjustflag": "3"}


def fetch_eastmoney(http: requests.Session, case: MinuteCase, period: str) -> tuple[pd.DataFrame, dict]:
    if not case.eastmoney_secid:
        raise ValueError("缺少 Eastmoney secid")
    if period == "1":
        url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "ndays": "5" if not case.source_symbol.startswith("BK") else "1",
            "iscr": "0",
            "secid": case.eastmoney_secid,
        }
        response = http.get(url, params=params, timeout=(10, 30))
        response.raise_for_status()
        payload = response.json()
        items = ((payload.get("data") or {}).get("trends") or [])
        fields = ["datetime", "open", "close", "high", "low", "volume", "amount", "average_price"]
    else:
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": case.eastmoney_secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": period,
            "fqt": "0",
            "beg": "0",
            "end": "20500101",
            "lmt": "1000000",
        }
        response = http.get(url, params=params, timeout=(10, 30))
        response.raise_for_status()
        payload = response.json()
        items = ((payload.get("data") or {}).get("klines") or [])
        fields = ["datetime", "open", "close", "high", "low", "volume", "amount", "amplitude", "pct_chg", "chg", "turnover"]
    rows = [item.split(",") for item in items]
    frame = pd.DataFrame(rows, columns=fields) if rows else pd.DataFrame(columns=fields)
    frame = normalize_numeric(frame, {"datetime"})
    return frame, {"url": url, "request": params, "data_name": (payload.get("data") or {}).get("name")}


def fetch_tencent(http: requests.Session, case: MinuteCase, period: str, count: int) -> tuple[pd.DataFrame, dict]:
    if case.instrument_type in {"industry_board", "concept_board"}:
        raise NotImplementedError("腾讯接口不提供东方财富来源专属板块")
    params = {"param": f"{case.source_symbol},m{period},,{count}"}
    response = http.get("https://ifzq.gtimg.cn/appstock/app/kline/mkline", params=params, timeout=(10, 30))
    response.raise_for_status()
    payload = response.json()
    bars = (((payload.get("data") or {}).get(case.source_symbol) or {}).get(f"m{period}") or [])
    fields = ["datetime", "open", "close", "high", "low", "volume"]
    frame = pd.DataFrame([item[:6] for item in bars if len(item) >= 6], columns=fields)
    frame = normalize_numeric(frame, {"datetime"})
    return frame, {"request": params, "raw_array_widths": sorted({len(item) for item in bars}), "status": payload.get("code")}


def fetch_sina(http: requests.Session, case: MinuteCase, period: str, count: int) -> tuple[pd.DataFrame, dict]:
    if case.instrument_type in {"industry_board", "concept_board"}:
        raise NotImplementedError("新浪接口不提供东方财富来源专属板块")
    params = {"symbol": case.source_symbol, "scale": period, "ma": "no", "datalen": str(count)}
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
    if payload is None:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"]), {
            "request": params,
            "rows": 0,
            "returned_null": True,
        }
    frame = pd.DataFrame(payload)
    if not frame.empty:
        frame = frame.rename(columns={"day": "datetime"})
        frame = normalize_numeric(frame, {"datetime"})
    return frame, {"request": params, "rows": len(payload)}


def save_case(source_dir: Path, case: MinuteCase, period: str, frame: pd.DataFrame, raw: dict) -> None:
    case_dir = source_dir / case.case_id / f"m{period}"
    case_dir.mkdir(parents=True, exist_ok=True)
    write_json(case_dir / "raw.json", raw)
    frame.to_parquet(case_dir / "source_rows.parquet", index=False)


def run_source(
    source: str,
    cases: list[MinuteCase],
    periods: list[str],
    count: int,
    out_dir: Path,
    logger: logging.Logger,
) -> list[dict]:
    rows: list[dict] = []
    interval = SOURCE_INTERVALS[source]
    http = request_session() if source != "baostock" else None
    bs = None
    if source == "baostock":
        import baostock as baostock

        bs = baostock
        login = bs.login()
        if login.error_code != "0":
            raise ConnectionError(f"BaoStock 登录失败: {login.error_code} {login.error_msg}")

    fetchers: dict[str, Callable[[MinuteCase, str], tuple[pd.DataFrame, dict]]] = {
        "baostock": lambda case, period: fetch_baostock(bs, case, period),
        "eastmoney": lambda case, period: fetch_eastmoney(http, case, period),
        "tencent": lambda case, period: fetch_tencent(http, case, period, count),
        "sina": lambda case, period: fetch_sina(http, case, period, count),
    }
    connection_failures = 0
    try:
        combinations = [(case, period) for case in cases for period in periods]
        for index, (case, period) in enumerate(combinations):
            started = time.monotonic()
            frame: pd.DataFrame | None = None
            error = ""
            status = "ERROR"
            try:
                frame, raw = fetchers[source](case, period)
                save_case(out_dir / source, case, period, frame, raw)
                status = "OK" if not frame.empty else "EMPTY"
                connection_failures = 0
            except NotImplementedError as exc:
                status = "UNSUPPORTED"
                error = str(exc)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                lowered = error.lower()
                connection_failures = connection_failures + 1 if any(item in lowered for item in ("connection", "timeout", "remote", "返回 html")) else 0
            first, last = frame_range(frame if frame is not None else pd.DataFrame())
            result = {
                "source": source,
                "case_id": case.case_id,
                "source_symbol": case.source_symbol,
                "instrument_type": case.instrument_type,
                "board_group": case.board_group,
                "period_minutes": int(period),
                "status": status,
                "rows": len(frame) if frame is not None else 0,
                "first": first,
                "last": last,
                "columns": "|".join(map(str, frame.columns)) if frame is not None else "",
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "error": error,
            }
            rows.append(result)
            pd.DataFrame(rows).to_csv(out_dir / "summary.csv", index=False, encoding="utf-8-sig")
            logger.info("%s/%s/m%s %s rows=%d", source, case.case_id, period, status, result["rows"])
            if connection_failures >= 5:
                logger.warning("%s 连续 5 次连接失败，打开熔断", source)
                break
            spent = time.monotonic() - started
            if index < len(combinations) - 1 and status != "UNSUPPORTED" and spent < interval:
                time.sleep(interval - spent)
    finally:
        if bs is not None:
            bs.logout()
    return rows


def main() -> int:
    args = parse_args()
    sources = [item.strip().lower() for item in args.sources.split(",") if item.strip()]
    periods = [item.strip() for item in args.periods.split(",") if item.strip()]
    unknown_sources = set(sources) - set(SOURCE_INTERVALS)
    unknown_periods = set(periods) - SUPPORTED_PERIODS
    if unknown_sources or unknown_periods:
        raise ValueError(f"未知来源={sorted(unknown_sources)}，未知周期={sorted(unknown_periods)}")
    selected_ids = set(args.cases.split(",")) if args.cases else None
    cases = [case for case in CASES if selected_ids is None or case.case_id in selected_ids]
    if selected_ids and selected_ids - {case.case_id for case in CASES}:
        raise ValueError(f"未知 case_id: {sorted(selected_ids - {case.case_id for case in CASES})}")

    stamp = datetime.now().strftime("run_%Y%m%d_%H%M%S")
    out_dir = args.output_dir.resolve() if args.output_dir else OUTPUT_ROOT / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    logger = configure_logging(out_dir)
    write_json(
        out_dir / "run_config.json",
        {
            "started_at": datetime.now().astimezone().isoformat(),
            "sources": sources,
            "periods": periods,
            "count": args.count,
            "cases": [asdict(case) for case in cases],
            "source_intervals": SOURCE_INTERVALS,
        },
    )
    summary: list[dict] = []
    with single_instance_lock(LOCK_PATH):
        for source in sources:
            logger.info("开始 %s 分钟线，最小间隔 %.1f 秒", source, SOURCE_INTERVALS[source])
            try:
                summary.extend(run_source(source, cases, periods, args.count, out_dir, logger))
            except Exception as exc:
                logger.error("%s 来源初始化失败: %s: %s", source, type(exc).__name__, exc)
                summary.append({"source": source, "case_id": "__source__", "status": "ERROR", "error": f"{type(exc).__name__}: {exc}"})
            pd.DataFrame(summary).to_csv(out_dir / "summary.csv", index=False, encoding="utf-8-sig")
    write_json(out_dir / "completed.json", {"completed_at": datetime.now().astimezone().isoformat(), "results": summary})
    (OUTPUT_ROOT / "LATEST.txt").write_text(str(out_dir), encoding="utf-8")
    print(json.dumps({"output_dir": str(out_dir), "results": len(summary)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
