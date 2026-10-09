from __future__ import annotations

"""Bounded historical-daily capability probe for Chinese market sources.

The probe deliberately uses a small representative instrument matrix.  It is
not a bulk downloader.  Each provider is paced independently and every raw
response is stored under that provider's directory for later schema work.
"""

import argparse
import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable

import pandas as pd
import requests
import yaml


BASE_DIR = Path(__file__).resolve().parent
CONTRACT_PATH = BASE_DIR / "market_data_contract.yaml"
OUTPUT_ROOT = BASE_DIR / "historical_daily_probe"
LOCK_PATH = OUTPUT_ROOT / ".historical_daily_probe.lock"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 Chrome/152 Safari/537.36"
)


@dataclass(frozen=True)
class ProbeCase:
    case_id: str
    instrument_id: str
    instrument_type: str
    source_symbol: str
    eastmoney_secid: str | None = None


CASES = [
    ProbeCase("sh_main", "CN.XSHG.EQUITY.600519", "equity", "sh600519", "1.600519"),
    ProbeCase("sz_main", "CN.XSHE.EQUITY.000001", "equity", "sz000001", "0.000001"),
    ProbeCase("chinext", "CN.XSHE.EQUITY.300750", "equity", "sz300750", "0.300750"),
    ProbeCase("star", "CN.XSHG.EQUITY.688001", "equity", "sh688001", "1.688001"),
    ProbeCase("new_302", "CN.XSHE.EQUITY.302132", "equity", "sz302132", "0.302132"),
    ProbeCase("bse", "CN.BSE.EQUITY.920000", "equity", "bj920000", "0.920000"),
    ProbeCase("suspended", "CN.XSHE.EQUITY.000016", "equity", "sz000016", "0.000016"),
    ProbeCase("sh_etf", "CN.XSHG.ETF.510300", "etf", "sh510300", "1.510300"),
    ProbeCase("sz_etf", "CN.XSHE.ETF.159919", "etf", "sz159919", "0.159919"),
    ProbeCase("sh_index", "CN.XSHG.INDEX.000001", "index", "sh000001", "1.000001"),
    ProbeCase(
        "em_industry_board",
        "CN.EASTMONEY.INDUSTRY_BOARD.BK0475",
        "industry_board",
        "BK0475",
        "90.BK0475",
    ),
    ProbeCase(
        "em_concept_board",
        "CN.EASTMONEY.CONCEPT_BOARD.BK1753",
        "concept_board",
        "BK1753",
        "90.BK1753",
    ),
]

SOURCE_CONTRACT_IDS = {
    "baostock": "baostock_history_daily",
    "eastmoney": "eastmoney_history_daily",
    "tencent": "tencent_history_daily",
    "sina": "sina_history_daily",
}

SOURCE_INTERVALS = {
    "baostock": 0.5,
    "eastmoney": 3.0,
    "tencent": 0.5,
    "sina": 3.0,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="历史日线来源能力安全探测")
    parser.add_argument(
        "--sources",
        default="baostock,eastmoney,tencent,sina",
        help="逗号分隔来源: baostock,eastmoney,tencent,sina",
    )
    parser.add_argument("--start", default="1990-01-01", help="BaoStock 请求起日")
    parser.add_argument("--end", default=date.today().isoformat(), help="BaoStock 请求止日")
    parser.add_argument(
        "--cases",
        help="逗号分隔 case_id；默认运行全部代表标的",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="指定输出目录；默认创建带时间戳的目录",
    )
    return parser.parse_args()


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
            raise RuntimeError("已有历史日线能力探测任务运行，拒绝重复启动") from exc
        yield
    finally:
        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            handle.close()


def configure_logging(output_dir: Path) -> logging.Logger:
    logger = logging.getLogger("historical_daily_probe")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    file_handler = logging.FileHandler(output_dir / "run.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger


def session() -> requests.Session:
    value = requests.Session()
    value.trust_env = False
    value.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json,text/plain,*/*"})
    return value


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_summary(output_dir: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(output_dir / "summary.csv", index=False, encoding="utf-8-sig")


def numeric_frame(frame: pd.DataFrame, exclude: set[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in result.columns:
        if column not in exclude:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    return result


def time_range(frame: pd.DataFrame) -> tuple[str | None, str | None]:
    for column in ("date", "datetime", "day"):
        if column in frame and not frame.empty:
            values = frame[column].dropna().astype(str)
            if not values.empty:
                return values.iloc[0], values.iloc[-1]
    return None, None


def contract_check(contract: dict, source: str, frame: pd.DataFrame) -> dict:
    source_id = SOURCE_CONTRACT_IDS[source]
    expected = set(contract["sources"][source_id]["raw_fields"])
    actual = set(map(str, frame.columns))
    # Some providers do not repeat request-context fields in every returned row.
    context_fields = {"code"} if source == "baostock" else set()
    missing = sorted(expected - actual - context_fields)
    extra = sorted(actual - expected)
    return {
        "contract_source_id": source_id,
        "missing_contract_fields": "|".join(missing),
        "extra_source_fields": "|".join(extra),
    }


def baostock_rows(result_set) -> list[dict]:
    fields = result_set.fields if isinstance(result_set.fields, list) else result_set.fields.split(",")
    rows: list[dict] = []
    while result_set.error_code == "0" and result_set.next():
        rows.append(dict(zip(fields, result_set.get_row_data())))
    return rows


def fetch_baostock(bs, case: ProbeCase, start: str, end: str) -> tuple[pd.DataFrame, dict]:
    if case.instrument_type in {"industry_board", "concept_board"}:
        raise NotImplementedError("BaoStock 不提供东方财富来源专属板块")
    code = case.source_symbol[:2] + "." + case.source_symbol[2:]
    result = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,"
        "tradestatus,pctChg,peTTM,pbMRQ,isST",
        start_date=start,
        end_date=end,
        frequency="d",
        adjustflag="3",
    )
    if result.error_code != "0":
        raise RuntimeError(f"{result.error_code}: {result.error_msg}")
    records = baostock_rows(result)
    frame = pd.DataFrame(records, columns=result.fields)
    frame = numeric_frame(frame, {"date", "code"}) if not frame.empty else frame
    return frame, {"request_code": code, "records": records, "adjustflag": "3"}


def fetch_eastmoney(http: requests.Session, case: ProbeCase) -> tuple[pd.DataFrame, dict]:
    if not case.eastmoney_secid:
        raise ValueError("缺少 Eastmoney secid")
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    fields = [
        "datetime",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "amplitude",
        "pct_chg",
        "chg",
        "turnover",
    ]
    params = {
        "secid": case.eastmoney_secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "0",
        "beg": "19900101",
        "end": "20500101",
        "smplmt": "10000",
        "lmt": "1000000",
    }
    response = http.get(url, params=params, timeout=(10, 30))
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data") or {}
    rows = [item.split(",") for item in (data.get("klines") or [])]
    frame = pd.DataFrame(rows, columns=fields)
    frame = numeric_frame(frame, {"datetime"}) if not frame.empty else frame
    return frame, {"request": params, "payload": payload}


def fetch_tencent(http: requests.Session, case: ProbeCase) -> tuple[pd.DataFrame, dict]:
    if case.instrument_type in {"industry_board", "concept_board"}:
        raise NotImplementedError("腾讯探测接口不提供东方财富来源专属板块")
    url = "https://web.ifzq.gtimg.cn/appstock/app/kline/kline"
    # The non-adjusted endpoint accepts 1024 and returns up to 1024 bars.
    # The qfq endpoint is a separate capability and was observed to cap
    # ordinary stock responses at 640 bars.
    params = {"param": f"{case.source_symbol},day,,,1024"}
    response = http.get(url, params=params, timeout=(10, 30))
    response.raise_for_status()
    payload = response.json()
    stock = ((payload.get("data") or {}).get(case.source_symbol) or {})
    adjustment_key = "day"
    bars = stock.get(adjustment_key) or []
    fields = ["date", "open", "close", "high", "low", "volume"]
    rows = [item[:6] for item in bars if len(item) >= 6]
    frame = pd.DataFrame(rows, columns=fields)
    frame = numeric_frame(frame, {"date"}) if not frame.empty else frame
    return frame, {
        "request": params,
        "adjustment_key": adjustment_key,
        "raw_array_widths": sorted({len(item) for item in bars}),
        "payload": payload,
    }


def fetch_sina(http: requests.Session, case: ProbeCase) -> tuple[pd.DataFrame, dict]:
    if case.instrument_type in {"industry_board", "concept_board"}:
        raise NotImplementedError("新浪探测接口不提供东方财富来源专属板块")
    url = (
        "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
        "CN_MarketData.getKLineData"
    )
    params = {"symbol": case.source_symbol, "scale": "240", "ma": "no", "datalen": "1023"}
    response = http.get(url, params=params, timeout=(10, 30))
    response.raise_for_status()
    body = response.text.strip()
    if not body or body.startswith("<"):
        raise ValueError("新浪返回 HTML 或空内容")
    payload = json.loads(body)
    frame = pd.DataFrame(payload)
    frame = numeric_frame(frame, {"day"}) if not frame.empty else frame
    return frame, {"request": params, "payload": payload}


def save_case(source_dir: Path, case: ProbeCase, frame: pd.DataFrame, raw: dict) -> None:
    case_dir = source_dir / case.case_id
    case_dir.mkdir(parents=True, exist_ok=True)
    write_json(case_dir / "raw.json", raw)
    frame.to_parquet(case_dir / "source_rows.parquet", index=False)


def result_row(
    source: str,
    case: ProbeCase,
    status: str,
    elapsed: float,
    frame: pd.DataFrame | None,
    error: str,
    contract: dict,
) -> dict:
    frame = frame if frame is not None else pd.DataFrame()
    start, end = time_range(frame)
    check = contract_check(contract, source, frame) if status == "OK" else {
        "contract_source_id": SOURCE_CONTRACT_IDS[source],
        "missing_contract_fields": "",
        "extra_source_fields": "",
    }
    return {
        "source": source,
        "case_id": case.case_id,
        "instrument_id": case.instrument_id,
        "instrument_type": case.instrument_type,
        "status": status,
        "elapsed_sec": round(elapsed, 3),
        "rows": len(frame),
        "start": start,
        "end": end,
        "columns": "|".join(map(str, frame.columns)),
        "error": error,
        **check,
    }


def run_provider(
    source: str,
    cases: list[ProbeCase],
    output_dir: Path,
    contract: dict,
    summary: list[dict],
    fetch: Callable[[ProbeCase], tuple[pd.DataFrame, dict]],
    logger: logging.Logger,
) -> None:
    source_dir = output_dir / source
    source_dir.mkdir(parents=True, exist_ok=True)
    interval = SOURCE_INTERVALS[source]
    connection_rejected = False

    for index, case in enumerate(cases):
        if connection_rejected:
            row = result_row(
                source,
                case,
                "SKIP_CIRCUIT_OPEN",
                0.0,
                None,
                "同一来源此前发生连接级拒绝，停止后续请求",
                contract,
            )
            summary.append(row)
            write_summary(output_dir, summary)
            continue

        started = time.perf_counter()
        try:
            frame, raw = fetch(case)
            save_case(source_dir, case, frame, raw)
            row = result_row(
                source,
                case,
                "OK" if not frame.empty else "EMPTY",
                time.perf_counter() - started,
                frame,
                "",
                contract,
            )
            logger.info(
                "%s/%s %s，%d 行，%s -> %s",
                source,
                case.case_id,
                row["status"],
                row["rows"],
                row["start"],
                row["end"],
            )
        except NotImplementedError as exc:
            row = result_row(
                source,
                case,
                "UNSUPPORTED",
                time.perf_counter() - started,
                None,
                str(exc),
                contract,
            )
            logger.info("%s/%s 不支持: %s", source, case.case_id, exc)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            row = result_row(
                source,
                case,
                "ERROR",
                time.perf_counter() - started,
                None,
                error,
                contract,
            )
            logger.warning("%s/%s 失败: %s", source, case.case_id, error)
            message = str(exc).lower()
            connection_rejected = any(
                marker in message
                for marker in (
                    "connection aborted",
                    "remotedisconnected",
                    "connection reset",
                    "返回 html",
                    "proxyerror",
                )
            )
        summary.append(row)
        write_summary(output_dir, summary)
        if index < len(cases) - 1 and row["status"] != "UNSUPPORTED":
            time.sleep(interval)


def main() -> int:
    args = parse_args()
    selected_sources = [item.strip() for item in args.sources.split(",") if item.strip()]
    unknown = set(selected_sources) - set(SOURCE_CONTRACT_IDS)
    if unknown:
        raise ValueError(f"不支持的数据源: {sorted(unknown)}")

    selected_case_ids = set(args.cases.split(",")) if args.cases else None
    cases = [case for case in CASES if selected_case_ids is None or case.case_id in selected_case_ids]
    if selected_case_ids:
        unknown_cases = selected_case_ids - {case.case_id for case in CASES}
        if unknown_cases:
            raise ValueError(f"不支持的 case_id: {sorted(unknown_cases)}")

    contract = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir.resolve() if args.output_dir else OUTPUT_ROOT / f"run_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    logger = configure_logging(output_dir)

    write_json(
        output_dir / "run_config.json",
        {
            "started_at": datetime.now().astimezone().isoformat(),
            "pid": os.getpid(),
            "sources": selected_sources,
            "cases": [asdict(case) for case in cases],
            "source_intervals": SOURCE_INTERVALS,
            "start": args.start,
            "end": args.end,
            "contract": str(CONTRACT_PATH),
        },
    )

    summary: list[dict] = []
    with single_instance_lock(LOCK_PATH):
        for source in selected_sources:
            logger.info("开始来源 %s，代表标的 %d 个，最小间隔 %.1f 秒", source, len(cases), SOURCE_INTERVALS[source])
            if source == "baostock":
                import baostock as bs

                login = bs.login()
                if login.error_code != "0":
                    logger.error("BaoStock 登录失败: %s %s", login.error_code, login.error_msg)
                    return 2
                try:
                    run_provider(
                        source,
                        cases,
                        output_dir,
                        contract,
                        summary,
                        lambda case: fetch_baostock(bs, case, args.start, args.end),
                        logger,
                    )
                finally:
                    bs.logout()
            else:
                http = session()
                fetchers = {
                    "eastmoney": lambda case: fetch_eastmoney(http, case),
                    "tencent": lambda case: fetch_tencent(http, case),
                    "sina": lambda case: fetch_sina(http, case),
                }
                run_provider(
                    source,
                    cases,
                    output_dir,
                    contract,
                    summary,
                    fetchers[source],
                    logger,
                )

    write_json(
        output_dir / "completed.json",
        {
            "completed_at": datetime.now().astimezone().isoformat(),
            "results": summary,
        },
    )
    logger.info("历史日线能力探测完成: %s", output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
