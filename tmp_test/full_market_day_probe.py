from __future__ import annotations

"""Safely probe one completed A-share trading day across market-data sources.

The probe is intentionally conservative:

* one process only (Windows file lock)
* no threading or multiprocessing
* bulk endpoints are preferred
* per-symbol BaoStock requests are rate limited and checkpointed
* repeated source failures trip a circuit breaker
* every source writes to its own directory

This is a measurement tool, not the production market-data pipeline.
"""

import argparse
import json
import logging
import math
import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_ROOT = BASE_DIR / "market_day_probe"
LOCK_PATH = OUTPUT_ROOT / ".full_day_probe.lock"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 Chrome/152 Safari/537.36"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="单日全市场数据源安全试跑")
    parser.add_argument("--date", help="目标交易日 YYYY-MM-DD；默认自动向前寻找")
    parser.add_argument(
        "--baostock-limit",
        type=int,
        default=0,
        help="BaoStock 最多逐股请求多少只；0 表示全部",
    )
    parser.add_argument(
        "--baostock-qps",
        type=float,
        default=2.0,
        help="BaoStock 每秒最大请求数，默认 2（逐只至少间隔 0.5 秒）",
    )
    parser.add_argument(
        "--tencent-batch-interval",
        type=float,
        default=3.0,
        help="腾讯批次间隔秒数，默认 3",
    )
    parser.add_argument(
        "--eastmoney-page-interval",
        type=float,
        default=3.0,
        help="东方财富分页间隔秒数，默认 3",
    )
    parser.add_argument(
        "--sina-page-interval",
        type=float,
        default=3.0,
        help="新浪分页间隔秒数，默认 3",
    )
    parser.add_argument(
        "--skip-sina",
        action="store_true",
        help="跳过新浪全市场分页快照",
    )
    parser.add_argument(
        "--sources",
        default="baostock,tencent,eastmoney,sina",
        help="逗号分隔的数据源: baostock,tencent,eastmoney,sina",
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        help="复用指定运行目录并从 BaoStock 检查点续跑",
    )
    return parser.parse_args()


@contextmanager
def single_instance_lock(path: Path):
    """Hold a non-blocking one-byte Windows lock for the whole run."""
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
            raise RuntimeError("已有同类型全量试跑任务正在运行，拒绝重复启动") from exc
        yield
    finally:
        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            handle.close()


def configure_logging(run_dir: Path) -> logging.Logger:
    logger = logging.getLogger("full_market_day_probe")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    file_handler = logging.FileHandler(run_dir / "run.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def timed_case(
    name: str,
    fn: Callable[[], dict],
    summary: list[dict],
    logger: logging.Logger,
) -> dict | None:
    started = time.perf_counter()
    try:
        result = fn()
        row = {
            "source": name,
            "status": "OK",
            "elapsed_sec": round(time.perf_counter() - started, 3),
            **result,
            "error": "",
        }
        summary.append(row)
        logger.info("%s 完成: %s", name, row)
        return result
    except Exception as exc:
        row = {
            "source": name,
            "status": "ERROR",
            "elapsed_sec": round(time.perf_counter() - started, 3),
            "rows": 0,
            "error": f"{type(exc).__name__}: {exc}",
        }
        summary.append(row)
        logger.warning("%s 失败: %s", name, row["error"])
        return None


def bs_rows(result_set) -> list[dict]:
    fields = result_set.fields
    if not isinstance(fields, list):
        fields = fields.split(",")
    rows: list[dict] = []
    while result_set.error_code == "0" and result_set.next():
        rows.append(dict(zip(fields, result_set.get_row_data())))
    return rows


def is_a_share(code: str) -> bool:
    market, _, digits = code.lower().partition(".")
    if market == "sh":
        return digits.startswith(("600", "601", "603", "605", "688", "689"))
    if market == "sz":
        return digits.startswith(("000", "001", "002", "003", "300", "301", "302"))
    if market == "bj":
        return digits.startswith(("4", "8", "9"))
    return False


def find_universe(
    requested_day: str | None,
    source_dir: Path,
    logger: logging.Logger,
) -> tuple[object, str, pd.DataFrame]:
    import baostock as bs

    login = bs.login()
    if login.error_code != "0":
        raise ConnectionError(f"BaoStock 登录失败: {login.error_code} {login.error_msg}")

    if requested_day:
        candidates = [datetime.strptime(requested_day, "%Y-%m-%d").date()]
    else:
        today = date.today()
        candidates = [today - timedelta(days=i) for i in range(0, 15)]

    for candidate in candidates:
        day_text = candidate.strftime("%Y-%m-%d")
        rs = bs.query_all_stock(day=day_text)
        if rs.error_code != "0":
            logger.warning("BaoStock 股票清单 %s 失败: %s", day_text, rs.error_msg)
            continue
        frame = pd.DataFrame(bs_rows(rs))
        if frame.empty or "code" not in frame:
            continue
        stocks = frame[frame["code"].astype(str).map(is_a_share)].copy()
        if stocks.empty:
            continue
        stocks["trade_date"] = day_text
        save_frame(source_dir / "universe.parquet", stocks)
        logger.info("确定交易日 %s，A股清单 %d 只", day_text, len(stocks))
        return bs, day_text, stocks

    bs.logout()
    raise RuntimeError("最近 15 天均未取得有效 A 股清单")


def fetch_baostock_daily(
    bs,
    trade_date: str,
    universe: pd.DataFrame,
    source_dir: Path,
    qps: float,
    limit: int,
    logger: logging.Logger,
) -> dict:
    checkpoint = source_dir / "daily_checkpoint.jsonl"
    completed: set[str] = set()
    if checkpoint.exists():
        with checkpoint.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                    if record.get("status") == "OK":
                        completed.add(str(record.get("code")))
                except json.JSONDecodeError:
                    continue

    codes = universe["code"].astype(str).tolist()
    if limit > 0:
        codes = codes[:limit]
    interval = max(0.5, 1.0 / max(qps, 0.1))
    attempted = succeeded = failed = 0
    consecutive_failures = 0
    started = time.perf_counter()

    with checkpoint.open("a", encoding="utf-8", buffering=1) as handle:
        for position, code in enumerate(codes, start=1):
            if code in completed:
                continue
            request_started = time.perf_counter()
            attempted += 1
            rs = bs.query_history_k_data_plus(
                code,
                "date,code,open,high,low,close,preclose,volume,amount,"
                "adjustflag,turn,tradestatus,pctChg,isST",
                start_date=trade_date,
                end_date=trade_date,
                frequency="d",
                adjustflag="3",
            )
            rows = bs_rows(rs) if rs.error_code == "0" else []
            if rows:
                record = {"status": "OK", **rows[0]}
                succeeded += 1
                consecutive_failures = 0
            else:
                record = {
                    "status": "EMPTY" if rs.error_code == "0" else "ERROR",
                    "code": code,
                    "date": trade_date,
                    "error_code": rs.error_code,
                    "error_msg": rs.error_msg,
                }
                failed += 1
                consecutive_failures += 1
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

            if position % 100 == 0 or position == len(codes):
                elapsed = time.perf_counter() - started
                logger.info(
                    "BaoStock 日线进度 %d/%d，成功 %d，空/失败 %d，%.2f 条/秒",
                    position,
                    len(codes),
                    succeeded,
                    failed,
                    attempted / max(elapsed, 0.001),
                )

            # Stop quickly when the upstream is clearly unhealthy.
            if consecutive_failures >= 20:
                raise RuntimeError("BaoStock 连续 20 只失败，已触发熔断")
            if attempted >= 100 and failed / attempted > 0.35:
                raise RuntimeError("BaoStock 失败/空数据比例超过 35%，已触发熔断")

            spent = time.perf_counter() - request_started
            if spent < interval:
                time.sleep(interval - spent)

    records = [json.loads(line) for line in checkpoint.read_text(encoding="utf-8").splitlines() if line]
    final = pd.DataFrame(records)
    save_frame(source_dir / "daily.parquet", final)
    return {
        "rows": len(final),
        "requested": len(codes),
        "ok_rows": int((final["status"] == "OK").sum()),
        "trade_date": trade_date,
    }


def request_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    return session


def tencent_symbol(code: str) -> str:
    return code.replace(".", "").lower()


def parse_tencent_line(line: str) -> dict | None:
    if "=" not in line:
        return None
    key, value = line.split("=", 1)
    symbol = key.strip().removeprefix("v_")
    parts = value.strip().strip(";\"").split("~")

    def item(index: int) -> str | None:
        if index >= len(parts) or parts[index] in ("", "-"):
            return None
        return parts[index]

    return {
        "source_symbol": symbol,
        "name": item(1),
        "code": item(2),
        "close": item(3),
        "pre_close": item(4),
        "open": item(5),
        "volume_raw_lot": item(6),
        "quote_time": item(30),
        "pct_change": item(32),
        "high": item(33),
        "low": item(34),
        "amount_raw_wan": item(37),
        "raw_field_count": len(parts),
    }


def fetch_tencent(
    universe: pd.DataFrame,
    source_dir: Path,
    logger: logging.Logger,
    batch_interval: float = 3.0,
) -> dict:
    source_dir.mkdir(parents=True, exist_ok=True)
    session = request_session()
    symbols = [tencent_symbol(code) for code in universe["code"].astype(str)]
    batch_size = 50
    raw_path = source_dir / "responses.jsonl"
    rows: list[dict] = []
    failures = 0
    with raw_path.open("w", encoding="utf-8", buffering=1) as raw_handle:
        for offset in range(0, len(symbols), batch_size):
            chunk = symbols[offset : offset + batch_size]
            try:
                response = session.get(
                    "https://qt.gtimg.cn/q=" + ",".join(chunk),
                    timeout=(5, 15),
                )
                response.raise_for_status()
                response.encoding = "gbk"
                raw_handle.write(
                    json.dumps(
                        {"symbols": chunk, "status": response.status_code, "text": response.text},
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                rows.extend(
                    parsed
                    for line in response.text.splitlines()
                    if (parsed := parse_tencent_line(line)) is not None
                )
                failures = 0
            except Exception as exc:
                failures += 1
                raw_handle.write(
                    json.dumps(
                        {"symbols": chunk, "error": f"{type(exc).__name__}: {exc}"},
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                if failures >= 5:
                    raise RuntimeError("腾讯连续 5 个批次失败，已触发熔断") from exc
            done = min(offset + batch_size, len(symbols))
            if done % 500 == 0 or done == len(symbols):
                logger.info("腾讯批量报价进度 %d/%d，已解析 %d", done, len(symbols), len(rows))
            if done < len(symbols):
                time.sleep(max(batch_interval, 3.0))
    frame = pd.DataFrame(rows)
    save_frame(source_dir / "daily.parquet", frame)
    return {"rows": len(frame), "requested": len(symbols)}


def fetch_eastmoney_bulk(
    source_dir: Path,
    dataset: str,
    fs: str,
    page_interval: float = 3.0,
    max_pages: int | None = None,
) -> dict:
    source_dir.mkdir(parents=True, exist_ok=True)
    session = request_session()
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    fields = "f2,f3,f5,f6,f7,f8,f12,f13,f14,f15,f16,f17,f18"
    attempt_stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    raw_path = source_dir / f"{dataset}_responses_{attempt_stamp}.jsonl"
    records: list[dict] = []
    total = None
    page = 1
    with raw_path.open("w", encoding="utf-8", buffering=1) as raw_handle:
        page_limit = min(max_pages or 200, 200)
        while page <= page_limit:
            response = session.get(
                url,
                params={
                    "pn": str(page),
                    "pz": "100",
                    "po": "1",
                    "np": "1",
                    "fltt": "2",
                    "invt": "2",
                    "fid": "f12",
                    "fs": fs,
                    "fields": fields,
                },
                timeout=(5, 25),
            )
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data") or {}
            diff = data.get("diff") or []
            total = data.get("total", total)
            raw_handle.write(
                json.dumps(
                    {"page": page, "status": response.status_code, "payload": payload},
                    ensure_ascii=False,
                )
                + "\n"
            )
            if not diff:
                break
            records.extend(diff)
            if total is not None and len(records) >= int(total):
                break
            page += 1
            if page <= page_limit:
                time.sleep(max(page_interval, 3.0))

    frame = pd.DataFrame(records)
    frame = frame.rename(
        columns={
            "f12": "code",
            "f13": "market_id",
            "f14": "name",
            "f17": "open",
            "f15": "high",
            "f16": "low",
            "f2": "close",
            "f18": "pre_close",
            "f5": "volume_raw_lot",
            "f6": "amount_raw_cny",
            "f3": "pct_change",
            "f7": "amplitude",
            "f8": "turnover",
        }
    )
    numeric_columns = [
        "market_id",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume_raw_lot",
        "amount_raw_cny",
        "pct_change",
        "amplitude",
        "turnover",
    ]
    for column in numeric_columns:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    save_frame(source_dir / f"{dataset}.parquet", frame)
    return {
        "rows": len(frame),
        "total": total,
        "pages": page,
        "complete": total is not None and len(frame) >= int(total),
    }


def fetch_sina(
    source_dir: Path,
    page_interval: float = 3.0,
    max_pages: int | None = None,
) -> dict:
    """Fetch Sina pages with an explicit interval; AKShare's wrapper has no delay."""
    from akshare.stock.cons import (
        zh_sina_a_stock_count_url,
        zh_sina_a_stock_payload,
        zh_sina_a_stock_url,
    )
    from akshare.utils import demjson

    source_dir.mkdir(parents=True, exist_ok=True)
    session = request_session()
    count_response = session.get(zh_sina_a_stock_count_url, timeout=(5, 20))
    count_response.raise_for_status()
    count_matches = re.findall(r"\d+", count_response.text)
    if not count_matches:
        raise ValueError("新浪股票数量接口返回的不是有效数字")
    total = int(count_matches[0])
    total_pages = math.ceil(total / 80)
    page_limit = min(max_pages or total_pages, total_pages)
    attempt_stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    raw_path = source_dir / f"stocks_responses_{attempt_stamp}.jsonl"
    frames: list[pd.DataFrame] = []
    with raw_path.open("w", encoding="utf-8", buffering=1) as raw_handle:
        for page in range(1, page_limit + 1):
            params = zh_sina_a_stock_payload.copy()
            params["page"] = page
            response = session.get(zh_sina_a_stock_url, params=params, timeout=(5, 25))
            response.raise_for_status()
            body = response.text.strip()
            if not body or body.startswith("<"):
                raise ValueError(f"新浪第 {page} 页返回 HTML 或空内容")
            payload = demjson.decode(body)
            raw_handle.write(
                json.dumps(
                    {"page": page, "status": response.status_code, "payload": payload},
                    ensure_ascii=False,
                )
                + "\n"
            )
            frames.append(pd.DataFrame(payload))
            if page < page_limit:
                time.sleep(max(page_interval, 3.0))

    frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    save_frame(source_dir / "stocks.parquet", frame)
    return {
        "rows": len(frame),
        "total": total,
        "pages": page_limit,
        "total_pages": total_pages,
        "complete": page_limit == total_pages,
        "columns": "|".join(map(str, frame.columns)),
    }


def main() -> int:
    args = parse_args()
    selected_sources = {item.strip().lower() for item in args.sources.split(",") if item.strip()}
    supported_sources = {"baostock", "tencent", "eastmoney", "sina"}
    unknown_sources = selected_sources - supported_sources
    if unknown_sources:
        raise ValueError(f"不支持的数据源: {sorted(unknown_sources)}")
    with single_instance_lock(LOCK_PATH):
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = args.run_dir.resolve() if args.run_dir else OUTPUT_ROOT / f"run_{stamp}"
        run_dir.mkdir(parents=True, exist_ok=True)
        logger = configure_logging(run_dir)
        summary: list[dict] = []
        write_json(
            run_dir / "run_config.json",
            {
                "started_at": datetime.now().astimezone().isoformat(),
                "pid": os.getpid(),
                "requested_date": args.date,
                "baostock_limit": args.baostock_limit,
                "baostock_qps": args.baostock_qps,
                "tencent_batch_interval": args.tencent_batch_interval,
                "eastmoney_page_interval": args.eastmoney_page_interval,
                "sina_page_interval": args.sina_page_interval,
                "skip_sina": args.skip_sina,
                "sources": sorted(selected_sources),
            },
        )

        baostock_dir = run_dir / "baostock"
        try:
            bs, trade_date, universe = find_universe(args.date, baostock_dir, logger)
        except Exception as exc:
            logger.error("无法取得基准证券清单: %s: %s", type(exc).__name__, exc)
            return 2

        try:
            if "baostock" in selected_sources:
                timed_case(
                    "baostock_daily",
                    lambda: fetch_baostock_daily(
                        bs,
                        trade_date,
                        universe,
                        baostock_dir,
                        args.baostock_qps,
                        args.baostock_limit,
                        logger,
                    ),
                    summary,
                    logger,
                )
        finally:
            try:
                bs.logout()
            except Exception:
                logger.warning("BaoStock 登出失败", exc_info=True)

        if "tencent" in selected_sources:
            timed_case(
                "tencent_stocks",
                lambda: fetch_tencent(
                    universe,
                    run_dir / "tencent",
                    logger,
                    args.tencent_batch_interval,
                ),
                summary,
                logger,
            )
        if "eastmoney" in selected_sources:
            timed_case(
                "eastmoney_stocks",
                lambda: fetch_eastmoney_bulk(
                    run_dir / "eastmoney",
                    "stocks",
                    "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
                    args.eastmoney_page_interval,
                ),
                summary,
                logger,
            )
            timed_case(
                "eastmoney_industry_boards",
                lambda: fetch_eastmoney_bulk(
                    run_dir / "eastmoney",
                    "industry_boards",
                    "m:90+t:2+f:!50",
                    args.eastmoney_page_interval,
                ),
                summary,
                logger,
            )
            timed_case(
                "eastmoney_concept_boards",
                lambda: fetch_eastmoney_bulk(
                    run_dir / "eastmoney",
                    "concept_boards",
                    "m:90+t:3+f:!50",
                    args.eastmoney_page_interval,
                ),
                summary,
                logger,
            )
        if "sina" in selected_sources and not args.skip_sina:
            timed_case(
                "sina_stocks",
                lambda: fetch_sina(
                    run_dir / "sina",
                    args.sina_page_interval,
                ),
                summary,
                logger,
            )

        summary_frame = pd.DataFrame(summary)
        summary_frame.to_csv(run_dir / "summary.csv", index=False, encoding="utf-8-sig")
        write_json(
            run_dir / "completed.json",
            {
                "completed_at": datetime.now().astimezone().isoformat(),
                "trade_date": trade_date,
                "sources": summary,
            },
        )
        logger.info("试跑完成，结果目录: %s", run_dir)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
