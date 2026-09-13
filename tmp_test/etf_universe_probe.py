"""Build a resumable ETF/LOF instrument master from current public sources.

This stage intentionally fetches only bulk snapshot/universe endpoints.  It does
not start per-symbol historical downloads.  Source snapshots are retained as
evidence and a normalized instrument master is emitted for the later daily and
minute pipelines.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import akshare as ak
import pandas as pd
import requests


MIN_PAGE_INTERVAL_SECONDS = 3.0
EASTMONEY_ETF_FS = "b:MK0021,b:MK0022,b:MK0023,b:MK0024,b:MK0827"
EASTMONEY_LOF_FS = "b:MK0404,b:MK0405,b:MK0406,b:MK0407"


def utc_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_frame(frame: pd.DataFrame, stem: Path) -> dict[str, str]:
    stem.parent.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}
    csv_path = stem.with_suffix(".csv")
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    outputs["csv"] = str(csv_path)
    try:
        parquet_path = stem.with_suffix(".parquet")
        frame.to_parquet(parquet_path, index=False)
        outputs["parquet"] = str(parquet_path)
    except Exception as exc:  # parquet is optional evidence
        outputs["parquet_error"] = f"{type(exc).__name__}: {exc}"
    return outputs


@contextmanager
def single_instance_lock(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".etf_universe_probe.lock"
    handle: int | None = None
    try:
        handle = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(handle, f"pid={os.getpid()} started={utc_now()}\n".encode("utf-8"))
        yield
    except FileExistsError as exc:
        raise RuntimeError(f"Another ETF universe probe appears active: {lock_path}") from exc
    finally:
        if handle is not None:
            os.close(handle)
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass


def fetch_sina_category(category: str) -> pd.DataFrame:
    # AKShare exposes this as a single bulk request (num=5000), so there is no
    # hidden per-symbol concurrency.  We throttle between category calls.
    frame = ak.fund_etf_category_sina(symbol=category)
    if frame.empty:
        raise RuntimeError(f"Sina returned an empty {category} snapshot")
    return frame


def normalize_sina(frame: pd.DataFrame, category: str) -> pd.DataFrame:
    if len(frame.columns) < 2:
        raise RuntimeError(f"Unexpected Sina columns: {list(frame.columns)}")
    code_col, name_col = frame.columns[:2]
    result = pd.DataFrame(
        {
            "code": frame[code_col].astype(str).str.lower().str.strip(),
            "name": frame[name_col].astype(str).str.strip(),
            "instrument_type": "etf" if category == "ETF基金" else "lof",
            "source": "sina",
            "source_category": category,
        }
    )
    result = result[result["code"].str.match(r"^(sh|sz)\d{6}$", na=False)].copy()
    result["market"] = result["code"].str[:2]
    return result.drop_duplicates("code").reset_index(drop=True)


def eastmoney_request(
    session: requests.Session,
    page: int,
    page_size: int,
    fs: str = EASTMONEY_ETF_FS,
) -> dict[str, Any]:
    params = {
        "pn": page,
        "pz": page_size,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": fs,
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
    }
    response = session.get(
        "https://push2delay.eastmoney.com/api/qt/clist/get",
        params=params,
        timeout=(5, 20),
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("data") is None:
        raise RuntimeError(f"Eastmoney page {page} returned no data")
    return payload


def fetch_eastmoney_snapshot(
    out_dir: Path,
    max_pages: int = 200,
    fs: str = EASTMONEY_ETF_FS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    session = requests.Session()
    session.trust_env = False
    rows: list[dict[str, Any]] = []
    page_records: list[dict[str, Any]] = []
    total: int | None = None
    failure: str | None = None
    page_size = 100
    for page in range(1, max_pages + 1):
        started = time.monotonic()
        try:
            payload = eastmoney_request(session, page=page, page_size=page_size, fs=fs)
            data = payload["data"]
            diff = data.get("diff") or []
            if total is None:
                total = int(data.get("total") or 0)
            rows.extend(diff)
            page_records.append(
                {"page": page, "rows": len(diff), "elapsed_seconds": round(time.monotonic() - started, 3)}
            )
            write_json(out_dir / "eastmoney_checkpoint.json", {"total": total, "pages": page_records})
            if not diff or (total is not None and len(rows) >= total):
                break
        except Exception as exc:
            failure = f"{type(exc).__name__}: {exc}"
            break
        wait = max(0.0, MIN_PAGE_INTERVAL_SECONDS - (time.monotonic() - started))
        if wait:
            time.sleep(wait + random.uniform(0, 0.15))

    raw = pd.DataFrame(rows)
    meta = {"status": "ok" if not failure else "failed", "failure": failure, "reported_total": total, "rows": len(raw), "pages": page_records}
    return raw, meta


def normalize_eastmoney(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["code", "name", "instrument_type", "source", "source_category", "market"])
    market_prefix = frame["f13"].astype(str).map({"1": "sh", "0": "sz"})
    result = pd.DataFrame(
        {
            "code": market_prefix.fillna("") + frame["f12"].astype(str).str.zfill(6),
            "name": frame["f14"].astype(str).str.strip(),
            "instrument_type": "etf",
            "source": "eastmoney",
            "source_category": "ETF基金",
            "market": market_prefix,
        }
    )
    return result[result["code"].str.match(r"^(sh|sz)\d{6}$", na=False)].drop_duplicates("code").reset_index(drop=True)


def build_master(parts: list[pd.DataFrame]) -> pd.DataFrame:
    available = [item for item in parts if not item.empty]
    if not available:
        return pd.DataFrame(columns=["code", "name", "instrument_type", "market", "sources", "source_categories"])
    union = pd.concat(available, ignore_index=True)
    records: list[dict[str, Any]] = []
    for code, group in union.groupby("code", sort=True):
        names = [value for value in group["name"].tolist() if value and value != "nan"]
        types = sorted(set(group["instrument_type"].tolist()))
        records.append(
            {
                "code": code,
                "name": names[0] if names else "",
                "instrument_type": "etf" if "etf" in types else types[0],
                "market": code[:2],
                "sources": ",".join(sorted(set(group["source"].tolist()))),
                "source_categories": ",".join(sorted(set(group["source_category"].tolist()))),
            }
        )
    return pd.DataFrame(records)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", default="etf_full_probe")
    parser.add_argument("--skip-eastmoney", action="store_true")
    args = parser.parse_args()

    output_root = Path(args.output_root).resolve()
    run_dir = output_root / datetime.now().strftime("run_%Y%m%d_%H%M%S")
    with single_instance_lock(output_root):
        run_dir.mkdir(parents=True, exist_ok=False)
        summary: dict[str, Any] = {"started_at": utc_now(), "policy": {"bulk_page_interval_seconds": MIN_PAGE_INTERVAL_SECONDS}, "sources": {}}
        normalized_parts: list[pd.DataFrame] = []

        for index, category in enumerate(("ETF基金", "LOF基金")):
            started = time.monotonic()
            try:
                raw = fetch_sina_category(category)
                source_key = "sina_etf" if category == "ETF基金" else "sina_lof"
                outputs = save_frame(raw, run_dir / source_key / "snapshot_raw")
                normalized = normalize_sina(raw, category)
                normalized_parts.append(normalized)
                norm_outputs = save_frame(normalized, run_dir / source_key / "instrument_normalized")
                summary["sources"][source_key] = {"status": "ok", "rows": len(raw), "normalized_rows": len(normalized), "elapsed_seconds": round(time.monotonic() - started, 3), "outputs": {**outputs, **{f"normalized_{k}": v for k, v in norm_outputs.items()}}}
            except Exception as exc:
                source_key = "sina_etf" if category == "ETF基金" else "sina_lof"
                summary["sources"][source_key] = {"status": "failed", "error": f"{type(exc).__name__}: {exc}", "elapsed_seconds": round(time.monotonic() - started, 3)}
            if index == 0:
                time.sleep(MIN_PAGE_INTERVAL_SECONDS)

        if not args.skip_eastmoney:
            raw, meta = fetch_eastmoney_snapshot(run_dir / "eastmoney_etf")
            raw_outputs = save_frame(raw, run_dir / "eastmoney_etf" / "snapshot_raw")
            normalized = normalize_eastmoney(raw)
            normalized_parts.append(normalized)
            norm_outputs = save_frame(normalized, run_dir / "eastmoney_etf" / "instrument_normalized")
            summary["sources"]["eastmoney_etf"] = {**meta, "normalized_rows": len(normalized), "outputs": {**raw_outputs, **{f"normalized_{k}": v for k, v in norm_outputs.items()}}}

        master = build_master(normalized_parts)
        master_outputs = save_frame(master, run_dir / "instrument_master")
        summary["instrument_master"] = {
            "rows": len(master),
            "etf_rows": int((master["instrument_type"] == "etf").sum()) if not master.empty else 0,
            "lof_rows": int((master["instrument_type"] == "lof").sum()) if not master.empty else 0,
            "by_market": master["market"].value_counts().to_dict() if not master.empty else {},
            "outputs": master_outputs,
        }
        summary["finished_at"] = utc_now()
        write_json(run_dir / "summary.json", summary)
        (output_root / "LATEST.txt").write_text(str(run_dir), encoding="utf-8")
        print(json.dumps({"run_dir": str(run_dir), **summary["instrument_master"], "sources": summary["sources"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
