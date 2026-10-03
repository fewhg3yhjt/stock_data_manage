"""低频、串行的第一阶段行业数据采集与标准化。"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

from StockInvestmentTool.warehouse.dataset_config import load_dataset_config
from StockInvestmentTool.warehouse.pipeline_state import PipelineState
from StockInvestmentTool.warehouse.quality import check_industry_daily, check_industry_membership
from StockInvestmentTool.warehouse.source_capture import capture_frames
from StockInvestmentTool.warehouse.source_batches import SourceBatchStore
from StockInvestmentTool.warehouse.publish import Publisher

logger = logging.getLogger(__name__)


def parse_baostock_industry(value: str) -> tuple[str, str]:
    """Parse BaoStock's ``I64互联网和相关服务`` style value without guessing."""
    raw = str(value or "").strip()
    match = re.match(r"^([A-Za-z][A-Za-z0-9]*)\s*(.*)$", raw)
    if not match or not match.group(2).strip():
        return "", ""
    return match.group(1), match.group(2).strip()


def normalize_membership(frame: pd.DataFrame, *, snapshot_date: str,
                         captured_at: str | None = None) -> pd.DataFrame:
    captured_at = captured_at or datetime.now().isoformat(timespec="seconds")
    result = frame.copy()
    raw = result["raw_industry"] if "raw_industry" in result else result.get("industry", pd.Series("", index=result.index))
    result["raw_industry"] = raw.fillna("").astype(str)
    parsed = result["raw_industry"].map(parse_baostock_industry)
    code = result["industry_code"] if "industry_code" in result else parsed.map(lambda x: x[0])
    name = result["industry_name"] if "industry_name" in result else parsed.map(lambda x: x[1])
    result["industry_code"] = code.fillna("").astype(str)
    result["industry_name"] = name.fillna("").astype(str)
    result["snapshot_date"] = snapshot_date
    classification = result["industry_classification"] if "industry_classification" in result else pd.Series("csrc", index=result.index)
    result["industry_classification"] = classification.fillna("csrc").astype(str)
    result["source_update_date"] = result["source_update_date"] if "source_update_date" in result else result.get("updateDate", pd.Series(pd.NA, index=result.index))
    result["source"] = "baostock"
    result["captured_at"] = captured_at
    result["code"] = result["code"].astype(str).str.lower()
    columns = load_dataset_config("industry_membership")["fields"]
    return result[[field["name"] for field in columns]].drop_duplicates(
        ["snapshot_date", "code", "industry_classification"]
    ).reset_index(drop=True)


_THS_COLUMNS = {
    "trading_date": ("日期", "date", "交易日期"),
    "industry_name": ("板块", "板块名称", "行业名称", "name"),
    "industry_id": ("板块代码", "行业代码", "industry_id", "代码", "code"),
    "open": ("开盘", "开盘价", "open"), "high": ("最高", "最高价", "high"),
    "low": ("最低", "最低价", "low"), "close": ("收盘", "收盘价", "close"),
    "volume": ("成交量", "volume"), "amount": ("成交额", "amount"),
}


def _column(frame: pd.DataFrame, names: tuple[str, ...], default=pd.NA):
    for name in names:
        if name in frame.columns:
            return frame[name]
    return pd.Series(default, index=frame.index)


def normalize_industry_daily(frame: pd.DataFrame, *, industry_id: str,
                             industry_name: str, source_symbol: str | None = None,
                             captured_at: str | None = None) -> pd.DataFrame:
    captured_at = captured_at or datetime.now().isoformat(timespec="seconds")
    result = pd.DataFrame(index=frame.index)
    for target, names in _THS_COLUMNS.items():
        result[target] = _column(frame, names)
    result["industry_id"] = result["industry_id"].fillna(industry_id).astype(str)
    result["industry_name"] = result["industry_name"].replace("<NA>", pd.NA).fillna(industry_name).astype(str)
    result["source"] = "akshare"
    result["source_symbol"] = source_symbol or industry_name
    result["captured_at"] = captured_at
    result["trading_date"] = pd.to_datetime(result["trading_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for name in ("open", "high", "low", "close", "volume", "amount"):
        result[name] = pd.to_numeric(result[name], errors="coerce")
    return result[list(_THS_COLUMNS) + ["source", "source_symbol", "captured_at"]].drop_duplicates(
        ["trading_date", "industry_id"]
    ).reset_index(drop=True)


class IndustryCollector:
    """Adapters are injected so tests never need network access."""

    def __init__(self, warehouse, *, baostock_query: Callable[[str], object] | None = None,
                 ths_names: Callable[[], pd.DataFrame] | None = None,
                 ths_history: Callable[..., pd.DataFrame] | None = None,
                 interval: float = 1.0, retries: int = 3, backoff: float = 2.0,
                 checkpoint_dir: Path | None = None, sleep: Callable[[float], None] = time.sleep):
        if not isinstance(retries, int) or isinstance(retries, bool) or retries < 1:
            raise ValueError("retries 必须是大于等于 1 的整数")
        if interval < 0:
            raise ValueError("interval 不能小于 0")
        if backoff < 0:
            raise ValueError("backoff 不能小于 0")
        self.warehouse, self.baostock_query, self.ths_names = warehouse, baostock_query, ths_names
        self.ths_history, self.interval, self.retries, self.backoff = ths_history, interval, retries, backoff
        self.sleep = sleep
        self.checkpoint_dir = checkpoint_dir or warehouse.base_dir / "checkpoints"

    def _call(self, fn, *args, **kwargs):
        error = None
        for attempt in range(self.retries):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                error = exc
                if attempt + 1 < self.retries:
                    self.sleep(self.backoff * (2 ** attempt))
        raise error

    def collect_membership(self, codes: Iterable[str] | None = None, *, snapshot_date: str | None = None,
                           max_symbols: int | None = None, refresh: bool = False) -> dict:
        from StockInvestmentTool.datasource.fetcher import StockDataFetcher
        query = self.baostock_query or StockDataFetcher().get_stock_industry
        codes = list(codes if codes is not None else self.warehouse.all_codes())
        types = self.warehouse.instrument_types()
        codes = [code for code in codes if types.get(code, "stock") == "stock"]
        if max_symbols:
            codes = codes[:max_symbols]
        snapshot_date = snapshot_date or datetime.now().strftime("%Y-%m-%d")
        checkpoint = self.checkpoint_dir / f"industry_membership_{snapshot_date}.json"
        completed = set()
        saved_values = {}
        if checkpoint.exists() and not refresh:
            checkpoint_data = json.loads(checkpoint.read_text(encoding="utf-8"))
            completed = set(checkpoint_data.get("completed", []))
            saved_values = checkpoint_data.get("results", {})
        # Checkpoints are source-pipeline state only. Never reconstruct a
        # formal membership snapshot from the legacy instruments.industry cache.
        rows = [{"code": code, "industry": saved_values[code]} for code in codes if code in completed]
        failed, skipped = [], 0
        for code in codes:
            if code in completed:
                skipped += 1
                continue
            try:
                value = self._call(query, code)
                source_update_date = None
                classification = "csrc"
                if isinstance(value, dict):
                    source_update_date = value.get("updateDate") or value.get("source_update_date")
                    classification = value.get("industryClassification") or value.get("industry_classification") or classification
                    value = value.get("industry", "")
                if not value or not str(value).strip():
                    failed.append(code)
                    logger.warning("行业成员采集为空 %s", code)
                else:
                    rows.append({"code": code, "industry": value,
                                 "source_update_date": source_update_date,
                                 # Keep a stable internal identifier in the
                                 # formal dataset; the source text remains in
                                 # the raw industry value and source metadata.
                                 "industry_classification": "csrc"})
                    saved_values[code] = value
                    completed.add(code)
            except Exception as exc:
                failed.append(code)
                logger.warning("行业成员采集失败 %s: %s", code, exc)
            checkpoint.parent.mkdir(parents=True, exist_ok=True)
            checkpoint.write_text(json.dumps({"completed": sorted(completed), "failed": failed,
                                              "results": saved_values}, ensure_ascii=False), encoding="utf-8")
            self.sleep(self.interval)
        normalized = normalize_membership(pd.DataFrame(rows, columns=["code", "industry", "source_update_date", "industry_classification"]), snapshot_date=snapshot_date)
        raw = capture_frames(self.warehouse, dataset_name="industry_membership", source_name="baostock", frames=[normalized],
                             run_date=snapshot_date, trade_date_start=snapshot_date, trade_date_end=snapshot_date,
                             expected_symbols=len(codes), success_symbols=len(rows), failed_symbols=len(failed), skipped_symbols=skipped,
                             universe_id=f"stock_{snapshot_date}", request_context={"snapshot_date": snapshot_date},
                             schema_version="industry_membership.v1", failure_details=failed) if not normalized.empty else None
        return {"raw_batch_id": raw["batch_id"] if raw else None, "success": len(rows), "expected_symbols": len(codes), "failed": failed, "skipped": skipped}

    def collect_daily(self, *, start_date: str, end_date: str, industries: list[dict] | None = None) -> dict:
        import akshare as ak
        names = industries if industries is not None else [
            {"industry_id": str(row.get("code", row.get("板块代码", row.get("代码", "")))), "industry_name": str(row.get("name", row.get("板块", row.get("行业名称", ""))))}
            for _, row in self._call(self.ths_names or ak.stock_board_industry_name_ths).iterrows()
        ]
        # 同花顺行业指数接口在单日窗口下经常只返回少量板块，导致质量检查 FAIL、
        # 旧版本不被替换，行业指数因此停滞。采集时向前扩展一个足够的历史窗口，
        # 覆盖全部板块且保证请求日有行情；后续 build 只发布请求日的分区。
        from datetime import timedelta
        collect_start = (pd.Timestamp(end_date) - pd.Timedelta(days=45)).strftime("%Y-%m-%d")
        d_start = collect_start.replace("-", "")
        d_end = end_date.replace("-", "")
        frames, failed = [], []
        for item in names:
            try:
                raw = self._call(self.ths_history or ak.stock_board_industry_index_ths,
                                 symbol=item["industry_name"], start_date=d_start, end_date=d_end)
                frames.append(normalize_industry_daily(raw, **item, source_symbol=item["industry_name"]))
            except Exception as exc:
                failed.append(item["industry_id"] or item["industry_name"])
                logger.warning("行业指数采集失败 %s: %s", item, exc)
            self.sleep(self.interval)
        raw_result = capture_frames(self.warehouse, dataset_name="industry_daily", source_name="akshare", frames=frames,
                                    run_date=end_date, trade_date_start=collect_start, trade_date_end=end_date,
                                    expected_symbols=len(names), success_symbols=len(frames), failed_symbols=len(failed),
                                    universe_id="ths_industry", request_context={"start_date": collect_start, "end_date": end_date,
                                    "api_name": "stock_board_industry_index_ths"}, schema_version="industry_daily.v1",
                                    failure_details=failed) if frames else None
        return {"raw_batch_id": raw_result["batch_id"] if raw_result else None, "success": len(frames), "expected_symbols": len(names), "failed": failed}


def build_industry_candidate(warehouse, dataset_name: str, partition: str, raw_path: Path) -> dict:
    config = load_dataset_config(dataset_name)
    frame = pd.read_parquet(raw_path)
    if dataset_name == "industry_daily":
        frame["trading_date"] = pd.to_datetime(frame["trading_date"], errors="coerce")
        frame = frame[frame["trading_date"].dt.strftime("%Y-%m") == partition].copy()
    keys = config["dataset"]["primary_keys"]
    frame = frame.drop_duplicates(keys).sort_values(keys).reset_index(drop=True)
    path = warehouse.base_dir / "candidates" / dataset_name / partition / f"{dataset_name}_{partition.replace('-', '')}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False, engine="pyarrow", compression="zstd")
    import hashlib
    return {"version_id": f"{dataset_name}_{partition.replace('-', '')}_{hashlib.sha256(path.read_bytes()).hexdigest()[:12]}",
            "dataset_name": dataset_name, "partition": partition, "path": path, "row_count": len(frame),
            "symbol_count": int(frame["code" if "code" in frame else "industry_id"].nunique()), "checksum": hashlib.sha256(path.read_bytes()).hexdigest(),
            "source_batches": [str(raw_path)]}


def stage_and_publish_industry(warehouse, *, dataset_name: str, partition: str,
                               batch_id: str, expected_symbols: int | None = None) -> dict:
    """Complete Raw -> Candidate -> Quality -> Publish for one captured batch."""
    batch = SourceBatchStore(warehouse.meta_db_path).get(batch_id)
    if not batch or not batch.get("raw_path"):
        raise ValueError(f"Raw Batch 不存在或没有文件: {batch_id}")
    build = build_industry_candidate(warehouse, dataset_name, partition, Path(batch["raw_path"]))
    state = PipelineState(warehouse.meta_db_path)
    version = state.create_version(
        build, source_batches=[batch_id], builder_version="industry_builder.v1",
        dataset_name=dataset_name, schema_version=load_dataset_config(dataset_name)["dataset"]["schema_version"],
    )
    checker = check_industry_membership if dataset_name == "industry_membership" else check_industry_daily
    quality = checker(build["path"], expected_symbols=expected_symbols)
    state.quality(version, status=quality["status"], checks=quality["checks"],
                  publish_allowed=quality["publish_allowed"], affected_symbols=[])
    published = Publisher(warehouse).publish(version) if quality["publish_allowed"] else None
    return {"version_id": version, "quality": quality, "published": published}


def stage_and_publish_industry_batch(warehouse, *, dataset_name: str, batch_id: str,
                                    expected_symbols: int | None = None,
                                    partition: str | None = None) -> dict:
    """Publish every month represented by one industry_daily Raw Batch.

    ``partition`` (YYYY-MM) restricts publish to one month, used by the daily
    retry so a wide capture window does not re-publish the prior month.
    """
    batch = SourceBatchStore(warehouse.meta_db_path).get(batch_id)
    if not batch or not batch.get("raw_path"):
        raise ValueError(f"Raw Batch 不存在或没有文件: {batch_id}")
    frame = pd.read_parquet(batch["raw_path"])
    date_col = "snapshot_date" if dataset_name == "industry_membership" else "trading_date"
    results = {}
    partitions = ([str(value)[:10] for value in frame[date_col].dropna().unique()]
                  if dataset_name == "industry_membership" else
                  [str(value)[:7] for value in frame[date_col].dropna().unique()])
    if partition:
        partitions = [p for p in partitions if p == partition]
    for partition in sorted(set(partitions)):
        try:
            results[partition] = stage_and_publish_industry(
                warehouse, dataset_name=dataset_name, partition=partition,
                batch_id=batch_id, expected_symbols=expected_symbols,
            )
        except Exception as exc:
            logger.error("行业数据分区发布失败 %s/%s: %s", dataset_name, partition, exc)
            results[partition] = {"error": str(exc), "published": None}
    return results
