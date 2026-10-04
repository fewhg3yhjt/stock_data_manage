from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ..contracts import FailureClass, ProviderContractError
from ...quality.normalization import Normalizer
from .session import load_client


@dataclass(frozen=True, slots=True)
class AkShareBoardResult:
    rows: tuple[Mapping[str, Any], ...]
    response_statuses: tuple[int, ...] = ()
    field_semantics: tuple[str, ...] = ()
    units: tuple[str, ...] = ()
    snapshot_at: datetime | None = None
    returned_first_key: str | None = None
    returned_last_key: str | None = None
    source_rows: tuple[Mapping[str, Any], ...] = ()
    mapping_context: Mapping[str, Any] = field(default_factory=dict)
    source_url: str | None = None


@dataclass(slots=True)
class AkShareBoardProvider:
    """THS board index history and THS industry/concept flow via AkShare."""

    client: Any | None = None
    name: str = "akshare"
    capability_version: str = "akshare-ths-board-input-v2"
    endpoint: str = "industry_index_daily"
    normalization_root: Path | None = None
    input_hosts: tuple[str, ...] = ("https://q.10jqka.com.cn", "https://d.10jqka.com.cn", "http://data.10jqka.com.cn")

    def fetch_concept_list(self, *, source_responses):
        from bs4 import BeautifulSoup
        from ..contracts import InputFetchResult
        self.client = self.client or load_client()
        frame = self.client.stock_board_concept_name_ths()
        responses = list(source_responses())
        if not responses or not any('/gn/detail/code/307822/' in r['url'] for r in responses):
            raise ValueError('original concept directory response is required; SDK cache alone is insufficient')
        if tuple(frame.columns) != ('name', 'code') or frame.empty:
            raise ValueError('concept directory fields or nonempty contract changed')
        rows = tuple(frame.astype(object).where(frame.notna(), None).to_dict('records'))
        if any(not r['name'] or not str(r['code']).isdigit() for r in rows):
            raise ValueError('concept directory name/code missing')
        if len({r['code'] for r in rows}) != len(rows):
            raise ValueError('duplicate concept code')
        # Independently justify every returned pair with an anchor in retained HTML.
        pairs = set()
        for response in responses:
            soup = BeautifulSoup(response['body'].decode(response.get('encoding') or 'utf-8'), 'lxml')
            pairs.update((a.get_text(), a['href'].rsplit('/')[-2]) for a in soup.find_all('a', href=True)
                         if '/detail/code/' in a['href'])
        if any((r['name'], r['code']) not in pairs for r in rows):
            raise ValueError('concept directory pair has no source anchor')
        return InputFetchResult(rows, source_rows=rows, source_url=responses[0]['url'],
            mapping_context={'board_type': 'concept', 'source': 'ths',
                             'scope_meaning': 'returned concept names and codes; no hotness or membership',
                             'pagination_completeness_verified': False})

    def _mapping(self, dataset: str, input_id: str) -> Mapping[str, str]:
        from ...config.loader import load_normalization_document
        document = load_normalization_document(self.normalization_root or Path(__file__).resolve().parents[4] / "config/normalization", dataset)
        return next(rule["field_mapping"] for rule in document["rules"] if rule["input_id"] == input_id)

    def fetch_industry_list(self) -> AkShareBoardResult:
        ak = self.client or load_client()
        try:
            frame = ak.stock_board_industry_name_ths()
        except (ValueError, FileNotFoundError, FileExistsError, PermissionError):
            raise
        except Exception as exc:
            raise ProviderContractError(
                "AkShare THS industry list request failed",
                FailureClass.CONNECTION,
                retryable=True,
            ) from exc
        columns = tuple(str(item) for item in getattr(frame, "columns", ()))
        if columns != ("name", "code"):
            raise ProviderContractError(
                f"AkShare THS industry list columns changed: {columns}",
                FailureClass.SCHEMA_CHANGED,
                retryable=False,
            )
        source_rows = tuple(dict(zip(columns, row)) for row in _values(frame)
                            if len(row) >= 2 and str(row[0]).strip() and str(row[1]).strip())
        mapping = self._mapping("industry_directory", "SDA-BOARD-001")
        rows = tuple({"board_type": "industry", **{key: str(value).strip()
                      for key, value in Normalizer.map_fields(row, mapping).items()}, "source": "ths"}
                     for row in source_rows)
        if not rows:
            raise ProviderContractError(
                "AkShare THS industry list returned no rows",
                FailureClass.TEMPORARY_EMPTY,
                retryable=False,
            )
        return AkShareBoardResult(
            rows,
            field_semantics=("board_name", "board_code"),
            returned_first_key=str(rows[0]["board_code"]),
            returned_last_key=str(rows[-1]["board_code"]),
            source_rows=source_rows,
            mapping_context={"board_type": "industry", "source": "ths"},
        )

    def fetch_industry_daily(
        self, board_name: str, start_date: date, end_date: date, *, board_code: str | None = None
    ) -> AkShareBoardResult:
        if end_date < start_date:
            raise ValueError("end_date must not precede start_date")
        if not board_code:
            directory = self.fetch_industry_list()
            board = next(
                (row for row in directory.rows if row["board_name"] == board_name), None
            )
            if board is None:
                raise ProviderContractError(
                    f"AkShare THS industry directory has no board named {board_name}",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
            board_code = str(board["board_code"])
        ak = self.client or load_client()
        try:
            frame = ak.stock_board_industry_index_ths(
                symbol=board_name,
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
            )
        except (ValueError, FileNotFoundError, FileExistsError, PermissionError):
            raise
        except Exception as exc:
            raise ProviderContractError(
                f"AkShare THS industry index request failed for {board_name}",
                FailureClass.CONNECTION,
                retryable=True,
            ) from exc
        expected = ("日期", "开盘价", "最高价", "最低价", "收盘价", "成交量", "成交额")
        columns = tuple(str(item) for item in getattr(frame, "columns", ()))
        if columns != expected:
            raise ProviderContractError(
                f"AkShare THS industry index columns changed: {columns}",
                FailureClass.SCHEMA_CHANGED,
                retryable=False,
            )
        rows: list[Mapping[str, Any]] = []
        source_rows = []
        mapping = self._mapping("industry_index_daily", "SDA-BOARD-002")
        for values in _values(frame):
            if len(values) < 7:
                raise ProviderContractError(
                    "AkShare THS industry index row is incomplete",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
            trade_date = _date(values[0])
            if not start_date.isoformat() <= trade_date <= end_date.isoformat():
                continue
            raw = dict(zip(columns, values))
            source_rows.append(raw)
            rows.append(
                {
                    **Normalizer.map_fields(raw, mapping),
                    "board_type": "industry",
                    "board_code": board_code,
                    "board_name": board_name,
                    "trade_date": trade_date,
                    "source": "ths",
                }
            )
        if not rows:
            raise ProviderContractError(
                f"AkShare THS industry index returned no rows for {board_name}",
                FailureClass.TEMPORARY_EMPTY,
                retryable=False,
            )
        return AkShareBoardResult(
            tuple(rows),
            field_semantics=("trade_date", "open", "high", "low", "close", "volume", "amount"),
            units=("volume:source_unit_unconfirmed", "amount:source_unit_unconfirmed"),
            returned_first_key=str(rows[0]["trade_date"]),
            returned_last_key=str(rows[-1]["trade_date"]),
            source_rows=tuple(source_rows),
            mapping_context={"board_type": "industry", "board_code": board_code, "board_name": board_name, "source": "ths"},
        )

    def fetch_fund_flow(
        self, board_type: str, *, period: str = "即时", snapshot_at: datetime | None = None
    ) -> AkShareBoardResult:
        normalized = _board_type(board_type)
        ak = self.client or load_client()
        function_name = "stock_fund_flow_industry" if normalized == "industry" else "stock_fund_flow_concept"
        try:
            frame = getattr(ak, function_name)(symbol=period)
        except (ValueError, FileNotFoundError, FileExistsError, PermissionError):
            raise
        except Exception as exc:
            raise ProviderContractError(
                f"AkShare THS {normalized} fund-flow request failed for {period}",
                FailureClass.CONNECTION,
                retryable=True,
            ) from exc
        columns = tuple(str(item) for item in getattr(frame, "columns", ()))
        if period == "即时":
            expected = ("序号", "行业", "行业指数", "行业-涨跌幅", "流入资金", "流出资金", "净额", "公司家数", "领涨股", "领涨股-涨跌幅", "当前价")
            if columns != expected:
                raise ProviderContractError(
                    f"AkShare THS {normalized} immediate fund-flow columns changed: {columns}",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
            values = _values(frame)
            if any(len(row) != 11 for row in values):
                raise ProviderContractError(
                    "AkShare THS immediate fund-flow row width changed",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
            semantics = ("rank", "board_name", "index_value", "change_pct", "money_inflow", "money_outflow", "net_inflow", "company_count", "leader_stock_name", "leader_change_pct", "leader_price")
        else:
            expected = ("序号", "行业", "公司家数", "行业指数", "阶段涨跌幅", "流入资金", "流出资金", "净额")
            if columns != expected:
                raise ProviderContractError(
                    f"AkShare THS {normalized} period fund-flow columns changed: {columns}",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
            values = _values(frame)
            if any(len(row) != 8 for row in values):
                raise ProviderContractError(
                    "AkShare THS period fund-flow row width changed",
                    FailureClass.SCHEMA_CHANGED,
                    retryable=False,
                )
            semantics = ("rank", "board_name", "company_count", "index_value", "change_pct", "money_inflow", "money_outflow", "net_inflow")
        source_rows = tuple(dict(zip(columns, row)) for row in values)
        mapping = dict(self._mapping("board_fund_flow", "SDA-BOARD-003" if normalized == "industry" else "SDA-BOARD-004"))
        if period != "即时":
            # Preserve the existing period-method return contract; only immediate inputs are bound in YAML.
            mapping = {key: value for key, value in mapping.items() if key in semantics}
            mapping["change_pct"] = "阶段涨跌幅"
        rows = tuple({"board_type": normalized, "period": period, **Normalizer.map_fields(row, mapping)}
                     for row in source_rows)
        if not rows:
            raise ProviderContractError(
                f"AkShare THS {normalized} fund-flow returned no rows",
                FailureClass.TEMPORARY_EMPTY,
                retryable=False,
            )
        captured_at = snapshot_at or datetime.now(timezone.utc)
        rows = tuple({**row, "snapshot_at": captured_at.isoformat(), "source": "ths"} for row in rows)
        return AkShareBoardResult(
            rows,
            field_semantics=semantics,
            units=("money:source_display_unit_unconfirmed", "percentage:percent"),
            snapshot_at=captured_at,
            returned_first_key=str(rows[0]["rank"]),
            returned_last_key=str(rows[-1]["rank"]),
            source_rows=source_rows,
            mapping_context={"board_type": normalized, "period": period, "source": "ths"},
        )


def _values(frame: Any) -> list[tuple[Any, ...]]:
    iterrows = getattr(frame, "itertuples", None)
    if callable(iterrows):
        return [tuple(row) for row in frame.itertuples(index=False, name=None)]
    if isinstance(frame, (list, tuple)):
        return [tuple(item) if isinstance(item, (list, tuple)) else tuple(item.values()) for item in frame]
    return []


def _date(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    text = str(value)[:10].replace("/", "-")
    if len(text) != 10 or text[4] != "-" or text[7] != "-":
        raise ProviderContractError(
            f"AkShare THS industry index returned invalid date {value!r}",
            FailureClass.SCHEMA_CHANGED,
            retryable=False,
        )
    return text


def _board_type(value: str) -> str:
    normalized = str(value).lower()
    if normalized in {"industry", "行业"}:
        return "industry"
    if normalized in {"concept", "概念"}:
        return "concept"
    raise ValueError(f"unsupported board type: {value}")
