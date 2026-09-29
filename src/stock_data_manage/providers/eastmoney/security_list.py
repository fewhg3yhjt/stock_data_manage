from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..contracts import EndpointContract, FailureClass, ProviderContractError
from ..transport import HttpTransport


@dataclass(slots=True)
class EastMoneySecurityListProvider:
    """EastMoney paged security-list adapter for Security Master validation."""

    transport: HttpTransport
    name: str = "eastmoney"
    endpoint: str = "security_list"
    capability_version: str = "eastmoney-security-list-v1"
    timeout_seconds: float = 20.0
    page_size: int = 100
    max_pages: int = 200
    url: str = "https://push2delay.eastmoney.com/api/qt/clist/get"
    filters: tuple[str, ...] = (
        "b:MK0021,b:MK0022,b:MK0023,b:MK0024,b:MK0827",
        "b:MK0404,b:MK0405,b:MK0406,b:MK0407",
        "m:1+t:2,m:1+t:23,m:0+t:6,m:0+t:80",
    )

    def fetch_security_list(self) -> list[Mapping[str, Any]]:
        rows: dict[tuple[str, str], dict[str, Any]] = {}
        for filter_expression in self.filters:
            for page in range(1, self.max_pages + 1):
                payload = self._fetch_page(page, filter_expression)
                data = payload.get("data") or {}
                diff = data.get("diff") or []
                if not isinstance(diff, list):
                    raise ProviderContractError(
                        "EastMoney security list rows changed",
                        FailureClass.SCHEMA_CHANGED,
                        retryable=False,
                    )
                for row in diff:
                    normalized = self._normalize_row(row)
                    if normalized is not None:
                        rows[(normalized["exchange"], normalized["symbol"])] = normalized
                total = int(data.get("total") or len(rows))
                if not diff or page * self.page_size >= total or len(diff) < self.page_size:
                    break
        return [rows[key] for key in sorted(rows)]

    def _fetch_page(self, page: int, filter_expression: str) -> Mapping[str, Any]:
        response = self.transport.get(
            self.url,
            params={
                "pn": str(page),
                "pz": str(self.page_size),
                "po": "1",
                "np": "1",
                "fltt": "2",
                "invt": "2",
                "fid": "f3",
                "fs": filter_expression,
                "fields": "f12,f13,f14,f18,f23",
            },
            timeout_seconds=self.timeout_seconds,
        )
        payload = EndpointContract(frozenset()).parse_json(response)
        if not isinstance(payload, dict) or payload.get("data") is None:
            raise ProviderContractError(
                "EastMoney security list response envelope changed",
                FailureClass.SCHEMA_CHANGED,
                retryable=False,
            )
        return payload

    @staticmethod
    def _normalize_row(row: object) -> dict[str, Any] | None:
        if not isinstance(row, dict):
            return None
        code = str(row.get("f12") or "").zfill(6)
        market_code = str(row.get("f13") or "")
        name = str(row.get("f14") or "").strip()
        if not code.isdigit() or market_code not in {"0", "1", "2"}:
            return None
        exchange = {"1": "XSHG", "0": "XSHE", "2": "BSE"}.get(market_code)
        if exchange is None:
            return None
        asset_type = _asset_type(code, name)
        prefix = {"XSHG": "sh", "XSHE": "sz", "BSE": "bj"}[exchange]
        return {
            "symbol": f"{prefix}{code}",
            "exchange": exchange,
            "asset_type": asset_type,
            "name": name or None,
            "status": "active",
            "source": "eastmoney",
        }


def _asset_type(code: str, name: str) -> str:
    if "ETF" in name.upper() or code.startswith(("159", "510", "511", "512", "513", "515", "516", "518", "560", "588")):
        return "etf"
    if "LOF" in name.upper() or code.startswith(("16", "50")):
        return "lof"
    if code.startswith(("000", "399", "899")) and ("指数" in name or "指" in name):
        return "index"
    return "stock"
