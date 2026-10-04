from __future__ import annotations

from typing import Any

from ..contracts import FailureClass, ProviderContractError


def load_client() -> Any:
    try:
        import akshare as ak
    except ImportError as exc:
        raise ProviderContractError(
            "AkShare dependency is not installed", FailureClass.CONNECTION, retryable=False
        ) from exc
    return ak


def source_symbol(symbol: str, *, keep_exchange: bool = False) -> str:
    text = str(symbol)
    if keep_exchange:
        return text
    return text[2:] if len(text) >= 8 and text[:2].lower() in {"sh", "sz", "bj"} else text
