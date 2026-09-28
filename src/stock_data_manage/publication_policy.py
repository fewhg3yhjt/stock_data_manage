from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import yaml


@dataclass(frozen=True, slots=True)
class PublicationPolicy:
    max_missing_ratio: Decimal
    max_missing_count: int

    def __post_init__(self) -> None:
        if not Decimal("0") <= self.max_missing_ratio <= Decimal("1"):
            raise ValueError("max_missing_ratio must be between 0 and 1")
        if self.max_missing_count < 0:
            raise ValueError("max_missing_count cannot be negative")

    def allows(self, *, expected_count: int, actual_count: int) -> bool:
        if expected_count <= 0:
            raise ValueError("expected_count must be positive")
        if not 0 <= actual_count <= expected_count:
            raise ValueError("actual_count must be between 0 and expected_count")
        missing_count = expected_count - actual_count
        missing_ratio = Decimal(missing_count) / Decimal(expected_count)
        return (
            missing_count <= self.max_missing_count
            and missing_ratio <= self.max_missing_ratio
        )


def load_publication_policy(path: str | Path, dataset: str) -> PublicationPolicy:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    publication = payload["datasets"][dataset]["publish"]
    return PublicationPolicy(
        max_missing_ratio=Decimal(str(publication["max_missing_ratio"])),
        max_missing_count=int(publication["max_missing_count"]),
    )
