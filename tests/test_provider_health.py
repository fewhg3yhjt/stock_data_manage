from datetime import datetime, timedelta, timezone

from stock_data_manage.metadata import MetadataStore


KEY = {
    "provider": "eastmoney",
    "endpoint": "daily_history",
    "market": "XSHG",
    "asset_type": "stock",
    "dataset": "daily_bar",
    "capability_version": "v1",
}


def test_rate_limit_opens_only_concrete_capability_until_probe_succeeds(tmp_path) -> None:
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        health = metadata.record_provider_failure(
            **KEY,
            failure_class="rate_limited",
            error="HTTP 429",
            now=now,
            cooldown_seconds=300,
            http_status=429,
        )
        assert health.http_429_count == 1
        assert not metadata.provider_available(health, now + timedelta(seconds=10))
        assert not metadata.provider_available(health, now + timedelta(seconds=301))
        assert metadata.provider_available(health, now + timedelta(seconds=301), for_probe=True)

        recovered = metadata.record_provider_success(
            **KEY,
            now=now + timedelta(seconds=302),
            coverage=1.0,
            probe_ttl=timedelta(days=1),
        )
        assert recovered.consecutive_failures == 0
        assert recovered.last_probe_at == now + timedelta(seconds=302)
        assert metadata.provider_available(recovered, now + timedelta(seconds=303))


def test_ordinary_failures_open_at_threshold(tmp_path) -> None:
    now = datetime(2026, 9, 13, tzinfo=timezone.utc)
    with MetadataStore(tmp_path / "metadata.duckdb") as metadata:
        first = metadata.record_provider_failure(
            **KEY,
            failure_class="connection",
            error="disconnect",
            now=now,
            cooldown_seconds=60,
            failure_threshold=2,
        )
        assert metadata.provider_available(first, now)
        second = metadata.record_provider_failure(
            **KEY,
            failure_class="connection",
            error="disconnect",
            now=now + timedelta(seconds=1),
            cooldown_seconds=60,
            failure_threshold=2,
        )
        assert not metadata.provider_available(second, now + timedelta(seconds=2))

