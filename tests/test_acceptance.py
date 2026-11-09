import json
from dataclasses import replace
from pathlib import Path
import pytest

from stock_data_manage.worker.acceptance import run_offline_acceptance
from stock_data_manage.cli import main


def test_offline_acceptance_report_covers_replay_capacity_and_recovery(tmp_path) -> None:
    report = run_offline_acceptance(tmp_path / "run", test_run_id="m1-test")
    assert report.test_run_id == "m1-test"
    assert report.gates["A"].startswith("NOT_RUN")
    assert report.gates["C"] == "PARTIAL_COMPONENT_ONLY"
    assert report.gates["D"] == "PARTIAL_COMPONENT_ONLY"
    assert report.gates["E"] == "PARTIAL_ESTIMATE_ONLY"
    assert all(check["status"] == "PASS" for check in report.checks.values())
    assert len(report.checks["storage_replay"]["evidence"]) == 20
    assert report.scope["data_origin"] == "synthetic"
    assert "implementation" not in report.to_dict()
    assert report.scope["replay_instruments"] == ["XSHG:600519"]
    assert report.metrics["replay_weekdays"] == 20
    assert report.metrics["replay_partition_no_ops"] == 20
    assert report.metrics["watchlist_symbols"] == 200
    assert report.metrics["daily_table_transaction_rollback"] is True
    assert report.metrics["daily_table_retry_written_count"] == 1
    assert report.gates["B"].startswith("NOT_RUN")
    assert report.gates["F"].startswith("NOT_STARTED")


def test_offline_acceptance_cli_can_write_report(tmp_path, capsys) -> None:
    output = tmp_path / "acceptance.json"
    assert main(["acceptance-offline", "--root", str(tmp_path / "run"), "--output", str(output)]) == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["conclusion"] == "OFFLINE_COMPONENT_CHECKS_PASS_PROJECT_GATES_PENDING"
    assert "replay_weekdays" in payload["metrics"]
    assert "test_run_id" in capsys.readouterr().out


def test_offline_acceptance_rejects_failed_idempotency(tmp_path, monkeypatch):
    from stock_data_manage.storage.daily import DailyBarStore
    original = DailyBarStore.publish_partition
    def changed(*args, **kwargs):
        return replace(original(*args, **kwargs), idempotent=False)
    monkeypatch.setattr(DailyBarStore, "publish_partition", changed)
    report = run_offline_acceptance(tmp_path)
    assert report.checks["storage_replay"]["actual"]["no_op_partitions"] == 0
    assert report.checks["storage_replay"]["status"] == "FAIL"
    assert report.gates["C"] == "FAIL_COMPONENT_CHECK"
    assert report.conclusion == "OFFLINE_COMPONENT_CHECKS_FAILED"


def test_offline_acceptance_rejects_invalid_recovery(tmp_path, monkeypatch):
    from stock_data_manage.storage.daily import DailyBarStore, DailyPublicationResult
    original = DailyBarStore.publish_partition
    skipped = set()
    def changed(*args, **kwargs):
        task_id = kwargs["task_id"]
        if "-transaction-retry-" in task_id and task_id not in skipped:
            skipped.add(task_id)
            return DailyPublicationResult("not-written", False, 0, 0, 0, (), 0, {}, 0)
        return original(*args, **kwargs)
    monkeypatch.setattr(DailyBarStore, "publish_partition", changed)
    report = run_offline_acceptance(tmp_path)
    assert report.checks["daily_table_transaction_recovery"]["status"] == "FAIL"
    assert report.gates["D"] == "FAIL_COMPONENT_CHECK"
    assert report.conclusion == "OFFLINE_COMPONENT_CHECKS_FAILED"


def test_offline_acceptance_does_not_hide_programming_errors(tmp_path, monkeypatch):
    from stock_data_manage.storage.daily import DailyBarStore
    original = DailyBarStore.publish_partition
    calls = {}
    def changed(*args, **kwargs):
        task_id = kwargs["task_id"]
        if "-transaction-retry-" in task_id:
            calls[task_id] = calls.get(task_id, 0) + 1
            if calls[task_id] == 2:
                raise RuntimeError("unexpected programming failure")
        return original(*args, **kwargs)
    monkeypatch.setattr(DailyBarStore, "publish_partition", changed)
    with pytest.raises(RuntimeError, match="unexpected programming failure"):
        run_offline_acceptance(tmp_path)


def test_offline_acceptance_cli_returns_nonzero_for_failed_check(tmp_path, monkeypatch, capsys):
    import stock_data_manage.cli as cli
    report = run_offline_acceptance(tmp_path / "run")
    failed = replace(report, checks={"storage_replay": {"status": "FAIL"}},
                     conclusion="OFFLINE_COMPONENT_CHECKS_FAILED")
    monkeypatch.setattr(cli, "run_offline_acceptance", lambda *args, **kwargs: failed)
    assert main(["acceptance-offline", "--root", str(tmp_path / "unused")]) == 2
    assert json.loads(capsys.readouterr().out)["conclusion"] == failed.conclusion


def test_repeated_component_runs_preserve_prior_publication_receipts(tmp_path):
    from stock_data_manage.storage.metadata import MetadataStore
    first = run_offline_acceptance(tmp_path, test_run_id="first-run")
    first_evidence = first.checks["daily_table_transaction_recovery"]["evidence"]
    descriptor_ref = json.loads(first_evidence["candidate_ref"])
    descriptor = Path(descriptor_ref["path"])
    original_bytes = descriptor.read_bytes()
    with MetadataStore(tmp_path / "metadata.sqlite3", read_only=True) as metadata:
        original_receipts = metadata.connection.execute(
            "SELECT publication_id,request_hash FROM daily_publication WHERE task_id=?",
            [first_evidence["task_id"]]).fetchall()
    second = run_offline_acceptance(tmp_path, test_run_id="first-run")
    second_evidence = second.checks["daily_table_transaction_recovery"]["evidence"]
    assert second_evidence["task_id"] != first_evidence["task_id"]
    assert descriptor.read_bytes() == original_bytes
    with MetadataStore(tmp_path / "metadata.sqlite3", read_only=True) as metadata:
        assert metadata.connection.execute(
            "SELECT publication_id,request_hash FROM daily_publication WHERE task_id=?",
            [first_evidence["task_id"]]).fetchall() == original_receipts
    assert all(check["status"] == "PASS" for check in second.checks.values())
