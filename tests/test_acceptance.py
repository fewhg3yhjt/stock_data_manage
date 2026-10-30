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
    assert report.implementation and report.scope["data_origin"] == "synthetic"
    assert report.scope["replay_instruments"] == ["XSHG:600519"]
    assert report.metrics["replay_weekdays"] == 20
    assert report.metrics["replay_partition_no_ops"] == 20
    assert report.metrics["watchlist_symbols"] == 200
    assert report.metrics["recovery_promoted_temporary_partitions"] == 1
    assert report.metrics["recovery_invalid_final_partitions"] == 0
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
    from stock_data_manage.storage.parquet import CanonicalPartitionStore
    original = CanonicalPartitionStore.publish
    def changed(*args, **kwargs):
        return replace(original(*args, **kwargs), no_op=False)
    monkeypatch.setattr(CanonicalPartitionStore, "publish", changed)
    report = run_offline_acceptance(tmp_path)
    assert report.checks["storage_replay"]["actual"]["no_op_partitions"] == 0
    assert report.checks["storage_replay"]["status"] == "FAIL"
    assert report.gates["C"] == "FAIL_COMPONENT_CHECK"
    assert report.conclusion == "OFFLINE_COMPONENT_CHECKS_FAILED"


def test_offline_acceptance_rejects_invalid_recovery(tmp_path, monkeypatch):
    from stock_data_manage.worker.recovery import RecoveryScanner
    original = RecoveryScanner.recover
    def changed(*args, **kwargs):
        return replace(original(*args, **kwargs), invalid_final_partitions=("broken-partition",))
    monkeypatch.setattr(RecoveryScanner, "recover", changed)
    report = run_offline_acceptance(tmp_path)
    assert report.checks["publication_recovery"]["status"] == "FAIL"
    assert report.gates["D"] == "FAIL_COMPONENT_CHECK"
    assert report.conclusion == "OFFLINE_COMPONENT_CHECKS_FAILED"


def test_offline_acceptance_does_not_hide_programming_errors(tmp_path, monkeypatch):
    from stock_data_manage.storage.parquet import CanonicalPartitionStore
    original = CanonicalPartitionStore.publish
    def changed(*args, **kwargs):
        if kwargs["run_id"].endswith("-recovery"):
            raise RuntimeError("unexpected programming failure")
        return original(*args, **kwargs)
    monkeypatch.setattr(CanonicalPartitionStore, "publish", changed)
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


def test_repeated_component_runs_preserve_prior_fault_evidence(tmp_path):
    first = run_offline_acceptance(tmp_path, test_run_id="first-run")
    evidence = Path(first.checks["publication_recovery"]["evidence"]["root"])
    original = {path.relative_to(evidence): path.read_bytes() for path in evidence.rglob("*") if path.is_file()}
    second = run_offline_acceptance(tmp_path, test_run_id="first-run")
    assert second.checks["publication_recovery"]["evidence"]["root"] != str(evidence)
    assert all(check["status"] == "PASS" for check in second.checks.values())
    assert {path.relative_to(evidence): path.read_bytes() for path in evidence.rglob("*") if path.is_file()} == original
