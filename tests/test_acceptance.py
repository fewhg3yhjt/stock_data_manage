import json

from stock_data_manage.worker.acceptance import run_offline_acceptance
from stock_data_manage.cli import main


def test_offline_acceptance_report_covers_replay_capacity_and_recovery(tmp_path) -> None:
    report = run_offline_acceptance(tmp_path / "run", test_run_id="m1-test")
    assert report.test_run_id == "m1-test"
    assert report.gates["A"] == "PASS"
    assert report.gates["C"] == "PASS_OFFLINE"
    assert report.gates["D"] == "PASS_OFFLINE"
    assert report.gates["E"] == "PASS_OFFLINE"
    assert report.metrics["replay_trading_days"] == 20
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
    assert payload["conclusion"].startswith("M1_OFFLINE")
    assert "replay_trading_days" in payload["metrics"]
    assert "test_run_id" in capsys.readouterr().out
