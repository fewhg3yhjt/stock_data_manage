from stock_data_manage.cli import main


def test_collect_input_context_file_merges_explicit_flags_and_preserves_dependency(tmp_path, monkeypatch, capsys):
    import json
    from datetime import date
    saved = tmp_path / "context.json"
    saved.write_text(json.dumps({"request": {"board_name": "半导体", "start_date": "2026-09-01"},
                                 "dependency": {"board_code": "881121"}, "config": {"period": "即时"}}), encoding="utf-8")
    calls = []
    def collector(**kwargs):
        calls.append(kwargs)
        return {"status": "candidate_complete"}
    monkeypatch.setattr("stock_data_manage.pipeline.inputs.collect_input", collector)
    assert main(["collect-input", "--input", "SDA-BOARD-002", "--context-file", str(saved),
                 "--start-date", "2026-09-02", "--end-date", "2026-10-02", "--output-root", str(tmp_path / "out")]) == 0
    context = calls[0]["context"]
    assert context["request"] == {"board_name": "半导体", "start_date": date(2026, 9, 2), "end_date": date(2026, 10, 2)}
    assert context["dependency"]["board_code"] == "881121"
    assert context["metadata"]["context_path"] == str(saved.resolve())


def test_collect_input_rejects_malformed_context_before_execution(tmp_path):
    import pytest
    saved = tmp_path / "context.json"
    saved.write_text('{"request": ["半导体"]}', encoding="utf-8")
    with pytest.raises(SystemExit) as exc:
        main(["collect-input", "--input", "SDA-BOARD-002", "--context-file", str(saved), "--output-root", str(tmp_path / "out")])
    assert exc.value.code == 2


def test_recover_cli_returns_success_for_empty_store(tmp_path, capsys) -> None:
    code = main(
        [
            "recover",
            "--canonical-root",
            str(tmp_path / "canonical"),
            "--metadata",
            str(tmp_path / "metadata.duckdb"),
        ]
    )
    assert code == 0
    assert '"promoted_temporary_partitions": 0' in capsys.readouterr().out




def test_due_input_cli_saves_plan_and_filters_explicit_closed_calendar_days(tmp_path, capsys):
    import json
    calendar = tmp_path / "calendar.json"
    calendar.write_text(json.dumps([{"trade_date": "2026-09-30", "is_trading_day": True},
                                    {"trade_date": "2026-10-03", "is_trading_day": False}]), encoding="utf-8")
    code = main(["collect-due-inputs", "--now", "2026-09-30T15:10:00+08:00", "--calendar-file", str(calendar),
                 "--output-root", str(tmp_path / "plans")])
    report = json.loads(capsys.readouterr().out)
    assert code == 0 and report["jobs"][0]["input_id"] == "ASTOCK-001"
    assert report["jobs"][0]["status"] == "disabled"
    assert report["scope_context"]["trading_dates"] == ["2026-09-30"]
    assert report["profiles"][0]["frequency_unit"] == "day"
    assert report["production_writes"] == 0
    assert main(["collect-due-inputs", "--now", "2026-10-03T15:10:00+08:00", "--calendar-file", str(calendar),
                 "--output-root", str(tmp_path / "closed")]) == 0
    assert json.loads(capsys.readouterr().out)["jobs"] == []



def test_default_collect_input_passes_unified_root_without_validation_output(tmp_path, monkeypatch):
    calls=[]
    def collector(**kwargs):
        calls.append(kwargs)
        return {"status":"candidate_complete"}
    monkeypatch.setattr("stock_data_manage.pipeline.inputs.collect_input",collector)
    assert main(["collect-input","--input","ASTOCK-002-daily","--data-root",str(tmp_path/"data")])==0
    assert calls[0]["output_root"] is None and calls[0]["data_root"]==tmp_path/"data"


def test_explicit_recovery_does_not_require_project_config(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert main(["recover","--canonical-root",str(tmp_path/"canonical"),"--metadata",str(tmp_path/"meta.duckdb")])==0
    assert '"invalid_final_partitions": []' in capsys.readouterr().out
