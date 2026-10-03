from stock_data_manage.cli import main


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
