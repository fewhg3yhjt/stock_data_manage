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

