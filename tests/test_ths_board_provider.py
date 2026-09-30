from io import StringIO

import pandas as pd

from stock_data_manage.providers.ths import ThsBoardProvider


class FakeResponse:
    status_code = 200
    url = "https://q.10jqka.com.cn/thshy/detail/code/881270/"
    text = "<html></html>"


def test_ths_board_provider_maps_member_table(monkeypatch):
    table = pd.DataFrame([{"序号": 1, "代码": 2913, "名称": "奥士康", "现价": 92.3}])
    monkeypatch.setattr(pd, "read_html", lambda source: [table])
    monkeypatch.setattr(
        "stock_data_manage.providers.ths.boards.requests.get",
        lambda *args, **kwargs: FakeResponse(),
    )
    result = ThsBoardProvider().fetch_members("industry", "881270")
    assert result.board_type == "industry"
    assert result.rows[0]["stock_code"] == "002913"
    assert result.rows[0]["stock_name"] == "奥士康"


def test_ths_board_provider_rejects_unknown_board_type():
    try:
        ThsBoardProvider().fetch_members("unknown", "x")
    except ValueError as exc:
        assert "unsupported" in str(exc)
    else:
        raise AssertionError("unknown board type should be rejected")
