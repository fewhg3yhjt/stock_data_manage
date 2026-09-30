import pandas as pd

from stock_data_manage.providers.ths import ThsBoardProvider
from stock_data_manage.providers.contracts import FailureClass, ProviderContractError
from stock_data_manage.storage.raw import RawObjectStore


class FakeResponse:
    status_code = 200
    url = "https://q.10jqka.com.cn/thshy/detail/code/881270/"
    text = "<html></html>"


def _response(url: str, text: str) -> FakeResponse:
    response = FakeResponse()
    response.url = url
    response.text = text
    return response


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


def test_ths_board_provider_enforces_configured_request_interval(monkeypatch):
    table = pd.DataFrame([{"序号": 1, "代码": 2913, "名称": "奥士康"}])
    monkeypatch.setattr(pd, "read_html", lambda source: [table])
    monkeypatch.setattr(
        "stock_data_manage.providers.ths.boards.requests.get",
        lambda *args, **kwargs: FakeResponse(),
    )
    now = iter((0.0, 0.0, 1.0, 4.0))
    sleeps = []
    provider = ThsBoardProvider(
        request_interval_seconds=3,
        clock=lambda: next(now),
        sleep=sleeps.append,
    )
    provider.fetch_members("industry", "881270")
    provider.fetch_members("industry", "881270")
    assert sleeps == [2.0]


def test_ths_board_provider_parses_board_list_from_main_table(monkeypatch):
    html = """
    <table class="m-table m-pager-table">
      <tr><th>板块</th></tr>
      <tr><td><a href="http://q.10jqka.com.cn/thshy/detail/code/881121/">半导体</a></td></tr>
      <tr><td><a href="http://q.10jqka.com.cn/thshy/detail/code/881273/">白酒</a></td></tr>
    </table>
    <div class="m-pager"><span class="page_info">1/2</span></div>
    <a href="http://q.10jqka.com.cn/thshy/detail/code/889999/">导航重复链接</a>
    """
    monkeypatch.setattr(
        "stock_data_manage.providers.ths.boards.requests.get",
        lambda url, **kwargs: _response(url, html),
    )
    result = ThsBoardProvider().fetch_board_list("industry")
    assert result.total_pages == 2
    assert result.rows == (
        {"board_type": "industry", "board_code": "881121", "board_name": "半导体", "source": "ths"},
        {"board_type": "industry", "board_code": "881273", "board_name": "白酒", "source": "ths"},
    )


def test_ths_board_provider_classifies_login_redirect(monkeypatch):
    response = _response(
        "https://q.10jqka.com.cn/account/login/",
        '<script>location.href="//upass.10jqka.com.cn/login?redir=https://q.10jqka.com.cn"</script>',
    )
    monkeypatch.setattr(
        "stock_data_manage.providers.ths.boards.requests.get",
        lambda *args, **kwargs: response,
    )
    try:
        ThsBoardProvider().fetch_board_list("concept")
    except ProviderContractError as exc:
        assert exc.failure_class is FailureClass.AUTH_REQUIRED
        assert not exc.retryable
    else:
        raise AssertionError("login redirect should be classified")


def test_ths_board_provider_passes_qstock_cookie(monkeypatch):
    captured = {}

    def request(url, **kwargs):
        captured.update(kwargs)
        return _response(url, '<span class="page_info">1/1</span>')

    monkeypatch.setattr("stock_data_manage.providers.ths.boards.requests.get", request)
    provider = ThsBoardProvider(cookie_v="cookie-value")
    try:
        provider.fetch_board_list("concept")
    except ProviderContractError:
        # The fixture has no board rows; only the request headers are under test.
        pass
    assert captured["headers"]["Cookie"] == "v=cookie-value"


def test_ths_board_provider_fetches_and_deduplicates_member_pages(monkeypatch):
    tables = {
        "/": pd.DataFrame([{"代码": 2913, "名称": "奥士康"}, {"代码": 3001, "名称": "测试一"}]),
        "/page/2/": pd.DataFrame([{"代码": 3001, "名称": "测试一"}, {"代码": 6000, "名称": "测试二"}]),
    }
    htmls = {
        "/": '<span class="page_info">1/2</span>',
        "/page/2/": '<span class="page_info">2/2</span>',
    }
    monkeypatch.setattr(
        "stock_data_manage.providers.ths.boards.requests.get",
        lambda url, **kwargs: _response(url, htmls["/page/2/"] if "/page/2/" in url else htmls["/"]),
    )
    parsed_tables = iter((tables["/"], tables["/page/2/"]))
    monkeypatch.setattr(pd, "read_html", lambda source: [next(parsed_tables)])
    result = ThsBoardProvider().fetch_all_members("industry", "881270")
    assert result.total_pages == 2
    assert [row["stock_code"] for row in result.rows] == ["002913", "003001", "006000"]


def test_ths_board_snapshot_can_be_committed_to_raw_store(tmp_path):
    from datetime import datetime, timezone

    snapshot = {
        "board_type": "industry",
        "board_code": "881270",
        "rows": [
            {
                "board_type": "industry",
                "board_code": "881270",
                "stock_code": "002913",
                "stock_name": "奥士康",
                "source": "ths",
            }
        ],
    }
    reference = RawObjectStore(tmp_path / "raw").write_json(
        snapshot,
        dataset="industry_board",
        provider="ths",
        endpoint="board_members",
        fetched_at=datetime(2026, 9, 30, tzinfo=timezone.utc),
        attempt_id="board-881270-page-1",
    )
    assert RawObjectStore.verify(reference)
