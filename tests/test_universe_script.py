"""Contracts migrated from the supplied universe script; fixtures are not live evidence."""
from datetime import date
from functools import lru_cache
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import yaml

from stock_data_manage.config.loader import load_input_capabilities, load_input_field_contract
from stock_data_manage.providers.exchanges.security import BseSecurityListProvider, select_script_listing
from stock_data_manage.providers.exchanges.daily import ExchangeDailyProvider
from stock_data_manage.providers.baostock.industry import BaoStockIndustryMembershipProvider
from stock_data_manage.providers.baostock.session import read_rows
from stock_data_manage.providers.contracts import FailureClass, ProviderContractError
from stock_data_manage.routing.factory import build_input_provider
from stock_data_manage.quality.normalization import Normalizer

ROOT = Path(__file__).resolve().parents[1]
DAY = date(2026, 10, 9)
SCRIPT_KEYS = {'bs_calendar', 'bs_all_stock', 'bs_stock_basic', 'ak_sh_main_current',
    'ak_sh_star_current', 'ak_sz_current', 'ak_bj_current', 'ak_sh_delisted', 'ak_sz_delisted',
    'ak_sh_etf_history', 'ak_sz_etf_history', 'ak_sh_market_stats', 'ak_sz_market_stats'}


class PagedBaoStockResult:
    def __init__(self, pages, *, fail_after_page=None, error_code='0'):
        self.fields = ['code']
        self.pages = pages
        self.page_index = 0
        self.data = pages[0]
        self.cur_page_num = '1'
        self.per_page_count = len(pages[0])
        self.cur_row_num = 0
        self.error_code = '0'
        self.error_msg = ''
        self.fail_after_page = fail_after_page
        self.next_error_code = error_code

    def next(self):
        if self.cur_row_num < len(self.data):
            return True
        if len(self.data) < self.per_page_count:
            return False
        if self.fail_after_page == int(self.cur_page_num):
            self.error_code = self.next_error_code
            self.error_msg = 'simulated next-page failure' if self.next_error_code != '0' else ''
            return False
        if self.page_index + 1 >= len(self.pages):
            self.cur_page_num = str(int(self.cur_page_num) + 1)
            self.data = []
            return False
        self.page_index += 1
        self.cur_page_num = str(int(self.cur_page_num) + 1)
        self.data = self.pages[self.page_index]
        self.cur_row_num = 0
        return bool(self.data)

    def get_row_data(self):
        row = self.data[self.cur_row_num]
        self.cur_row_num += 1
        return [row]


def test_baostock_read_rows_accepts_later_pages_and_normal_short_final_page():
    result = PagedBaoStockResult([['a', 'b'], ['c']])
    result.per_page_count = 2
    assert [row['code'] for row in read_rows(result)] == ['a', 'b', 'c']


def test_baostock_read_rows_rejects_silent_full_page_truncation():
    result = PagedBaoStockResult([['a', 'b']], fail_after_page=1)
    result.per_page_count = 2
    with pytest.raises(ProviderContractError, match='without advancing') as error:
        read_rows(result)
    assert error.value.failure_class is FailureClass.CONNECTION
    assert error.value.retryable


def test_baostock_read_rows_preserves_explicit_later_page_error():
    result = PagedBaoStockResult([['a', 'b']], fail_after_page=1, error_code='10002007')
    result.per_page_count = 2
    with pytest.raises(ProviderContractError, match='10002007') as error:
        read_rows(result)
    assert error.value.failure_class is FailureClass.CONNECTION


def test_script_inventory_has_executable_yaml_and_mapping_contracts():
    document = yaml.safe_load((ROOT/'config/providers.yaml').read_text(encoding='utf-8'))
    contracts = {c.input_id: c for c in load_input_capabilities(ROOT/'config/providers.yaml')}
    represented = set()
    for input_id, item in document['input_capabilities'].items():
        keys = item.get('script_source_keys', [])
        if not keys:
            continue
        represented.update(keys)
        contract = contracts[input_id]
        provider = build_input_provider(contract, providers_path=ROOT/'config/providers.yaml', client=SimpleNamespace())
        assert callable(getattr(provider, contract.runtime_method))
        load_input_field_contract(ROOT/'config', contract)
        context = {'request': {'trade_date': DAY.isoformat()}, 'calendar': {'trading_dates': [DAY]}}
        assert contract.bind_parameters(context)['trade_date'] == DAY
    assert represented == SCRIPT_KEYS


def test_bse_listing_date_selection_preserves_uncertain_and_future_rows():
    rows = [{'证券代码': '920001', '证券简称': '已上市', '上市日期': '2026-10-08'},
            {'证券代码': '920157', '证券简称': '当日上市', '上市日期': '2026-10-09'},
            {'证券代码': '920003', '证券简称': '待上市', '上市日期': '2026-10-12'},
            {'证券代码': '920004', '证券简称': '日期缺失', '上市日期': None}]
    selected, excluded = select_script_listing(rows, kind='bj_current', trade_date=DAY)
    assert [r['stock_code'] for r in selected] == ['920001', '920157']
    assert len(excluded) == 2
    assert all(r['status'] == 'unknown' and r['date_basis'] == 'current_listing_date_candidate' for r in selected)


def test_termination_boundary_and_suspension_are_different():
    sz = {'证券代码': '000001', '证券简称': '测试', '上市日期': '1991-01-01', '终止上市日期': '2026-10-09'}
    assert not select_script_listing([sz], kind='sz_delisted', trade_date=DAY)[0]
    assert select_script_listing([sz], kind='sz_delisted', trade_date=date(2026, 10, 8))[0][0]['delist_date'] == DAY
    sh = {'公司代码': '600001', '公司简称': '测试', '上市日期': '1991-01-01', '暂停上市日期': '2026-10-10'}
    row = select_script_listing([sh], kind='sh_delisted', trade_date=DAY)[0][0]
    assert row['delist_date'] is None and row['date_basis'] == 'suspension_interval_candidate'


def test_sz_native_a_share_fields_and_missing_termination_candidate():
    row = {'A股代码': '302132', 'A股简称': '测试', 'A股上市日期': '2026-01-01'}
    assert select_script_listing([row], kind='sz_current', trade_date=DAY)[0][0]['stock_code'] == '302132'
    old = {'证券代码': '000001', '证券简称': '测试', '上市日期': '1991-01-01', '终止上市日期': None}
    selected = select_script_listing([old], kind='sz_delisted', trade_date=DAY)[0][0]
    assert selected['date_basis'] == 'termination_unknown_candidate' and selected['delist_date'] is None


def test_bse_native_function_cache_is_bypassed_and_pages_checked():
    calls = []
    @lru_cache()
    def listing():
        calls.append(True)
        return pd.DataFrame([{'证券代码': '920001', '证券简称': '测试', '上市日期': '2026-01-01'}])
    listing()
    def responses(total=1):
        block = {'number': 0, 'totalPages': 1, 'totalElements': total, 'content': [{'id': 1}]}
        response = {'body': ('callback('+json.dumps([block])+');')[:-1].encode(),
                    'encoding': 'utf-8', 'url': 'https://www.bse.cn/nqxxController/nqxxCnzq.do'}
        return [response, response]
    provider = BseSecurityListProvider(client=SimpleNamespace(stock_info_bj_name_code=listing))
    result = provider.fetch_listing_history(trade_date=DAY, source_responses=responses)
    assert len(calls) == 2 and len(result.rows) == 1
    with pytest.raises(ValueError, match='pagination'):
        provider.fetch_listing_history(trade_date=DAY, source_responses=lambda: responses(2))


def test_market_statistics_preserve_original_units_and_date_parameter():
    calls = []
    def statistics(**kwargs):
        calls.append(kwargs)
        return pd.DataFrame([{'单日情况': '总成交金额', '股票': '12.34亿元', '主板A': 10}])
    provider = ExchangeDailyProvider()
    provider.client = SimpleNamespace(stock_sse_deal_daily=statistics)
    result = provider.fetch_market_statistics(kind='sh', trade_date=DAY,
        source_responses=lambda: [{'url': 'https://query.sse.com.cn/sseQuery/commonQuery.do'}])
    assert calls == [{'date': '20261009'}]
    assert json.loads(result.rows[0]['source_values'])['股票'] == '12.34亿元'


def test_baostock_script_order_keeps_stock_and_index_types_and_archives_before_selection():
    calls, archived = [], []
    def result(rows):
        fields = list(rows[0]); position = -1
        def next_row():
            nonlocal position
            position += 1
            return position < len(rows)
        return SimpleNamespace(fields=fields, error_code='0', error_msg='', next=next_row,
            get_row_data=lambda: [rows[position][f] for f in fields])
    def query(name, rows):
        def run(**kwargs):
            calls.append((name, kwargs)); return result(rows)
        return run
    session = lambda name: (calls.append((name, {})) or SimpleNamespace(error_code='0', error_msg=''))
    basic = [{'code':'sh.600001','code_name':'测试','ipoDate':'1991-01-01','outDate':'','type':'1','status':'1'},
             {'code':'sh.000001','code_name':'指数','ipoDate':'1991-01-01','outDate':'','type':'2','status':'1'},
             {'code':'sz.302132','code_name':'保留范围','ipoDate':'2026-10-09','outDate':'','type':'1','status':'1'}]
    client = SimpleNamespace(login=lambda: session('login'), logout=lambda: session('logout'),
        query_trade_dates=query('query_trade_dates', [{'calendar_date':'2026-10-09','is_trading_day':'1'}]),
        query_all_stock=query('query_all_stock', [{'code':'sh.600001','code_name':'测试','tradeStatus':'1'},
            {'code':'sh.000001','code_name':'指数','tradeStatus':'1'}]),
        query_stock_basic=query('query_stock_basic', basic))
    archive = SimpleNamespace(store_source_payload=lambda payload, **kwargs: archived.append((payload, kwargs)))
    fetched = BaoStockIndustryMembershipProvider(client=client).fetch_universe(trade_date=DAY, raw_archive=archive)
    assert [name for name, _ in calls] == ['login','query_trade_dates','query_all_stock','query_stock_basic','logout']
    assert len(archived) == 3
    assert {r['stock_code'] for r in fetched.rows} == {'600001','000001','302132'}
    assert next(r for r in fetched.rows if r['stock_code'] == '000001')['asset_type'] == 'index'
    assert fetched.mapping_context['asset_type_mapping'] == {'1': 'stock', '2': 'index'}
    assert fetched.mapping_context['historical_universe_verified_complete'] is False
    assert fetched.rows[0]['script_sources'] == 'bs_stock_basic;bs_all_stock'
