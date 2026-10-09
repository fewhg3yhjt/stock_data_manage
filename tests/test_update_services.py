from dataclasses import dataclass
from datetime import date

from stock_data_manage.domain import AssetType, Exchange
from stock_data_manage.service.instruments import SecurityMasterStore, stable_instrument_id
from stock_data_manage.service.instruments_update import SecurityMasterUpdater
from stock_data_manage.service.calendar import CalendarDay, TradingCalendarStore
from stock_data_manage.service.calendar_update import TradingCalendarUpdater
import pytest


def calendar_import_fixture(root, legacy=False):
    import json,inspect
    from datetime import datetime,timezone
    from pathlib import Path
    from types import SimpleNamespace
    from stock_data_manage.providers.sina.calendar import SinaTradingCalendarProvider
    from stock_data_manage.storage.raw import RawObjectStore
    from stock_data_manage.storage.integrity import file_hash
    now=datetime.now(timezone.utc)
    adapter=SinaTradingCalendarProvider();adapter_path=Path(inspect.getfile(type(adapter)))
    paths=[]
    for mode in ('live','replay'):
        work=root/mode;store=RawObjectStore(work/'raw')
        event=store.record_response(response=SimpleNamespace(content=b'{"synthetic_calendar":true}',status_code=200,
            headers={},encoding='utf-8'),url='https://fixture.invalid/calendar',method='GET',request_headers={},
            provider='sina',endpoint='trading_calendar',scope={},code_version='synthetic-fixture',mode=mode)
        rows=[{'trade_date':'2026-10-09','source':'sina'},{'trade_date':'2026-10-12','source':'sina'}]
        dates=work/'dates.json';dates.write_text(json.dumps(rows))
        report={'input_id':'ASTOCK-070','dataset':'trading_calendar','provider':'sina','endpoint':'trading_calendar',
            'parameters':{},'status':'candidate_complete','run_directory':str(work),'mode':mode,'row_count':2,
            'live_http_calls':int(mode=='live'),'adapter_version':adapter.capability_version,
            'sdk_dependency':{'original_source_sha256':'synthetic-fixture'},
            'code_files':[{'path':str(adapter_path),'sha256':file_hash(adapter_path)}],'config_files':[],
            'raw_manifest':{'path':'raw/manifest.ndjson','sha256':file_hash(store.root/'manifest.ndjson')},
            'output':{'path':'dates.json','sha256':file_hash(dates),'row_count':2,'source_response_hashes':[event['body_sha256']]}}
        saved=work/'report.json'
        if legacy and mode=='live':
            report.pop('run_directory')
            report['sdk_dependency']={'sha256':report['sdk_dependency']['original_source_sha256']}
            report['code_files'][0]['path']=str(root/'old/providers/sina/calendar.py')
            saved=work/'2026-10-09/input_report/sina/trading_calendar/report.json'
            saved.parent.mkdir(parents=True)
        saved.write_text(json.dumps(report));paths.append(saved)
    return paths,datetime.now(timezone.utc)


@pytest.mark.parametrize('legacy',[False,True])
def test_verified_calendar_import_preserves_official_conflicts_and_unknown_dates(tmp_path,legacy):
    paths,now=calendar_import_fixture(tmp_path,legacy)
    with TradingCalendarStore(str(tmp_path/'metadata.duckdb')) as store:
        store.upsert([CalendarDay(date(2026,10,12),False,'exchange',20,'existing evidence')])
        result=TradingCalendarUpdater(store,[]).update_from_input_report(paths[1],live_report_path=paths[0],
            start=date(2026,10,9),end=date(2026,10,12),now=now)
        assert store.is_trading_day(date(2026,10,9)) is True
        assert store.is_trading_day(date(2026,10,10)) is None
        assert store.is_trading_day(date(2026,10,12)) is False
        assert result.merge.conflicts
        assert 'revalidation_report' in store.all()[0].evidence


def test_calendar_import_rejects_stale_or_changed_evidence_before_writing(tmp_path):
    import json,pytest
    from datetime import timedelta
    paths,now=calendar_import_fixture(tmp_path)
    with TradingCalendarStore(str(tmp_path/'metadata.duckdb')) as store:
        updater=TradingCalendarUpdater(store,[])
        args=dict(live_report_path=paths[0],start=date(2026,10,9),end=date(2026,10,12))
        with pytest.raises(ValueError,match='expired'):
            updater.update_from_input_report(paths[1],now=now+timedelta(days=31),**args)
        current=json.loads(paths[1].read_text());current['code_files'][0]['sha256']='wrong'
        paths[1].write_text(json.dumps(current))
        with pytest.raises(ValueError,match='code or configuration'):
            updater.update_from_input_report(paths[1],now=now,**args)
        assert store.all()==()


@dataclass
class SecurityFixture:
    name: str
    rows: list[dict]
    error: Exception | None = None

    def fetch_security_list(self):
        if self.error:
            raise self.error
        return self.rows


@dataclass
class CalendarFixture:
    name: str
    priority: int
    days: list[CalendarDay]
    error: Exception | None = None

    def fetch_calendar(self, start, end):
        if self.error:
            raise self.error
        return self.days


def test_security_master_update_does_not_delist_when_one_source_is_empty(tmp_path) -> None:
    instrument_id = stable_instrument_id(Exchange.XSHG, "sh600519")
    with SecurityMasterStore(str(tmp_path / "security.duckdb")) as store:
        updater = SecurityMasterUpdater(
            store,
            [
                SecurityFixture(
                    "sina",
                    [{"symbol": "sh600519", "exchange": "XSHG", "asset_type": "stock", "name": "贵州茅台"}],
                ),
                SecurityFixture("eastmoney", []),
            ],
        )
        first = updater.update(effective_date=date(2026, 9, 13))
        assert first.source_errors == {}
        assert store.all()[0].instrument_id == instrument_id
        second = SecurityMasterUpdater(
            store,
            [SecurityFixture("sina", [], RuntimeError("temporarily missing"))],
        ).update(effective_date=date(2026, 9, 14))
        assert second.source_errors == {"sina": "temporarily missing"}
        assert store.all()[0].delist_date is None


def test_calendar_update_keeps_previous_day_when_source_fails(tmp_path) -> None:
    trade_day = date(2026, 9, 11)
    with TradingCalendarStore(str(tmp_path / "calendar.duckdb")) as store:
        updater = TradingCalendarUpdater(
            store,
            [CalendarFixture("exchange", 100, [CalendarDay(trade_day, True, "exchange", 100)])],
        )
        updater.update(start=trade_day, end=trade_day)
        failed = TradingCalendarUpdater(
            store,
            [CalendarFixture("sina", 10, [], RuntimeError("network"))],
        ).update(start=trade_day, end=trade_day)
        assert failed.source_errors == {"sina": "network"}
        assert store.is_trading_day(trade_day) is True


def test_calendar_update_does_not_create_holiday_from_single_empty_source(tmp_path) -> None:
    trade_day = date(2026, 9, 12)
    with TradingCalendarStore(str(tmp_path / "calendar.duckdb")) as store:
        result = TradingCalendarUpdater(
            store, [CalendarFixture("sina", 10, [])]
        ).update(start=trade_day, end=trade_day)
        assert result.merge.days == ()
        assert store.is_trading_day(trade_day) is None
