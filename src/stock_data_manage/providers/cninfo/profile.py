"""Original CNInfo profile SDK, preserving generated request headers and field order."""
import json
from ..akshare.session import load_client
from ..contracts import InputFetchResult
from ..eastmoney.realtime import history_stock_identity


class CNInfoCompanyProfileProvider:
    capability_version = 'cninfo-company-profile-input-v1'
    input_hosts = ('https://webapi.cninfo.com.cn',)

    def __init__(self, client=None):
        self.client = client

    def fetch(self, *, code, source_responses):
        code = history_stock_identity(code)[0]
        self.client = self.client or load_client()
        frame = self.client.stock_profile_cninfo(symbol=code)
        responses = list(source_responses())
        if len(responses) != 1:
            raise ValueError('one CNInfo profile response is required')
        payload = json.loads(responses[0]['body'])
        if payload.get('resultcode') != 200 or payload.get('count') != 1 or len(payload.get('records', [])) != 1:
            raise ValueError('CNInfo profile business status/count changed')
        item = payload['records'][0]
        expected = ('ORGNAME','F001V','F002V','ASECCODE','ASECNAME','BSECCODE','BSECNAME','HSECCODE','HSECNAME',
            'F044V','MARKET','F032V','F003V','F007N','F010D','F006D','F011V','F012V','F013V','F014V','F004V',
            'F005V','F006V','F015V','F016V','F017V','F018V','F042V','F052V','F053V')
        columns = ('公司名称','英文名称','曾用简称','A股代码','A股简称','B股代码','B股简称','H股代码','H股简称',
            '入选指数','所属市场','所属行业','法人代表','注册资金','成立日期','上市日期','官方网站','电子邮箱','联系电话',
            '传真','注册地址','办公地址','邮政编码','主营业务','经营范围','机构简介')
        if tuple(item) != expected or item['ASECCODE'] != code or tuple(frame.columns) != columns or len(frame) != 1:
            raise ValueError('CNInfo profile source field order or security identity changed')
        rows = tuple(frame.astype(object).where(frame.notna(), None).to_dict('records'))
        if any(rows[0][label] != item[field] for label, field in zip(columns, expected)):
            raise ValueError('CNInfo SDK profile disagrees with source fields')
        return InputFetchResult(rows, source_rows=(item,), source_url=responses[0]['url'],
            mapping_context={'source_security_code': code, 'registered_capital_unit_verified': False,
                             'scope_meaning': 'one returned CNInfo company profile, not a complete F10 bundle'})
