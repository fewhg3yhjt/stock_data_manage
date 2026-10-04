from datetime import datetime
import math

from ..akshare.session import load_client
from ..contracts import InputFetchResult


class MofcomSocialFinancingProvider:
    """Keep the successful bodyless POST, SDK TLS adapter, sorting and source labels."""
    capability_version = "mofcom-social-financing-input-v1"
    input_hosts = ("https://data.mofcom.gov.cn",)
    raw_fields = ("date","ndbab","entrustloan","forcloan","rmblaon","bibae","tiosfs","sfinfe","trustloan")
    sdk_fields = ("月份","社会融资规模增量","其中-人民币贷款","其中-委托贷款外币贷款","其中-委托贷款",
                  "其中-信托贷款","其中-未贴现银行承兑汇票","其中-企业债券","其中-非金融企业境内股票融资")

    def __init__(self,client=None):
        self.client = client

    def fetch(self, *, source_payloads):
        self.client = self.client or load_client()
        frame = self.client.macro_china_shrzgm()
        payloads = list(source_payloads())
        if len(payloads) != 1:
            raise RuntimeError("one successful social-financing response is required")
        items = payloads[0]
        if not isinstance(items,list) or not items or any(not isinstance(row,dict) or tuple(row) != self.raw_fields for row in items):
            raise RuntimeError("social-financing positional source schema changed")
        if len(items) != len(frame) or tuple(frame.columns) != self.sdk_fields:
            raise RuntimeError("social-financing SDK schema or count changed")
        periods=[]
        for row in items:
            if not isinstance(row["date"],str) or len(row["date"])!=6:
                raise RuntimeError("social-financing month changed")
            datetime.strptime(row["date"],"%Y%m")
            periods.append(row["date"])
            for key in self.raw_fields[1:]:
                value=row[key]
                if value is not None and (isinstance(value,bool) or not math.isfinite(float(value))):
                    raise RuntimeError("social-financing source number changed")
        if len(set(periods)) != len(periods):
            raise RuntimeError("duplicate social-financing month")
        import pandas as pd
        frame=frame.astype(object).where(pd.notna(frame),None)
        rows=tuple({**row,"statistical_month":datetime.strptime(row["月份"],"%Y%m").strftime("%Y-%m")}
                   for row in frame.to_dict(orient="records"))
        return InputFetchResult(rows,source_rows=tuple(items),source_url="https://data.mofcom.gov.cn/datamofcom/front/gnmy/shrzgmQuery")
