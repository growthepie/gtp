import io
import re
import requests
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract

HTTP_HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36'}
## World Gold Council: page text contains e.g. "Total above-ground stock (end-Q2 2026): 222,600 tonnes" (updated quarterly)
WGC_GOLD_STOCK_URL = 'https://www.gold.org/goldhub/data/how-much-gold'
## FRED csv download (no API key), M2SL: US dollar M2 money supply in billions USD (monthly, revised retroactively)
FRED_M2_CSV_URL = 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=M2SL'

class AdapterReferenceData(AbstractAdapter):
    """
    Reference data for the ETH asset page that is published as plain web pages (no API).

    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Reference data", adapter_params, db_connector)
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        load_type:str - 'gold_stock' or 'm2'
    """
    def extract(self, load_params:dict):
        self.load_type = load_params['load_type']
        if self.load_type == 'gold_stock':
            df = self.extract_gold_stock()
        elif self.load_type == 'm2':
            df = self.extract_m2()
        else:
            raise ValueError(f"load_type {self.load_type} not supported for this adapter")
        print_extract(self.name, load_params, df.shape)
        return df

    def load(self, df:pd.DataFrame):
        tbl_name = 'eim_fact'
        upserted = self.db_connector.upsert_table(tbl_name, df)
        print_load(self.name, upserted, tbl_name)

    ## ----------------- Helper functions --------------------

    ## latest above-ground gold stock in tonnes, stored at the end date of the period it refers to (e.g. end-Q2 2026 -> 2026-06-30)
    def extract_gold_stock(self):
        r = requests.get(WGC_GOLD_STOCK_URL, headers=HTTP_HEADERS, timeout=60)
        r.raise_for_status()
        text = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', r.text))

        m = re.search(r'Total above-ground stock \(end-(?:Q([1-4]) )?(\d{4})\):\s*([\d,]+)\s*tonnes', text)
        if not m:
            raise ValueError(f"Above-ground gold stock not found on {WGC_GOLD_STOCK_URL}, page layout might have changed")
        quarter, year, tonnes = m.group(1), int(m.group(2)), float(m.group(3).replace(',', ''))
        date = (pd.Period(f'{year}Q{quarter}', freq='Q') if quarter else pd.Period(f'{year}', freq='Y')).end_time.date()
        print(f"...{self.name} - gold above-ground stock {tonnes:,.0f} t at {date}")

        df = pd.DataFrame([{'metric_key': 'gold_stock_tonnes', 'origin_key': 'gold', 'date': date, 'value': tonnes}])
        return df.set_index(['metric_key', 'origin_key', 'date'])

    ## full US dollar M2 history in USD, stored per month start (FRED convention), whole series upserted because FRED revises past months
    def extract_m2(self):
        r = requests.get(FRED_M2_CSV_URL, headers=HTTP_HEADERS, timeout=60)
        r.raise_for_status()
        ## FRED serves an HTML maintenance page with HTTP 200 during outages
        if not r.text.startswith('observation_date'):
            raise ValueError(f"FRED returned no csv for M2SL (maintenance?): {r.text[:100]!r}")
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = ['date', 'value']
        df['date'] = pd.to_datetime(df['date']).dt.date
        df['value'] = pd.to_numeric(df['value'], errors='coerce') * 1e9
        df = df.dropna()
        print(f"...{self.name} - M2 {df['value'].iloc[-1]/1e12:,.2f}T USD at {df['date'].iloc[-1]} ({len(df)} months)")

        df['metric_key'] = 'm2_usd'
        df['origin_key'] = 'usd'
        return df.set_index(['metric_key', 'origin_key', 'date'])
