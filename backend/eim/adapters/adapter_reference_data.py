import io
import re
import requests
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract

HTTP_HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36'}
## World Gold Council: page text contains e.g. "Total above-ground stock (end-Q2 2026): 222,600 tonnes" (updated quarterly)
WGC_GOLD_STOCK_URL = 'https://www.gold.org/goldhub/data/how-much-gold'
## FRED csv download (no API key), cosd: first observation date
FRED_CSV_URL = 'https://fred.stlouisfed.org/graph/fredgraph.csv?id={}&cosd={}'
## years of history loaded per run (the ETH asset page compares against a year ago at most), past values get revised by FRED
FRED_HISTORY_YEARS = 3

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
        load_type:str - 'gold_stock' or 'fred'
        series:list - FRED series ids (only for load_type 'fred')
    """
    def extract(self, load_params:dict):
        self.load_type = load_params['load_type']
        if self.load_type == 'gold_stock':
            df = self.extract_gold_stock()
        elif self.load_type == 'fred':
            df = self.extract_fred(load_params['series'])
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

    ## FRED series as raw values, stored with origin_key 'fred' and the lowercased series id as metric_key, empty observations (e.g. holidays) dropped
    def extract_fred(self, series:list):
        start = (pd.Timestamp.now(tz='UTC').normalize() - pd.DateOffset(years=FRED_HISTORY_YEARS)).date()
        dfs = []
        for series_id in series:
            ## no browser User-Agent: FRED's bot protection drops python requests that pretend to be a browser
            r = requests.get(FRED_CSV_URL.format(series_id, start), timeout=60)
            r.raise_for_status()
            ## FRED serves an HTML maintenance page with HTTP 200 during outages
            if not r.text.startswith('observation_date'):
                raise ValueError(f"FRED returned no csv for {series_id} (maintenance?): {r.text[:100]!r}")
            df = pd.read_csv(io.StringIO(r.text))
            df.columns = ['date', 'value']
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.dropna()
            print(f"...{self.name} - FRED {series_id}: {df['value'].iloc[-1]} at {df['date'].iloc[-1]} ({len(df)} rows)")
            df['metric_key'] = series_id.lower()
            dfs.append(df)

        df = pd.concat(dfs)
        df['origin_key'] = 'fred'
        return df.set_index(['metric_key', 'origin_key', 'date'])
