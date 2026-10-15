import pandas as pd
from datetime import datetime, timezone

from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan, api_get_call
from src.misc.jinja_helper import execute_jinja_query

## timeframes of the KPI charts on the ETH asset page
## hours: window length based on hourly data, days: window length based on daily data, months: calendar months incl. current (partial) month
ETH_ASSET_TIMEFRAMES = {
    '24h': {'granularity': 'hourly', 'hours': 24, 'bucket_hours': 1},
    '7d':  {'granularity': '3_hours', 'hours': 24*7, 'bucket_hours': 3},
    '60d': {'granularity': 'daily', 'days': 60, 'bucket_days': 1},
    '1y':  {'granularity': 'weekly', 'days': 52*7, 'bucket_days': 7},
    '5y':  {'granularity': 'monthly', 'months': 60},
}

SOURCES = {
    'coingecko': {'name': 'CoinGecko', 'url': 'https://www.coingecko.com/en/coins/ethereum'},
    'ultrasound': {'name': 'ultrasound.money', 'url': 'https://ultrasound.money'},
    'ethsupply': {'name': 'ethsupply.fyi', 'url': 'https://ethsupply.fyi'},
    'growthepie': {'name': 'growthepie', 'url': 'https://www.growthepie.com'},
    'worldbank': {'name': 'World Bank', 'url': 'https://data.worldbank.org/indicator/SP.POP.TOTL'},
}

## World Bank Population Estimates and Projections (source 40), mid-year (July 1) world population incl. projections
WORLD_POPULATION_URL = 'https://api.worldbank.org/v2/country/WLD/indicator/SP.POP.TOTL?format=json&source=40&date=2020:2040&per_page=100'
## window to derive per_second_increase of total wallets (7d is noticeably noisier due to HLL estimation error)
TOTAL_WALLETS_GROWTH_DAYS = 30

## col: column in the prepared hourly/daily dataframes
## agg: how values are aggregated into a bucket and into the 'current' value
##   last -> stock (value at end of bucket, current = latest value, change vs. value one window ago)
##   mean -> rate (avg over bucket, current = latest value, change vs. value one window ago)
##   sum  -> flow (total in bucket, current = total in window, change vs. total in previous window)
ETH_ASSET_KPIS = {
    'eth_price':     {'name': 'ETH Price', 'unit': 'usd', 'col': 'price_usd', 'agg': 'last', 'sources': ['coingecko']},
    'eth_supply':    {'name': 'ETH Supply', 'unit': 'eth', 'col': 'eth_supply_eth', 'agg': 'last', 'sources': ['ultrasound']},
    'staking_yield': {'name': 'Staking Yield', 'unit': 'percent', 'col': 'staking_yield', 'agg': 'mean', 'sources': ['ethsupply', 'growthepie']},
    'staked_share':  {'name': 'Share of Supply Staked', 'unit': 'percent', 'col': 'staked_share', 'agg': 'last', 'sources': ['ethsupply', 'growthepie', 'ultrasound']},
    'eth_burned':    {'name': 'ETH Burned', 'unit': 'eth', 'col': 'eth_burned_eth', 'agg': 'sum', 'sources': ['growthepie']},
}

STOCK_COLS = ['price_usd', 'eth_supply_eth', 'eth_staked_eth']


class EthAssetJSONCreation():
    def __init__(self, s3_bucket, cf_distribution_id, db_connector, api_version='v1'):
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector
        self.api_version = api_version

    ## ----------------- Data loading --------------------

    def get_hourly_data(self, days:int=16):
        exec_string = f"""
            SELECT metric_key, "timestamp", value
            FROM public.fact_kpis_granular
            WHERE origin_key = 'ethereum'
                AND granularity = 'hourly'
                AND metric_key IN ('price_usd', 'eth_supply_eth', 'eth_burned_eth', 'eth_staked_eth', 'staking_rewards_cl_eth')
                AND "timestamp" >= date_trunc('hour', now() at time zone 'utc') - interval '{days} days'
        """
        df = self.db_connector.execute_query(exec_string, load_df=True)
        return df.pivot_table(index='timestamp', columns='metric_key', values='value', aggfunc='last').sort_index()

    def get_daily_data(self, days:int=6*366):
        exec_string = f"""
            SELECT metric_key, "date", value
            FROM public.fact_kpis
            WHERE origin_key = 'ethereum' AND metric_key = 'price_usd'
                AND "date" >= current_date - interval '{days} days'
            UNION ALL
            SELECT metric_key, "date", value
            FROM public.eim_fact
            WHERE origin_key = 'ethereum'
                AND metric_key IN ('eth_supply_eth', 'eth_burned_eth', 'eth_staked_eth', 'staking_yield_cl', 'staking_yield_el')
                AND "date" >= current_date - interval '{days} days'
        """
        df = self.db_connector.execute_query(exec_string, load_df=True)
        df['date'] = pd.to_datetime(df['date'])
        df = df.pivot_table(index='date', columns='metric_key', values='value', aggfunc='last').sort_index()
        ## only complete days
        today = pd.Timestamp(datetime.now(timezone.utc).date())
        return df[df.index < today]

    ## ----------------- Preparation --------------------

    ## complete index until end, forward fill stocks (e.g. staked ETH can lag a few hours)
    def complete_index(self, df:pd.DataFrame, end:pd.Timestamp, freq:str):
        df.index = df.index.astype('datetime64[ns]')
        idx = pd.date_range(start=df.index.min(), end=end, freq=freq).astype('datetime64[ns]')
        df = df.reindex(df.index.union(idx))
        for col in STOCK_COLS:
            if col in df.columns:
                df[col] = df[col].ffill()
        return df.reindex(idx)

    ## staking yield (CL + EL) as 7d rolling average of daily values
    def prepare_daily(self, df:pd.DataFrame, end_day:pd.Timestamp):
        df = self.complete_index(df, end_day, 'D')
        df['staking_yield'] = (df['staking_yield_cl'].ffill() + df['staking_yield_el'].ffill()).rolling(7, min_periods=1).mean()
        df['staked_share'] = df['eth_staked_eth'] / df['eth_supply_eth']
        return df

    ## staking yield = hourly CL yield (trailing 24h avg) + EL yield (7d avg of daily, EL is only available daily)
    ## if hourly CL rewards are missing (ethsupply.fyi fallback), the daily 7d avg CL yield is used
    def prepare_hourly(self, df:pd.DataFrame, df_daily:pd.DataFrame, end_hour:pd.Timestamp):
        df = self.complete_index(df, end_hour, 'h')

        df_d = pd.DataFrame({
            'cl_7d': df_daily['staking_yield_cl'].rolling(7, min_periods=1).mean(),
            'el_7d': df_daily['staking_yield_el'].rolling(7, min_periods=1).mean(),
        }).dropna(how='all')
        df_d = pd.merge_asof(pd.DataFrame(index=df.index), df_d, left_index=True, right_index=True, direction='backward')

        cl_hourly = df['staking_rewards_cl_eth'] * 24 * 365 / df['eth_staked_eth']
        cl = cl_hourly.rolling(24, min_periods=1).mean().fillna(df_d['cl_7d'])
        df['staking_yield'] = cl + df_d['el_7d'].ffill()
        df['staked_share'] = df['eth_staked_eth'] / df['eth_supply_eth']
        return df

    ## ----------------- Bucketing --------------------

    ## assigns every row in [start, end] a bucket label (bucket start), counted from window start so all buckets are complete
    def bucketize(self, df:pd.DataFrame, start:pd.Timestamp, end:pd.Timestamp, tf:dict):
        df = df[(df.index >= start) & (df.index <= end)].copy()
        if 'bucket_hours' in tf:
            step = pd.Timedelta(hours=tf['bucket_hours'])
        elif 'bucket_days' in tf:
            step = pd.Timedelta(days=tf['bucket_days'])
        else:
            df['bucket'] = df.index.to_period('M').to_timestamp()
            return df
        df['bucket'] = start + ((df.index - start) // step) * step
        return df

    def window_bounds(self, tf:dict, end_hour:pd.Timestamp, end_day:pd.Timestamp, shift:int=0):
        if 'hours' in tf:
            end = end_hour - pd.Timedelta(hours=tf['hours'] * shift)
            return end - pd.Timedelta(hours=tf['hours'] - 1), end
        if 'days' in tf:
            end = end_day - pd.Timedelta(days=tf['days'] * shift)
            return end - pd.Timedelta(days=tf['days'] - 1), end
        end = end_day
        start = (end.to_period('M') - (tf['months'] - 1)).to_timestamp()
        return start, end

    def aggregate_buckets(self, df:pd.DataFrame, col:str, agg:str):
        g = df.groupby('bucket')[col]
        if agg == 'last':
            return g.last()
        if agg == 'sum':
            return g.sum(min_count=1)
        return g.mean()

    def value_at(self, s:pd.Series, ts:pd.Timestamp):
        s = s[s.index <= ts].dropna()
        return s.iloc[-1] if len(s) > 0 else None

    def pct_change(self, current, previous):
        if current is None or previous is None or pd.isna(current) or pd.isna(previous) or previous == 0:
            return None
        return round((current - previous) / previous, 6)

    ## ----------------- JSON --------------------

    def create_kpi_dict(self, df_hourly_raw:pd.DataFrame, df_daily_raw:pd.DataFrame):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        end_hour = pd.Timestamp(now).floor('h') - pd.Timedelta(hours=1) ## last complete hour
        end_day = pd.Timestamp(now.date()) - pd.Timedelta(days=1) ## last complete day

        df_daily = self.prepare_daily(df_daily_raw, end_day)
        df_hourly = self.prepare_hourly(df_hourly_raw, df_daily, end_hour)

        kpi_dict = {}
        for kpi, cfg in ETH_ASSET_KPIS.items():
            col, agg = cfg['col'], cfg['agg']
            kpi_dict[kpi] = {
                'name': cfg['name'],
                'unit': cfg['unit'],
                'sources': [SOURCES[s] for s in cfg['sources']],
                'timeframes': {},
            }

            for tf_key, tf in ETH_ASSET_TIMEFRAMES.items():
                hourly_tf = 'hours' in tf
                df_src = df_hourly if hourly_tf else df_daily
                start, end = self.window_bounds(tf, end_hour, end_day)

                df_tf = self.bucketize(df_src, start, end, tf)
                s_buckets = self.aggregate_buckets(df_tf, col, agg)

                if agg == 'sum':
                    ## flow: total in window vs. total in previous window (data has to cover the full previous window)
                    current = df_tf[col].sum(min_count=1)
                    change = None
                    if 'months' not in tf:
                        prev_start, prev_end = self.window_bounds(tf, end_hour, end_day, shift=1)
                        df_prev = df_src[(df_src.index >= prev_start) & (df_src.index <= prev_end)]
                        first_valid = df_prev[col].first_valid_index()
                        if first_valid is not None and first_valid <= prev_start:
                            change = self.pct_change(current, df_prev[col].sum(min_count=1))
                else:
                    ## stock/rate: latest hourly value (same for all timeframes) vs. value one window ago
                    current = self.value_at(df_hourly[col], end_hour)
                    change = None
                    if hourly_tf:
                        change = self.pct_change(current, self.value_at(df_hourly[col], end_hour - pd.Timedelta(hours=tf['hours'])))
                    elif 'days' in tf:
                        ## daily values are start of day snapshots, window start = 'days' ago
                        prev = df_daily[col].get(start) if df_daily.index.min() <= start else None
                        change = self.pct_change(current, prev)

                unix = (s_buckets.index.tz_localize('UTC').astype('int64') // 10**6)
                data = [[int(u), None if pd.isna(v) else float(v)] for u, v in zip(unix, s_buckets.values)]

                kpi_dict[kpi]['timeframes'][tf_key] = {
                    'granularity': tf['granularity'],
                    'types': ['unix', 'value'],
                    'data': data,
                    'current': None if current is None or pd.isna(current) else float(current),
                    'change_pct': change,
                }
        return kpi_dict

    ## ----------------- Stats (single values, extrapolated per second by the frontend) --------------------

    ## lifetime unique addresses that initiated a tx on Ethereum (same as the 'daa' lifetime achievement on the chain page)
    ## value as of today 00:00 UTC (only complete days), per_second_increase = avg growth over the last 30 days
    def get_total_wallets(self):
        total = execute_jinja_query(self.db_connector, 'api/select_total_aa.sql.j2', {'origin_key': 'ethereum'}, return_df=True).iloc[0]['value']
        total_prev = execute_jinja_query(self.db_connector, 'api/select_total_aa.sql.j2', {'origin_key': 'ethereum', 'days': TOTAL_WALLETS_GROWTH_DAYS}, return_df=True).iloc[0]['value']
        ts = pd.Timestamp(datetime.now(timezone.utc).date(), tz='UTC')
        return {
            'name': 'Total Wallets',
            'unit': 'addresses',
            'value': int(total),
            'timestamp': int(ts.timestamp() * 1000),
            'per_second_increase': float(total - total_prev) / (TOTAL_WALLETS_GROWTH_DAYS * 86400),
            'sources': [SOURCES['growthepie']],
        }

    ## world population at ts, linearly interpolated between mid-year values, plus growth per second of the current segment
    def get_world_population(self, ts:pd.Timestamp):
        response_json = api_get_call(WORLD_POPULATION_URL, sleeper=10, retries=5)
        if not response_json or len(response_json) < 2 or not response_json[1]:
            raise ValueError("No world population data returned from World Bank API")
        pop = pd.Series({pd.Timestamp(f"{r['date']}-07-01", tz='UTC'): float(r['value']) for r in response_json[1] if r['value'] is not None}).sort_index()

        before, after = pop[pop.index <= ts], pop[pop.index > ts]
        if before.empty or after.empty:
            raise ValueError(f"World population data does not cover {ts}")
        t0, p0, t1, p1 = before.index[-1], before.iloc[-1], after.index[0], after.iloc[0]
        per_second = (p1 - p0) / (t1 - t0).total_seconds()
        return p0 + per_second * (ts - t0).total_seconds(), per_second

    ## ETH supply split evenly across world population
    ## per_second_increase = d(S/P)/dt = S'/P - S*P'/P^2, with S' = avg supply change per second over the last 7 days (hourly supply)
    def get_eth_per_person(self):
        df = self.db_connector.execute_query("""
            SELECT "timestamp", value
            FROM public.fact_kpis_granular
            WHERE origin_key = 'ethereum' AND granularity = 'hourly' AND metric_key = 'eth_supply_eth'
                AND "timestamp" >= date_trunc('hour', now() at time zone 'utc') - interval '7 days'
            ORDER BY 1
        """, load_df=True)
        if df.shape[0] < 2:
            raise ValueError("Not enough hourly supply data for eth_per_person")
        t0, s0 = pd.Timestamp(df['timestamp'].iloc[0], tz='UTC'), df['value'].iloc[0]
        ts, supply = pd.Timestamp(df['timestamp'].iloc[-1], tz='UTC'), df['value'].iloc[-1]
        supply_per_second = (supply - s0) / (ts - t0).total_seconds()

        population, population_per_second = self.get_world_population(ts)
        return {
            'name': 'ETH per Person',
            'unit': 'eth',
            'value': supply / population,
            'timestamp': int(ts.timestamp() * 1000),
            'per_second_increase': supply_per_second / population - supply * population_per_second / population**2,
            'sources': [SOURCES['ultrasound'], SOURCES['worldbank']],
            'details': {
                'eth_supply': supply,
                'eth_supply_per_second': supply_per_second,
                'world_population': population,
                'world_population_per_second': population_per_second,
            },
        }

    def create_stats_json(self):
        details_dict = {
            'data': {
                'total_wallets': self.get_total_wallets(),
                'eth_per_person': self.get_eth_per_person(),
            },
            'last_updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        }
        details_dict = fix_dict_nan(details_dict, 'eth_asset/stats')

        if self.s3_bucket is None:
            import os, json
            os.makedirs(f'output/{self.api_version}/eth_asset', exist_ok=True)
            with open(f'output/{self.api_version}/eth_asset/stats.json', 'w') as fp:
                json.dump(details_dict, fp)
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/eth_asset/stats', details_dict, self.cf_distribution_id, cache_control="public, max-age=60, s-maxage=900, stale-while-revalidate=60, stale-if-error=86400")
        print('DONE -- ETH asset stats json export done')
        return details_dict

    def create_kpis_json(self):
        df_hourly = self.get_hourly_data()
        df_daily = self.get_daily_data()

        details_dict = {
            'data': {
                'kpis': self.create_kpi_dict(df_hourly, df_daily),
            },
            'last_updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        }
        details_dict = fix_dict_nan(details_dict, 'eth_asset/kpis')

        if self.s3_bucket is None:
            import os, json
            os.makedirs(f'output/{self.api_version}/eth_asset', exist_ok=True)
            with open(f'output/{self.api_version}/eth_asset/kpis.json', 'w') as fp:
                json.dump(details_dict, fp)
        else:
            upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/eth_asset/kpis', details_dict, self.cf_distribution_id, cache_control="public, max-age=60, s-maxage=900, stale-while-revalidate=60, stale-if-error=86400")
        print('DONE -- ETH asset KPIs json export done')
        return details_dict
