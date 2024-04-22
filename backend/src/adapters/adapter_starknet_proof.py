import time
import pandas as pd
from datetime import datetime, timedelta

from src.adapters.abstract_adapters import AbstractAdapter
from src.chain_config import adapter_mapping
from src.misc.helper_functions import api_get_call, upsert_to_kpis
from src.misc.helper_functions import print_init, print_load, print_extract


class AdapterStarknetProof(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Starknet Proof", adapter_params, db_connector)
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
    """
    def extract(self, load_params:dict):
        days = load_params.get('days', 1)

        ## extract data
        df = self.extract_data(days)

        print_extract(self.name, load_params,df.shape)
        return df 

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)

    ## ----------------- Helper functions --------------------

    def extract_data(self, days):
        origin_key = 'starknet'
        metric_key = 'proof_costs_eth'

        dfMain = pd.DataFrame()

        for i in range(1, days):
            print(f"...{self.name} - loading for {origin_key} - day {i}")
            load_date = datetime.today() - timedelta(days=i)

            ##check if load date before 2024-02-26
            if load_date < datetime(2024, 2, 26):
                print("Reached the end of the data. Stopping extraction.")
                break

            date_string = load_date.strftime('%Y-%m-%d')
            url = f"https://storage.googleapis.com/starknet-sharp-bi-public-buckets/files/namespace%3Dsharp5-production-bi/daily_reports/gcp-starknet-production_starknet-mainnet/pricing_report_{date_string}_gcp-starknet-production_starknet-mainnet.json"       

            response_json = api_get_call(url, sleeper=10, retries=10)
            proof_costs = response_json['total_cost']

            # Create a DataFrame with one entry
            df = pd.DataFrame({
                'origin_key': [origin_key],
                'metric_key': [metric_key],
                'date': [date_string],
                'value': [proof_costs]
            })

            df['date'] = pd.to_datetime(df['date'])
            df['date'] = df['date'].dt.date

            dfMain = pd.concat([dfMain, df])
            time.sleep(0.2)

        print(f"...{self.name} - loaded for {origin_key}. Shape: {dfMain.shape}")
        dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain