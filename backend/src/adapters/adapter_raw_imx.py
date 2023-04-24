import time
import pandas as pd
from datetime import datetime, timedelta

from src.adapters.abstract_adapters import AbstractAdapterRaw
from src.misc.helper_functions import api_get_call
from src.misc.helper_functions import print_init, print_extract_raw, dataframe_to_s3

##disable pandas warnings
pd.options.mode.chained_assignment = None

##ToDos: 
# Add logs (query execution, execution fails, etc)

class AdapterRawImx(AbstractAdapterRaw):
    """
    adapter_params require the following fields
        load_types:list(str) - list of load types to be used
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("IMX", adapter_params, db_connector)
        self.base_url = "https://api.x.immutable.com/v1/"
        self.origin_key = 'imx'
        self.load_types = adapter_params['load_types']

        self.forced_refresh = adapter_params['forced_refresh']

        print_init(self.name, self.adapter_params)

    def extract_raw(self) -> pd.DataFrame:
        total_load = 0
        for load_type in self.load_types:
            main_props = self.get_main_properties(load_type)
            if self.forced_refresh == 'no':
                current_refresh_param = self.prep_timestamp(self.db_connector.get_latest_imx_refresh_date(main_props['tbl_name']))
            else:
                current_refresh_param = self.forced_refresh
            cursor = ''

            counter = 0
            type_load = 0
            dfMain = pd.DataFrame()
            print(f"... start loading {load_type} - with refresh_param: {current_refresh_param}")
            while True:
                try:
                    #print(f"... IMX {load_type} - #{counter} - Currently loaded: {type_load}. Used refresh_param: {current_refresh_param}")
                    if cursor != '':
                        url = self.base_url + main_props['url_part'] + current_refresh_param + "&cursor=" + cursor
                    else:
                        url = self.base_url + main_props['url_part']  + current_refresh_param

                    response_json = api_get_call(url, sleeper=10, retries=20)
                    cursor = response_json['cursor']

                    if cursor == '': #cursors are only empty for empty api calls, hence we check before we normalize and append
                        break
                    else:
                        df = pd.json_normalize(response_json['result'], sep='_')
                        dfMain = pd.concat([dfMain,df])
                        counter += 1
                        type_load += df.shape[0]

                    if dfMain.shape[0] > 10000:
                        print(f"... IMX {load_type} - Loaded to df: {dfMain.shape[0]} rows. Max timestamp: {dfMain.timestamp.max()}")
                        self.load_raw(dfMain, main_props, load_type)
                        dfMain = pd.DataFrame()

                    time.sleep(0.2)

                except KeyboardInterrupt:
                    print(f"Execution ended successfully. Last cursor: {cursor}")
                    break
                except Exception as e:
                    print("unexpected error")
                    print(e)
                    print(f"cursor: {cursor}")
                    break
            
            self.load_raw(dfMain, main_props, load_type)
            total_load += type_load
            print(f"... Load for {load_type} finished. Loaded: {type_load} rows.")
        
        print_extract_raw(self.name, total_load)
        return dfMain

    def load_raw(self, df, main_props:dict, load_type:str):
        if df.shape[0] > 0:
            # df.to_parquet(f"output/raw_data/imx/{load_type}_{df.transaction_id.min()}-{df.transaction_id.max()}.parquet", index=False)
            # print(f"... Stored file locally: {main_props['tbl_name']}_{df.transaction_id.min()}-{df.transaction_id.max()}.parquet")
            
            if load_type == 'orders_filled':
                file_name = f"{main_props['tbl_name']}_{df.order_id.min()}-{df.order_id.max()}"
            else:
                file_name = f"{main_props['tbl_name']}_{df.transaction_id.min()}-{df.transaction_id.max()}"
            dataframe_to_s3(f'{load_type}/{file_name}', df)

            if load_type == 'orders_filled':
                dfFees = df[['order_id', 'user', 'updated_timestamp', 'fees']]

                ## split fees list into separate rows and add index columns with number of row
                dfFees = dfFees.explode('fees').reset_index(drop=True)
                dfFees['index'] = dfFees.groupby('order_id').cumcount()
                ## expand fees dict into separate columns
                dfFees = dfFees.join(pd.json_normalize(dfFees.pop('fees'), sep='_'))
                ## filter out when type is NaN
                dfFees = dfFees[dfFees.type.notna()]
                ## hex cols
                dfFees['user'] = dfFees['user'].str.replace('0x', '\\x', regex=False)
                dfFees['address'] = dfFees['address'].str.replace('0x', '\\x', regex=False)
                if 'token_data_contract_address' in dfFees.columns:
                    dfFees['token_data_contract_address'] = dfFees['token_data_contract_address'].str.replace('0x', '\\x', regex=False)
                dfFees['order_id_index'] = dfFees['order_id'].astype(str) + '_' + dfFees['index'].astype(str)

                ## upload to DB
                dfFees.set_index('order_id_index', inplace=True)
                self.db_connector.upsert_table('imx_fees', dfFees)
                print(f'... upserted fees: {dfFees.shape[0]}')

            ## prepare df for upload to DB
            if len(main_props['df_columns']) > 0:
                df = df[main_props['df_columns']]
            for col in main_props['hex_columns']:
                df[col] = df[col].str.replace('0x', '\\x', regex=False)            

            ## upload to DB
            df.set_index([main_props['index']], inplace=True)
            upserted = self.db_connector.upsert_table(main_props['tbl_name'], df)
            print(f"... upserted {load_type} to {main_props['tbl_name']}: {df.shape[0]}")
            ##print_load_raw(self.name, upserted, main_props['tbl_name'])
        else:
            print(f"Nothing to load for {main_props['tbl_name']}")

    ## ----------------- Helper functions --------------------

    def get_main_properties(self, load_type):
        if load_type == 'deposits':
            df_columns = ['transaction_id', 'status', 'user', 'timestamp', 'token_type', 'token_data_id', 'token_data_token_address', 'token_data_decimals', 'token_data_quantity']
            hex_columns = ['user', 'token_data_token_address']
            tbl_name = 'imx_deposits'
            url_part = "deposits?page_size=200&order_by=transaction_id&direction=asc&min_timestamp="
            index = 'transaction_id'

        elif load_type == 'withdrawals':
            df_columns = ['transaction_id', 'status', 'rollup_status', 'withdrawn_to_wallet', 'sender', 'timestamp', 'token_type',
                'token_data_id', 'token_data_token_address', 'token_data_decimals', 'token_data_quantity']
            hex_columns = ['sender', 'token_data_token_address']
            tbl_name = 'imx_withdrawals'
            url_part = "withdrawals?page_size=200&order_by=transaction_id&direction=asc&min_timestamp="
            index = 'transaction_id'

        elif load_type == 'transfers':
            df_columns = ['transaction_id', 'status', 'user', 'receiver', 'timestamp', 'token_type', 'token_data_token_id', 
                          'token_data_id', 'token_data_token_address', 'token_data_decimals','token_data_quantity']
            hex_columns = ['user', 'receiver', 'token_data_token_address']
            tbl_name = 'imx_transfers'
            url_part = "transfers?page_size=200&order_by=created_at&direction=asc&min_timestamp=" ## created_at can be replaced with transaction_id once it works again
            index = 'transaction_id'

        elif load_type == 'trades':
            df_columns = [] ## no subset defined
            hex_columns = ['a_token_address', 'b_token_address']
            tbl_name = 'imx_trades'
            url_part = "trades?page_size=200&order_by=created_at&direction=asc&min_timestamp=" ## created_at can be replaced with transaction_id once it works again
            index = 'transaction_id'

        elif load_type == 'mints':
            df_columns = ['transaction_id', 'status', 'user', 'timestamp', 'token_type',
                'token_data_token_id', 'token_data_id', 'token_data_token_address', 'token_data_quantity']
            hex_columns = ['user', 'token_data_token_address']
            tbl_name = 'imx_mints'
            url_part = "mints?page_size=200&order_by=created_at&direction=asc&min_timestamp=" ## created_at can be replaced with transaction_id once it works again
            index = 'transaction_id'

        elif load_type == 'orders_filled':
            df_columns = ['order_id', 'status', 'user', 'timestamp', 'updated_timestamp', 'sell_type', 'sell_data_token_address', 'sell_data_quantity', 'buy_type', 
                          'buy_data_token_address', 'buy_data_quantity']
            hex_columns = ['user', 'sell_data_token_address', 'buy_data_token_address']
            tbl_name = 'imx_orders'
            url_part = "orders?status=filled&include_fees=true&page_size=200&order_by=updated_timestamp&direction=asc&updated_min_timestamp="
            index = 'order_id'

        else:
            raise ValueError(f"Unknown load type: {load_type}")     
        

        return {'df_columns':df_columns, 'hex_columns':hex_columns, 'tbl_name':tbl_name, 'url_part':url_part, 'index':index}    
    
    def prep_timestamp(self, timestamp:str):
        timestamp = (str(datetime.fromisoformat(timestamp) + timedelta(milliseconds=1)))
        return timestamp[:10] + 'T' + timestamp[11:26] + 'Z'