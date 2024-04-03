from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from airflow.decorators import dag, task 
from src.db_connector import DbConnector

import pandas as pd
import os
from src.chain_config import adapter_mapping
from src.api.json_creation import JSONCreation

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email' : ['matthias@orbal-analytics.com'],
        'email_on_failure': True,
        'retry_delay' : timedelta(minutes=5)
    },
    dag_id='api_sql_fees',
    description='Run some sql aggregations for fees page.',
    tags=['metrics', 'near-real-time'],
    start_date=datetime(2023,4,24),
    schedule_interval='*/30 * * * *'
)

def etl():
        @task()
        def run_aggregate_metrics():
                db_connector = DbConnector()
                days = 2
                granularities = {
                        'daily'         : """date_trunc('day', "block_timestamp")""", 
                        '4_hours'       : """date_trunc('day', block_timestamp) +  INTERVAL '1 hour' * (EXTRACT(hour FROM block_timestamp)::int / 4 * 4)""", 
                        'hourly'        : """date_trunc('hour', "block_timestamp")""", 
                        '10_min'        : """date_trunc('hour', block_timestamp) + INTERVAL '10 min' * FLOOR(EXTRACT(minute FROM block_timestamp) / 10)""", 
                }
                for chain in adapter_mapping:
                        origin_key = chain.origin_key
                        if chain.in_fees_api == False:
                                print(f"...skipping {origin_key} as it is not in the fees api")
                        else:
                                
                                for granularity in granularities:
                                        ## txcosts_average
                                        print(f"... processing txcosts_average for {origin_key} and {granularity} granularity")
                                        timestamp_query = granularities[granularity]
                                        exec_string = f"""
                                                SELECT
                                                        {timestamp_query} AS timestamp,
                                                        '{origin_key}' as origin_key,
                                                        'txcosts_avg_eth' as metric_key,
                                                        '{granularity}' as granularity,
                                                        AVG(tx_fee) as value
                                                FROM public.{origin_key}_tx
                                                WHERE tx_fee <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{days} days' AND now()
                                                GROUP BY 1,2,3,4
                                                having count(*) > 20
                                        """

                                        df = pd.read_sql(exec_string, db_connector.engine.connect())
                                        df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                                        db_connector.upsert_table('fact_kpis_granular', df)

                                        ## txcosts_median
                                        print(f"... processing txcosts_median for {origin_key} and {granularity} granularity")
                                        exec_string = f"""
                                                WITH 
                                                median_tx AS (
                                                        SELECT
                                                                {timestamp_query} AS timestamp,
                                                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                                                        FROM public.{origin_key}_tx
                                                        WHERE tx_fee <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{days} days' AND now()
                                                        GROUP BY 1
                                                        having count(*) > 20
                                                )

                                                SELECT
                                                        '{origin_key}' as origin_key,
                                                        'txcosts_median_eth' as metric_key,
                                                        z.timestamp,
                                                        '{granularity}' as granularity,
                                                        z.median_tx_fee as value
                                                FROM median_tx z
                                        """

                                        df = pd.read_sql(exec_string, db_connector.engine.connect())
                                        df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                                        db_connector.upsert_table('fact_kpis_granular', df)


                                        ## txcosts_native_median
                                        if origin_key != 'starknet':
                                                print(f"... processing txcosts_median for {origin_key} and {granularity} granularity")                                        
                                                exec_string = f"""
                                                        WITH 
                                                        median_tx AS (
                                                                SELECT
                                                                        {timestamp_query} AS timestamp,
                                                                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee) AS median_tx_fee
                                                                FROM public.{origin_key}_tx
                                                                WHERE tx_fee <> 0 AND block_timestamp BETWEEN date_trunc('day', now()) - interval '{days} days' AND now()
                                                                        AND empty_input = TRUE
                                                                GROUP BY 1
                                                                having count(*) > 10
                                                        )

                                                        SELECT
                                                                '{origin_key}' as origin_key,
                                                                'txcosts_native_median_eth' as metric_key,
                                                                z.timestamp,
                                                                '{granularity}' as granularity,
                                                                z.median_tx_fee as value
                                                        FROM median_tx z
                                                """

                                                df = pd.read_sql(exec_string, db_connector.engine.connect())
                                                df.set_index(['origin_key', 'metric_key', 'timestamp', 'granularity'], inplace=True)
                                                db_connector.upsert_table('fact_kpis_granular', df)

        @task()
        def run_create_fees_json(run_aggregate_metrics:str):
                db_connector = DbConnector()
                json_creator = JSONCreation(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, "v1")
                df = json_creator.get_data_fees()
                json_creator.create_fees_table_json(df)
                json_creator.create_fees_linechart_json(df)

                ## TO BE DEPRECATED:
                json_creator.create_fees_json()
                json_creator.create_fees_dict()
   
        run_create_fees_json(run_aggregate_metrics())
etl()