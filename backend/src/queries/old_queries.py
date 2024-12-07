sql_queries_old= [
   
        ## Celestia
        ,SQLQuery(metric_key = "da_data_posted_bytes", origin_key = "da_celestia", sql=sql_q["celestia_da_data_posted_bytes"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "da_fees_eth", origin_key = "da_celestia", sql=sql_q["celestia_da_fees_eth"], currency_dependent = True, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "da_unique_blob_producers", origin_key = "da_celestia", sql=sql_q["celestia_da_unique_blob_producers"], currency_dependent = False, query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "da_blob_count", origin_key = "da_celestia", sql=sql_q["celestia_da_blob_count"], currency_dependent = False, query_parameters={"Days": 7})

        ## Starknet
        ,SQLQuery(metric_key = "user_base_weekly", origin_key = "starknet", sql=sql_q["starknet_user_base_xxx"], currency_dependent = False, query_parameters={"Days": 28, "aggregation": "week"})
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "starknet", sql=sql_q["starknet_fees_paid_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "starknet", sql=sql_q["starknet_txcosts_median_eth"], query_parameters={"Days": 7})
        ,SQLQuery(metric_key = "rent_paid_eth", origin_key = "starknet", sql=sql_q["starknet_rent_paid_eth"], query_parameters={"Days": 7})
]