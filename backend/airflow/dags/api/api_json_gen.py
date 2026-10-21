from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

api_version = "v1"

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=1),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_json_gen',
    description='NEW DAG to create JSON files for our frontend.',
    tags=['api', 'daily'],
    start_date=datetime(2025,8,28),
    schedule='35 05 * * *'
)

def run():
    @task(execution_timeout=timedelta(minutes=30))
    def run_create_metrics_per_chain_jsons_daily():
        from src.misc.helper_functions import defer_cloudfront_invalidations
        defer_cloudfront_invalidations()
        import os
        from src.api.json_gen import JsonGen
        from src.db_connector import DbConnector

        db_connector = DbConnector()
        
        json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_gen.create_metric_jsons(level='chains', metric_ids=['tvl', 'stables_mcap', 'rent_paid', 'profit', 'fdv', 'market_cap', 'app_revenue']) ## all metric_ids that don't run hourly
        json_gen.create_metric_jsons(level='data_availability')
        
    @task(execution_timeout=timedelta(minutes=30))
    def run_create_chain_overview_jsons():    
        from src.misc.helper_functions import defer_cloudfront_invalidations
        defer_cloudfront_invalidations()
        import os
        from src.api.json_gen import JsonGen
        from src.db_connector import DbConnector
        
        db_connector = DbConnector()

        json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_gen.create_chains_jsons()
    
    @task(execution_timeout=timedelta(minutes=30))
    def run_create_chain_user_insights_jsons():    
        from src.misc.helper_functions import defer_cloudfront_invalidations
        defer_cloudfront_invalidations()
        import os
        from src.api.json_gen import JsonGen
        from src.db_connector import DbConnector
        
        db_connector = DbConnector()

        json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_gen.create_user_insights_json()
        
    @task(execution_timeout=timedelta(minutes=30))
    def run_create_treemap_json():
        from src.misc.helper_functions import defer_cloudfront_invalidations
        defer_cloudfront_invalidations()
        import os
        from src.api.json_gen import JsonGen
        from src.db_connector import DbConnector
        
        db_connector = DbConnector()

        json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_gen.create_blockspace_tree_map_json()
        
    @task(execution_timeout=timedelta(minutes=30))
    def run_create_temp_megaeth_apps_json():
        from src.misc.helper_functions import defer_cloudfront_invalidations
        defer_cloudfront_invalidations()
        import os
        from src.api.json_gen import JsonGen
        from src.db_connector import DbConnector
        
        db_connector = DbConnector()

        json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_gen.temp_megaeth_apps_export()

    @task(execution_timeout=timedelta(minutes=30))
    def run_create_contract_owner_project_mappings_json():
        from src.misc.helper_functions import defer_cloudfront_invalidations
        defer_cloudfront_invalidations()
        import os
        from src.api.json_gen import JsonGen
        from src.db_connector import DbConnector

        db_connector = DbConnector()

        json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_gen.create_contract_owner_project_mappings_json()

    @task(execution_timeout=timedelta(minutes=30))
    def run_create_seo_jsons():
        from src.misc.helper_functions import defer_cloudfront_invalidations
        defer_cloudfront_invalidations()
        import os
        from src.api.json_gen import JsonGen
        from src.db_connector import DbConnector

        db_connector = DbConnector()

        json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_gen.create_seo_jsons()
        
    # @task()
    # def run_create_ecosystem_jsons():    
    #     import os
    #     from src.api.json_gen import JsonGen
    #     from src.db_connector import DbConnector
        
    #     db_connector = DbConnector()

    #     json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
    #     json_gen.create_ecosystem_builders_json()
        
    
    @task(trigger_rule='all_done')
    def run_invalidate_cloudfront():
        ## one wildcard invalidation (billed as 1 path) instead of one per uploaded file; runs even if some tasks failed
        import os
        from src.misc.helper_functions import empty_cloudfront_cache
        empty_cloudfront_cache(os.getenv("CF_DISTRIBUTION_ID"), f'/{api_version}/*')

    uploads = [
        run_create_metrics_per_chain_jsons_daily(),
        run_create_chain_overview_jsons(),
        #run_create_chain_user_insights_jsons(),
        #run_create_ecosystem_jsons(),
        run_create_treemap_json(),
        run_create_temp_megaeth_apps_json(),
        run_create_contract_owner_project_mappings_json(),
        run_create_seo_jsons(),
    ]
    uploads >> run_invalidate_cloudfront()

run()
