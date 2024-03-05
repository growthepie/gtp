
class ZettablockQuery():
    def __init__(self, query_id: str, metric_key: str, origin_key: str, query_parameters: dict = None):
        self.query_id = query_id
        self.metric_key = metric_key
        self.origin_key = origin_key
        self.query_parameters = query_parameters
        self.last_run_id = None
        self.last_execution_loaded = None

class ZettablockRaw():
    def __init__(self, key:str, table_name: str, query_id:str, s3_folder:str,max_block_query_id: str, query_parameters: dict = None, steps: int = 1000):
        self.key = key
        self.table_name = table_name
        self.query_id = query_id
        self.last_run_id = None
        self.last_execution_loaded = None
        self.steps = steps
        self.s3_folder = s3_folder
        self.max_block_query_id = max_block_query_id

zettablock_queries = [
    # ## Polygon zkEVM
    # ZettablockQuery(metric_key = "txcount", origin_key = "polygon_zkevm", query_id='qu7-fb9cf342-195e-40e4-9802-762d0b8fc41c', query_parameters={"Days": 7})
    # ,ZettablockQuery(metric_key = "daa", origin_key = "polygon_zkevm", query_id='qu10-04a1b83c-eac2-4c42-8ba6-d2fc7133e08d', query_parameters={"Days": 7})
    # ,ZettablockQuery(metric_key = "fees_paid_eth", origin_key = "polygon_zkevm", query_id='qu10-7a0b740f-2c21-4bd1-a805-534f6e1e23bc', query_parameters={"Days": 7})
    # ,ZettablockQuery(metric_key = "txcosts_median_eth", origin_key = "polygon_zkevm", query_id='qu17-cd955d3e-4111-4f20-99e6-6c3630e02939', query_parameters={"Days": 7})

    ## zkSync era
    ZettablockQuery(metric_key = "txcount", origin_key = "zksync_era", query_id='qu17-f35b45c1-5f98-44e0-bcc7-c250739cf908', query_parameters={"Days": 7})
    ,ZettablockQuery(metric_key = "daa", origin_key = "zksync_era", query_id='qu17-99e3c55c-2487-4d67-95fc-5d5500e917b9', query_parameters={"Days": 7})
    ,ZettablockQuery(metric_key = "fees_paid_eth", origin_key = "zksync_era", query_id='qu17-0fca0e9d-d3d8-43ae-871d-80535e7af454', query_parameters={"Days": 7})
    ,ZettablockQuery(metric_key = "txcosts_median_eth", origin_key = "zksync_era", query_id='qu17-bb58eac1-8c13-4bec-adad-026ec52e22bd', query_parameters={"Days": 7})

    # ## Arbitrum
    # ,ZettablockQuery(metric_key = "txcount", origin_key = "arbitrum", query_id='qu2-d9bfa3ab-9a7f-4195-b5b7-926657031cf7', query_parameters={"Days": 7})
    # ,ZettablockQuery(metric_key = "daa", origin_key = "arbitrum", query_id='qu2-2bfae4a0-d1e1-4758-9b1e-e7cf21ac34ab', query_parameters={"Days": 7})
    # ,ZettablockQuery(metric_key = "fees_paid_usd", origin_key = "arbitrum", query_id='qu2-73510541-5904-4763-a33e-601ea2acfd26', query_parameters={"Days": 7})
]

zettablock_raws = [
    # ## Polygon zkEVM
    # ZettablockRaw(key = "polygon_zkevm_tx", table_name = "polygon_zkevm_tx", query_id = 'qu30-065e163f-a38a-4765-ab95-49db7fd65695', steps=2500, s3_folder = "polygon_zkevm", max_block_query_id="qu17-51501ccc-fec1-4641-bcc0-fd4c6a34234a")

    ## zkSync era
    ZettablockRaw(key = "zksync_era_tx", table_name = "zksync_era_tx", query_id = 'qu17-0d6b9c22-5df6-4f42-9812-3fd6fbfb40db', steps=1000, s3_folder = "zksync_era", max_block_query_id = 'qu17-78e5469a-27bb-43cb-ad45-ba8e11930288')

    # ## Arbitrum
    # ,ZettablockRaw(key = "arbitrum_tx", table_name = "arbitrum_tx", query_id = 'qu2-02900a44-d99f-4807-bdfe-2c1308e311aa', steps=2000, s3_folder = "arbitrum")
]