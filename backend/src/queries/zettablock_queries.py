
class ZettablockQuery():
    def __init__(self, query_id: str, metric_key: str, origin_key: str, query_parameters: dict = None):
        self.query_id = query_id
        self.metric_key = metric_key
        self.origin_key = origin_key
        self.query_parameters = query_parameters
        self.last_run_id = None
        self.last_execution_loaded = None

class ZettablockRaw():
    def __init__(self, key:str, table_name: str, query_id:str, s3_folder:str, query_parameters: dict = None, steps: int = 1000):
        self.key = key
        self.table_name = table_name
        self.query_id = query_id
        self.last_run_id = None
        self.last_execution_loaded = None
        self.steps = steps
        self.s3_folder = s3_folder

zettablock_queries = [
    ## Polygon zkEVM
    ZettablockQuery(metric_key = "txcount", origin_key = "polygon_zkevm", query_id='qu7-fb9cf342-195e-40e4-9802-762d0b8fc41c', query_parameters={"Days": 7})
    ,ZettablockQuery(metric_key = "daa", origin_key = "polygon_zkevm", query_id='qu10-04a1b83c-eac2-4c42-8ba6-d2fc7133e08d', query_parameters={"Days": 7})
    ,ZettablockQuery(metric_key = "fees_paid_usd", origin_key = "polygon_zkevm", query_id='qu10-7a0b740f-2c21-4bd1-a805-534f6e1e23bc', query_parameters={"Days": 7})
]

zettablock_raws = [
    ## Polygon zkEVM
    ZettablockRaw(key = "polygon_zkevm_tx", table_name = "polygon_zkevm_tx", query_id = 'qu30-065e163f-a38a-4765-ab95-49db7fd65695', steps=2500, s3_folder = "polygon_zkevm")
]