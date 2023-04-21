import requests
import json
import io
import pandas as pd


class ZettaBlock_API():
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://api.zettablock.com/api/v1'
        self.header = {
            "accept": "application/json",
            "content-type": "application/json",
            "X-API-KEY": self.api_key
        }

    ## Trigger query and return queryrun id
    def trigger_query(self, query_id, payload):

        query_url = f'{self.base_url}/queries/{query_id}/trigger'
        res = requests.post(query_url, headers=self.header, json=payload)

        return res.json()['queryrunId']

    # check query status
    def check_query_execution(self, queryrun_id):
        queryrun_status_endpoint = f'{self.base_url}/queryruns/{queryrun_id}/status'

        res = requests.get(queryrun_status_endpoint, headers=self.header)
        response_json = json.loads(res.text)

        if response_json['state'] == 'SUCCEEDED':
            return True
        elif response_json['state'] == 'FAILED':
            raise Exception(f'Query failed: {response_json}')
        else:
            return False

    def get_query_results(self, queryrun_id, format_json=False):
        # Fetch result from queryrun id
        if format_json == False:
            queryrun_result_endpoint = f'{self.base_url}/stream/queryruns/{queryrun_id}/result?includeColumnName=true'
        else:
            queryrun_result_endpoint = f'{self.base_url}/stream/queryruns/{queryrun_id}/result?includeColumnName=true&format=json'

        res = requests.get(queryrun_result_endpoint, headers=self.header)

        df = pd.read_csv(io.StringIO(res.text))
        return df

