from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json

class BigQuery():
    def __init__(self, credentials_json):
        # Set up the credentials
        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)

        # Create a BigQuery client
        self.client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    def execute_bigquery(self, query):
        # Execute the query
        query_job = self.client.query(query)
        # Wait for the query to complete and get the results
        results = query_job.result()
        # Convert the results to a pandas DataFrame
        df = results.to_dataframe()
        return df