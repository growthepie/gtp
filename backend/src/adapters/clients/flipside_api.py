import requests
import json
import time

class FlipsideAPI():
    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {"Accept": "application/json", "Content-Type": "application/json", "x-api-key": self.api_key}
        self.url = 'https://node-api.flipsidecrypto.com/queries'
    

    def create_query(self, sql:str, ttl_minutes=180):
        r = requests.post(
            self.url, 
            data=json.dumps({
                "sql": sql,
                "ttlMinutes": ttl_minutes
            }),
            headers=self.headers,
        )
        if r.status_code != 200:
            raise Exception("Error creating query, got response: " + r.text + "with status code: " + str(r.status_code))
        
        response_json = json.loads(r.text)
        return response_json   


    def check_query_execution(self, token, page_number=1, page_size=100000): ## returns True if query is done
        r = requests.get(
            f'{self.url}/{token}?pageNumber={page_number}&pageSize={page_size}'.format(
            token=token,
            page_number=page_number,
            page_size=page_size
            ),
            headers=self.headers,
        )
        if r.status_code != 200:
            raise Exception("Error getting query results, got response: " + r.text + "with status code: " + str(r.status_code))
        
        data = json.loads(r.text)
        if data['status'] == 'running':
            return False
        
        return True

    def get_query_results(self, token, page_number=1, page_size=100000, sleeper=5):
        r = requests.get(
            f'{self.url}/{token}?pageNumber={page_number}&pageSize={page_size}'.format(
            token=token,
            page_number=page_number,
            page_size=page_size
            ),
            headers=self.headers,
        )
        if r.status_code != 200:
            raise Exception("Error getting query results, got response: " + r.text + "with status code: " + str(r.status_code))
        
        data = json.loads(r.text)
        if data['status'] == 'running':
            time.sleep(sleeper)
            return self.get_query_results(token)

        return data


    # def run(self):
    #     query = self.create_query()
    #     token = query.get('token')
    #     data = self.get_query_results(token)

    #     print(data['columnLabels'])
    #     for row in data['results']:
    #         print(row)