import requests
import pandas as pd

class AgoraAPI:    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            'Authorization': f"Bearer {self.api_key}",
            'accept': 'application/json'
        }
        # base_url mapping
        self.base_url = {
            "Optimism": "https://vote.optimism.io/api/v1/",
            "Scroll": "https://gov.scroll.io/api/v1/"
        }
    
    def get_proposals(self, url, limit: int = 10, offset: int = 0) -> dict:
        """
        Fetch proposals from the Agora API.
        Args:
            limit: Number of proposals to return (default: 10)
            offset: Number of proposals to skip (default: 0)
        Returns:
            Dict containing the API response.
        """
        try:
            endpoint = "proposals"
            params = {
                'limit': limit,
                'offset': offset
            }
            
            # Print the URL for debugging purposes
            print(f"Fetching: {url + endpoint}")

            # Make the GET request
            response = requests.get(
                url + endpoint,
                headers=self.headers,
                params=params
            )
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            df = pd.DataFrame(response.json()['data'])

            # Return the JSON response
            return df
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch AgoraAPI proposals: {str(e)}")