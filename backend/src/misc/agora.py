import requests
import pandas as pd

class AgoraAPI:    
    def __init__(self, api_key: str):
        self.base_url = "https://vote.optimism.io/api/v1/"
        self.api_key = api_key
        
    def _get_headers(self) -> dict:
        """
        Generate headers for the API request.
        Returns:
            dict: Headers including authorization and accept type.
        """
        headers = {
            'Authorization': f"Bearer {self.api_key}",
            'accept': 'application/json'
        }
        return headers
    
    def get_proposals(self, limit: int = 10, offset: int = 0) -> dict:
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
            print(f"Fetching: {self.base_url + endpoint}")

            # Make the GET request
            response = requests.get(
                self.base_url + endpoint,
                headers=self._get_headers(),
                params=params
            )
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            df = pd.DataFrame(response.json()['data'])

            # Return the JSON response
            return df
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch AgoraAPI proposals: {str(e)}")