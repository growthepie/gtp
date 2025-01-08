# agora is a governance platform for voting on proposals of L2 DAOs ### STILL WAITING FOR NEW API KEY
import requests

class AgoraAPI:
    """
    A client for interacting with the Tally voting API.
    """
    
    def __init__(self, api_key: str):
        self.base_url = "https://vote.optimism.io/api/v1/", 
        self.api_key = api_key
        
    def _get_headers(self) -> dict:
        headers = {
            'Authorization': self.api_key,
            'accept': 'application/json'
        }
        return headers
    
    def get_proposals(self, limit: int = 10, offset: int = 0) -> dict:
        """
        Fetch proposals from the Tally API.
        Args:
            limit: Number of proposals to return (default: 10)
            offset: Number of proposals to skip (default: 0)
        Returns:
            Dict containing the API response
        """
        try:
            params = {
                'limit': limit,
                'offset': offset
            }
            
            response = requests.get(
                self.base_url + 'proposals',
                headers=self._get_headers(),
                params=params
            )
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch proposals: {str(e)}")