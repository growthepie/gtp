# tally is a governance platform for voting on proposals of L2 DAOs
import requests
import pandas as pd

class TallyAPI:
    def __init__(self, api_key: str):

        self.api_key = api_key
        self.base_url = "https://api.tally.xyz/query"
        self.headers = {
            "Api-Key": self.api_key,
            "Content-Type": "application/json"
        }

        # organisation id mapping
        self.organisation_id = {
            "Arbitrum": "2206072050315953936",
            "Zksync": "2297436623035434412"
        }
        
        # Store GraphQL queries as class attributes
        self.proposals_query = """
        query GovernanceProposals($input: ProposalsInput!) {
            proposals(input: $input) {
                nodes {
                    ... on Proposal {
                        id
                        onchainId
                        status
                        originalId
                        createdAt
                        voteStats {
                            votesCount
                            percent
                            type
                            votersCount
                        }
                        metadata {
                            description
                        }
                        start {
                            ... on Block {
                                timestamp
                            }
                            ... on BlocklessTimestamp {
                                timestamp
                            }
                        }
                        block {
                            timestamp
                        }
                        governor {
                            id
                            quorum
                            name
                            timelockId
                            token {
                                decimals
                            }
                        }
                    }
                }
                pageInfo {
                    firstCursor
                    lastCursor
                    count
                }
            }
        }
        """

    def _handle_request(self, query: str, variables: dict) -> dict:
        """Send GraphQL request and handle response.
        Args:
            query (str): GraphQL query string
            variables (dict): Query variables
        Returns:
            dict: JSON response data
        """
        response = requests.post(
            self.base_url,
            json={"query": query, "variables": variables},
            headers=self.headers
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Query failed with status code {response.status_code}: {response.text}")

    def get_proposals(self, organization_id: str, limit: int = 10, sort_by: str = "id", descending: bool = True) -> pd.DataFrame:
        """Fetch proposals for a given organization.
        Args:
            organization_id (str): ID of the governance organization
            limit (int, optional): Number of proposals to fetch. Defaults to 10.
            sort_by (str, optional): Field to sort by. Defaults to "id".
            descending (bool, optional): Sort in descending order. Defaults to True. 
        Returns:
            pd.DataFrame: DataFrame containing proposal data with metadata expanded
        """
        variables = {
            "input": {
                "filters": {
                    "organizationId": organization_id
                },
                "sort": {
                    "sortBy": sort_by,
                    "isDescending": descending
                },
                "page": {
                    "limit": limit
                }
            }
        }

        data = self._handle_request(self.proposals_query, variables)
        
        # Convert to DataFrame
        df = pd.DataFrame(data['data']['proposals']['nodes'])
        
        # Expand metadata column if it exists
        if 'metadata' in df.columns:
            df = pd.concat([
                df.drop(['metadata'], axis=1),
                df['metadata'].apply(pd.Series)
            ], axis=1)
        
        # Expand start to startTime
        df['startTime'] = df['start'].apply(lambda x: x['timestamp'])

        return df