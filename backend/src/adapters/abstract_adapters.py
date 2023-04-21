from abc import ABC, abstractmethod
import pandas as pd

class AbstractAdapter(ABC):

    @abstractmethod
    def __init__(self, name, adapter_params:dict, db_connector):
        self.adapter_params = adapter_params
        self.name = name
        self.db_connector = db_connector
        """
        - param adapter_params: some useful adapter config stuff such as API keys, URLs, etc
        - para db_connector: database connection
        """

class AbstractAdapterRaw(AbstractAdapter):

    def __init__(self, name, adapter_params:dict, db_connector):
        super().__init__(name, adapter_params, db_connector)
        self.orchestration = False
    
    @abstractmethod
    def extract_raw(self, load_params:dict) -> pd.DataFrame:
        """
        This function should be used to request the most granular transaction (raw) data from a datasource (e.g. API).
        - param load_params: some useful load config
        """
        raise NotImplementedError

    def load_raw(self, df_transformed) -> None:
        """
        This function should be used to persist our data to our database
        - param df_transformed: the transformed dataframe that should be used for the upload
        """
        raise NotImplementedError

    def orchestratation_raw(self):
        """
        This function call extract_raw script and whenever the df hits 10k rows, we load it into the db and start over. 
        It is useful for endpoints that return a lot of data and we wont to make sure that we don't run into memory issues or similar.
        """
        raise NotImplementedError