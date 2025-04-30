from web3 import Web3, HTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware
from web3.middleware.base import Web3Middleware
import time
import random
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError

class RateLimitExceededException(Exception):
    """Exception raised when the RPC call rate limit is exceeded."""
    pass

class EthProxy:
    def __init__(self, eth, increment_call_count_func, web3cc):
        self._eth = eth
        self._increment_call_count = increment_call_count_func
        self._web3cc = web3cc

    def __getattr__(self, name):
        if name == "block_number":
            self._increment_call_count()
            return getattr(self._eth, name)

        attr = getattr(self._eth, name)

        if callable(attr):
            def hooked(*args, **kwargs):
                if name in ["get_block", "get_transaction_receipt", "get_transaction", "get_block_receipts"]:
                    self._increment_call_count()
                    
                # Check if this method is known to be unsupported for this RPC
                if name == "get_block_receipts":
                    rpc_url = self._web3cc.get_rpc_url()
                    if not Web3CC.is_method_supported(rpc_url, name):
                        # Method not supported, raise exception to trigger fallback
                        raise ValueError(f"Method '{name}' is known to be unsupported for RPC: {rpc_url}")
                
                return self.retry_operation(attr, name, *args, **kwargs)
            return hooked
        else:
            return attr

    def retry_operation(self, func, method_name, *args, max_retries=5, initial_wait=1.0, **kwargs):
        retries = 0
        wait_time = initial_wait
        current_rpc = self._web3cc.get_rpc_url()
        while retries < max_retries:
            #print(f"Attempting {method_name} on RPC {current_rpc} with args {args} and kwargs {kwargs}. Retry {retries+1}")
            if self._web3cc.is_rate_limited and retries > 2:
                print(f"RETRY - Rate Limit exceed: for {current_rpc}: Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            try:
                return func(*args, **kwargs)
            except ReadTimeout as e:
                print(f"RETRY - Read timeout: for {current_rpc}: Read timeout while trying {method_name} with params {args}, {kwargs}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                wait_time = min(wait_time * 2, 30) + random.uniform(0, wait_time * 0.1)
                retries += 1
            except ConnectionError as e:
                print(f"RETRY - Connection Error: for {current_rpc}: Connection error while trying {method_name}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                wait_time = min(wait_time * 2, 30) + random.uniform(0, wait_time * 0.1)
                retries += 1
            except HTTPError as e:
                if e.response.status_code == 429:  # Rate Limit Exceeded
                    print(f"RETRY - Too Many Requests: for {current_rpc}: Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, 30) + random.uniform(0, wait_time * 0.1)
                    retries += 1
                else:
                    raise e
            except Exception as e:
                # For specific method not supported errors related to get_block_receipts
                if method_name == "get_block_receipts" and any(x in str(e).lower() for x in ["method not found", "not supported", "method not supported", "not implemented"]):
                    # Mark this RPC as not supporting get_block_receipts
                    Web3CC.method_not_supported(current_rpc, method_name)
                    # Raise the exception to trigger fallback
                    raise e
                else:
                    raise e

        raise Exception(f"ERROR: for {current_rpc}: Operation failed after {max_retries} retries for {method_name} with parameters {args}, {kwargs}.")

class ResponseNormalizerMiddleware(Web3Middleware):
    def response_processor(self, method, response):
        if response is None:
            return response
        if 'result' in response and 'uncles' in response['result'] and response['result']['uncles'] is None:
            response['result']['uncles'] = []
        return response
    
class Web3CC:
    # Static cache to track RPCs that don't support get_block_receipts
    _unsupported_methods_cache = {}
    
    def __init__(self, rpc_config):
        #print(f"Initializing Web3CC with RPC URL: {rpc_config['url']}")
        self._w3 = self._connect(rpc_config['url'])
        self.call_count = 0
        self.calls_per_sec_count = 0
        self.last_call_time = time.time()
        self.cooldown_until = 0
        self.is_rate_limited = False
        self.max_call_count = rpc_config.get('max_req', float('inf'))
        self.max_calls_per_sec = rpc_config.get('max_tps', float('inf'))
        if 'hypersync' in self._w3.provider.endpoint_uri:
            print("Hypersync is enabled. Skipping connection check.")
        else:
            if not self._w3.is_connected():
                raise ConnectionError("Failed to connect to the node.")

        self.eth = EthProxy(self._w3.eth, self._increment_call_count_and_rate_limit, self)

    def _connect(self, url):
        w3 = Web3(HTTPProvider(url))
        
        # Inject middlewares
        w3.middleware_onion.add(ResponseNormalizerMiddleware)
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        
        return w3
    
    def _increment_call_count_and_rate_limit(self):
        current_time = time.time()

        if self.cooldown_until > current_time:
            self.is_rate_limited = True
        else:
            self.call_count += 1

        if current_time - self.last_call_time >= 1:
            self.calls_per_sec_count = 0
            self.last_call_time = current_time

        self.calls_per_sec_count += 1

        if self.call_count > self.max_call_count or self.calls_per_sec_count > self.max_calls_per_sec:
            self.is_rate_limited = True
        else:
            self.is_rate_limited = False
        
    def __getattr__(self, name):
        if name in ['cooldown_until', 'initiate_cooldown', '_increment_call_count_and_rate_limit']:
            return object.__getattribute__(self, name)
        return getattr(self._w3, name)
    
    def get_rpc_url(self):
        raw_url = self._w3.provider.endpoint_uri
        # Remove http:// or https:// from the URL for clarity
        url = raw_url.split("//")[-1]

        return url

    @classmethod
    def method_not_supported(cls, rpc_url, method_name):
        """
        Marks a method as not supported for a specific RPC URL
        
        Args:
            rpc_url (str): The RPC URL
            method_name (str): The name of the unsupported method
        """
        if rpc_url not in cls._unsupported_methods_cache:
            cls._unsupported_methods_cache[rpc_url] = set()
        
        cls._unsupported_methods_cache[rpc_url].add(method_name)
        print(f"Marked method '{method_name}' as unsupported for RPC: {rpc_url}")
    
    @classmethod
    def is_method_supported(cls, rpc_url, method_name):
        """
        Checks if a method is supported by a specific RPC URL
        
        Args:
            rpc_url (str): The RPC URL
            method_name (str): The method name to check
            
        Returns:
            bool: True if the method is supported or unknown, False if known to be unsupported
        """
        if rpc_url in cls._unsupported_methods_cache:
            return method_name not in cls._unsupported_methods_cache[rpc_url]
        return True