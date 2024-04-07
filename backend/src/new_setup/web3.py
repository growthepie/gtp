from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware
import time
import random

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
                if name in ["get_block", "get_transaction_receipt", "get_transaction"]:
                    self._increment_call_count()
                return self.retry_operation(attr, *args, **kwargs)
            return hooked
        else:
            return attr

    def retry_operation(self, func, *args, max_retries=5, initial_wait=1.0, **kwargs):
        retries = 0
        wait_time = initial_wait
        while retries < max_retries:
            if self._web3cc.is_rate_limited and retries > 2:
                print(f"For {self._web3cc.get_rpc_url()}: Rate limit exceeded, waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"For {self._web3cc.get_rpc_url()}: Operation failed ({self._web3cc._w3}) with exception: {e}. Retrying in {wait_time} seconds...")
                retries += 1
                time.sleep(wait_time)
                wait_time = min(wait_time * 2, 30) + random.uniform(0, wait_time * 0.1)

        raise Exception(f"For {self._web3cc.get_rpc_url()}: Operation failed after {max_retries} retries.")

class ResponseNormalizerMiddleware:
    def __init__(self, web3):
        self.web3 = web3

    def __call__(self, make_request, web3):
        def middleware(method, params):
            response = make_request(method, params)
            if 'result' in response and 'uncles' in response['result'] and isinstance(response['result']['uncles'], type(None)):
                response['result']['uncles'] = []
            return response
        return middleware
    
class Web3CC:
    def __init__(self, rpc_config):
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
        response_normalizer = ResponseNormalizerMiddleware(w3)
        w3.middleware_onion.inject(response_normalizer, layer=0)
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
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
        ## remove http:// or https://
        url = raw_url.split("//")[-1]

        return url