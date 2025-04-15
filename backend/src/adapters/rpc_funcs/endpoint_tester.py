import json
import logging
import threading
import time
from web3 import Web3, HTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware
import math

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class EndpointTester:
    def __init__(self, url):
        """
        Initializes the EndpointTester instance with the given RPC URL and initializes 
        necessary attributes for tracking calls, latency, and errors.

        Args:
            url (str): The RPC URL to test.

        Attributes:
            w3: Web3 instance for making requests.
            error_occurred (bool): Tracks if any error has occurred during the test.
            calls_made (int): Tracks the total number of calls made.
            max_calls_per_sec (int): Tracks the maximum number of calls made per second.
            latency_sum (float): Tracks the sum of all latencies for calculating average latency.
            latencies_recorded (int): Number of successful latencies recorded.
            test_start_time (float): The start time of the test.
            calls_lock (threading.Lock): Lock for synchronizing call count updates.
            calls_last_check (int): Tracks the number of calls at the last rate check.
            last_check_time (float): The time of the last rate check.
            test_duration (int): Duration of the test in seconds.
        """
        self.url = url
        self.w3 = None
        self.error_occurred = False
        self.calls_made = 0
        self.max_calls_per_sec = 0
        self.latency_sum = 0
        self.latencies_recorded = 0  # Count of latencies measured
        self.test_start_time = time.time()
        self.calls_lock = threading.Lock()
        self.calls_last_check = 0
        self.last_check_time = self.test_start_time
        self.test_duration = 60
        logging.debug(f"EndpointTester initialized for URL: {url}")
        
    def connect_to_node(self):
        """
        Connects to the Ethereum node using the specified RPC URL.
        Injects the POA middleware for compatibility with certain chains (e.g., Geth POA).
        
        Logs whether the connection was successful or failed.
        """
        self.w3 = Web3(HTTPProvider(self.url))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        if self.w3.is_connected():
            logging.info("Web3 instance is connected to node: {}".format(self.w3.is_connected()))
        else:
            logging.error("Failed to connect to RPC node.")

    def make_request(self):
        """
        Makes a request to retrieve the latest block number from the Ethereum node.
        Measures and logs the latency of the request and updates the call count and latency sum.
        
        Handles any exceptions and logs errors if the request fails.
        """
        logging.debug("Making a request...")
        try:
            start_time = time.time()
            latest_block = self.w3.eth.block_number
            end_time = time.time()
            latency = end_time - start_time
            logging.info(f"Latest block number: {latest_block}, Latency: {latency:.3f} seconds")
            with self.calls_lock:
                self.calls_made += 1
                self.latency_sum += latency
                self.latencies_recorded += 1
            logging.debug(f"Request successful, total calls made: {self.calls_made}, Latency recorded")
        except Exception as e:
            self.error_occurred = True
            logging.error(f"Exception while making a request to {self.url}: {str(e)}")
            logging.debug("Request failed.")

    def calculate_average_latency(self):
        """
        Calculates the average latency of the successful requests made during the test.

        Returns:
            float: The average latency in seconds, rounded to 3 decimal places, or None if no latencies were recorded.
        """
        if self.latencies_recorded > 0:
            return round(self.latency_sum / self.latencies_recorded, 3)
        else:
            return None

    def check_rate_reset(self):
        """
        Checks the call rate and resets the tracking for the number of calls made per second. 
        Updates the maximum calls per second if the current rate exceeds the previous maximum.
        Resets the call count for the next period.
        """
        logging.debug("Checking rate and resetting if necessary...")
        current_time = time.time()
        elapsed_time = current_time - self.last_check_time
        
        calls_this_period = self.calls_made - self.calls_last_check
        if elapsed_time >= 1.0 or self.error_occurred:
            calls_per_sec_this_period = calls_this_period / max(elapsed_time, 1)
            if calls_per_sec_this_period > self.max_calls_per_sec:
                self.max_calls_per_sec = calls_per_sec_this_period
            self.calls_last_check = self.calls_made
            self.last_check_time = current_time
            logging.debug(f"Rate check and reset done, max_calls_per_sec: {self.max_calls_per_sec}")
            
    def run_test(self):
        """
        Runs the performance test for the specified RPC URL.
        Spawns multiple threads to make requests to the node, increasing the thread count over time to stress test the endpoint.
        
        Tracks the number of calls made, the maximum number of calls per second, and the average latency.
        Stops the test if an error occurs or the test duration is exceeded.

        Returns:
            dict: A dictionary containing test results including the RPC URL, maximum call count, 
                maximum calls per second, average latency, and error details (if any).
        """
        logging.debug("Starting test...")
        try:
            self.connect_to_node()
            if not self.w3:
                logging.info("Web3 connection failed, exiting test.")
                return {
                    "url": self.url,
                    "error": "Web3 connection failed",
                    "max_call_count": "error",
                    "max_calls_per_sec": "error",
                    "average_latency_sec": "error"
                }

            thread_count = 1
            while time.time() - self.test_start_time < self.test_duration and not self.error_occurred:
                logging.debug(f"Starting iteration with thread count: {thread_count}")
                threads = [threading.Thread(target=self.make_request) for _ in range(thread_count)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                self.check_rate_reset()
                if not self.error_occurred:
                    thread_count += 1
        except Exception as e:
            logging.error(f"An unexpected error occurred during the test: {e}")
            return {
                "url": self.url,
                "error": "Unexpected error occurred",
                "max_call_count": self.calls_made,
                "max_calls_per_sec": math.floor(self.max_calls_per_sec) if self.max_calls_per_sec is not None else None,
                "average_latency_sec": self.calculate_average_latency()
            }

        logging.debug("Test loop finished.")
        if not self.error_occurred:
            return {
                "url": self.url,
                "max_call_count": None,
                "max_calls_per_sec": None,
                "average_latency_sec": self.calculate_average_latency()
            }
        else:
            return {
                "url": self.url,
                "max_req": self.calls_made,
                "max_tps": math.floor(self.max_calls_per_sec) if self.max_calls_per_sec is not None else None,
                "average_latency_sec": self.calculate_average_latency()
            }


def main():
    """
    Main function to test multiple RPC endpoints for performance.
    For each endpoint, it initializes an EndpointTester instance, runs the test, and saves the results to a JSON file.
    
    Logs errors if any occur during testing or file writing.
    """
    logging.debug("Starting main function...")
    chain = "blast"
    endpoints = [
        "https://blastl2-mainnet.public.blastapi.io",
        "https://blast.blockpi.network/v1/rpc/public",
        "https://blast.gasswap.org",
        "https://rpc.blast.io",
        "https://blast.din.dev/rpc",
        "https://blast.rpc.hypersync.xyz",
    ]

    results = []

    try:
        for endpoint in endpoints:
            tester = EndpointTester(endpoint)
            result = tester.run_test()
            results.append(result)

        with open(chain + '_rpc_config.json', 'w') as f:
            json.dump(results, f, indent=2)
    except Exception as e:
        logging.error(f"An error occurred during testing or file writing: {e}")
    else:
        logging.info("Finished testing all endpoints.")

if __name__ == "__main__":
    main()
