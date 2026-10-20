class RedisKeys:
    """Redis key helpers shared across realtime services."""

    # Global keys
    GLOBAL_TPS_ATH = "global:tps:ath"
    GLOBAL_TPS_24H = "global:tps:24h_high"
    TPS_HISTORY_24H = "global:tps:history_24h"
    ATH_HISTORY = "global:tps:ath_history"

    # Live ETH/USD price (onchain, Base)
    ETH_PRICE_STREAM = "eth:price"          # stream with the last ~100 prices
    ETH_PRICE_LATEST = "eth:price:latest"   # json with the latest price and its status

    @staticmethod
    def chain_stream(chain_name: str) -> str:
        return f"chain:{chain_name}"

    @staticmethod
    def chain_tps_ath(chain_name: str) -> str:
        return f"chain:{chain_name}:tps:ath"

    @staticmethod
    def chain_tps_24h(chain_name: str) -> str:
        return f"chain:{chain_name}:tps:24h_high"

    @staticmethod
    def chain_tps_history_24h(chain_name: str) -> str:
        return f"chain:{chain_name}:tps:history_24h"

    @staticmethod
    def chain_ath_history(chain_name: str) -> str:
        return f"chain:{chain_name}:tps:ath_history"


__all__ = ["RedisKeys"]
