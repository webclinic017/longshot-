from enum import Enum

class Ranker(Enum):
    QUARTERLY_STOCK_EARNINGS_RANKER = "QUARTERLY_STOCK_EARNINGS_RANKER"
    WEEKLY_STOCK_ROLLING_RANKER = "WEEKLY_STOCK_ROLLING_RANKER"
    WEEKLY_CRYPTO_FASTSLOW_RANKER = "WEEKLY_CRYPTO_FASTSLOW_RANKER"
    WEEKLY_STOCK_FASTSLOW_RANKER = "WEEKLY_STOCK_FASTSLOW_RANKER"
    NONE = "NONE"