from enum import Enum

class Pricer(Enum):
    QUARTERLY_STOCK_FINANCIAL = "QUARTERLY_STOCK_FINANCIAL"
    WEEKLY_STOCK_SPECULATION = "WEEKLY_STOCK_SPECULATION"
    WEEKLY_STOCK_ROLLING = "WEEKLY_STOCK_ROLLING"
    WEEKLY_STOCK_WINDOW = "WEEKLY_STOCK_WINDOW"
    WEEKLY_CRYPTO_SPECULATION = "WEEKLY_CRYPTO_SPECULATION"
    WEEKLY_CRYPTO_ROLLING = "WEEKLY_CRYPTO_ROLLING"
    WEEKLY_CRYPTO_WINDOW = "WEEKLY_CRYPTO_WINDOW"
    DAILY_STOCK_SPECULATION = "DAILY_STOCK_SPECULATION"
    DAILY_STOCK_ROLLING = "DAILY_STOCK_ROLLING"
    DAILY_STOCK_WINDOW = "DAILY_STOCK_WINDOW"
    DAILY_CRYPTO_SPECULATION = "DAILY_CRYPTO_SPECULATION"
    DAILY_CRYPTO_ROLLING = "DAILY_CRYPTO_ROLLING"
    DAILY_CRYPTO_WINDOW = "DAILY_CRYPTO_WINDOW"