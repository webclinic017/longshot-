from enum import Enum

class States(Enum):
    BACKTEST = "backtest"
    SIM = "sim"
    DEPLOY = "deploy"
    LIVE = "live"
