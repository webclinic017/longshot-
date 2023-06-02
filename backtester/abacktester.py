from returns.products import Products as returns_products_class
from database.market import Market

class ABacktester(object):

    def __init__(self,strat_class,current,positions,start_date,end_date):
        self.current = current
        self.tyields = returns_products_class.tyields()
        self.bench_returns = returns_products_class.spy_bench()
        self.start_date = start_date
        self.end_date = end_date
        self.positions = positions
        self.stocks = 10
        self.strat_class = strat_class
