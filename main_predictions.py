from pricer.pricer import Pricer
from pricer.pricer_factory import PricerFactory
from datetime import datetime
from processor.processor import Processor as p

pricers = []
pricers.append(Pricer.DAILY_STOCK_WINDOW)
pricers.append(Pricer.DAILY_STOCK_ROLLING)
pricers.append(Pricer.WEEKLY_STOCK_WINDOW)
pricers.append(Pricer.WEEKLY_STOCK_ROLLING)
pricers.append(Pricer.MONTHLY_STOCK_WINDOW)
pricers.append(Pricer.MONTHLY_STOCK_ROLLING)

backtest_start_date = datetime(2020,1,1)
backtest_end_date = datetime(2023,1,1)
current_year = datetime.now().year

## initializing pricer_class
for pricer_enum in pricers:
    try:
        status = "initialize"
        pricer_class = PricerFactory.build(pricer_enum)
        pricer_class.initialize()
        pricer_class.drop_predictions()
        status = "sim"
        test_sim = pricer_class.create_predictions()
    except Exception as e:
        print(status,pricer_enum,str(e))