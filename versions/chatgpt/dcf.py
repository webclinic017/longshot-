# Import necessary libraries
import pandas as pd
import numpy as np

# Define inputs
forecast_period = 10
discount_rate = 0.1

# Load financial statements data
income_statement = pd.read_csv('income_statement.csv')
balance_sheet = pd.read_csv('balance_sheet.csv')
cash_flow_statement = pd.read_csv('cash_flow_statement.csv')

# Calculate free cash flows
ebit = income_statement['EBIT']
tax_rate = income_statement['Tax Rate']
depreciation = cash_flow_statement['Depreciation']
capital_expenditure = cash_flow_statement['Capital Expenditure']
working_capital = balance_sheet['Current Assets'] - balance_sheet['Current Liabilities']

free_cash_flow = ebit * (1 - tax_rate) + depreciation - capital_expenditure - working_capital

# Calculate terminal value
last_year_free_cash_flow = free_cash_flow[-1]
terminal_growth_rate = 0.02
terminal_value = (last_year_free_cash_flow * (1 + terminal_growth_rate)) / (discount_rate - terminal_growth_rate)

# Calculate present value of forecast period cash flows
discount_factors = np.power(1 + discount_rate, range(1, forecast_period + 1))
present_values = free_cash_flow[:forecast_period] / discount_factors
present_value_forecast_period = present_values.sum()

# Calculate present value of terminal value
present_value_terminal_value = terminal_value / np.power(1 + discount_rate, forecast_period)

# Calculate intrinsic value
intrinsic_value = present_value_forecast_period + present_value_terminal_value
print(f'The intrinsic value of the stock is {intrinsic_value:.2f}')
