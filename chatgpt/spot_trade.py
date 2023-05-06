import pandas as pd
import talib

# Collect historical data for currency pair
df = pd.read_csv('currency_pair_data.csv')

def calculate_RSI(data, period):
    # Calculate differences in price
    delta = data['Close'].diff()
    
    # Separate gains and losses
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    # Calculate average gains and losses
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    # Calculate RSI
    RS = avg_gain / avg_loss
    RSI = 100 - (100 / (1 + RS))
    
    return RSI

# Calculate technical indicators
df['RSI'] = talib.RSI(df['Close'])
df['MA10'] = talib.MA(df['Close'], timeperiod=10)
df['MA20'] = talib.MA(df['Close'], timeperiod=20)

# Define trading strategy
df['Signal'] = 0
df.loc[df['RSI'] < 30, 'Signal'] = 1
df.loc[df['RSI'] > 70, 'Signal'] = -1

# Backtest trading strategy
df['Position'] = df['Signal'].shift(1)
df['Returns'] = df['Position'] * df['Close'].pct_change()
df['Cumulative Returns'] = (1 + df['Returns']).cumprod()

# Execute trades
# Code for executing trades will depend on the specific brokerage or trading platform being used
