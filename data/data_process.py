import pandas as pd

# -------------------------
# LOAD DATA
# -------------------------
trades = pd.read_parquet("trades_2026-03-21.parquet")
orderbook = pd.read_parquet("recon_orderbook_2026-03-21.parquet")

# -------------------------
# FIX TIMESTAMP
# -------------------------
trades['timestamp'] = pd.to_datetime(trades['timestamp'], unit='ms')
orderbook['timestamp'] = pd.to_datetime(orderbook['timestamp'], unit='ms')

# -------------------------
# EXTRACT BEST BID / ASK (IMPORTANT STEP BEFORE FEATURES)
# -------------------------

# Your orderbook has nested lists → we extract first level

orderbook['best_bid'] = orderbook['bids'].apply(lambda x: x[0][0])
orderbook['bid_size'] = orderbook['bids'].apply(lambda x: x[0][1])

orderbook['best_ask'] = orderbook['asks'].apply(lambda x: x[0][0])
orderbook['ask_size'] = orderbook['asks'].apply(lambda x: x[0][1])

# -------------------------
# FEATURE ENGINEERING (THIS IS WHERE YOUR CODE GOES)
# -------------------------

# 1. Mid Price
orderbook['mid_price'] = (orderbook['best_bid'] + orderbook['best_ask']) / 2

# 2. Spread
orderbook['spread'] = orderbook['best_ask'] - orderbook['best_bid']

# 3. Orderbook Imbalance (VERY IMPORTANT)
orderbook['imbalance'] = (
    (orderbook['bid_size'] - orderbook['ask_size']) /
    (orderbook['bid_size'] + orderbook['ask_size'])
)

# -------------------------
# OUTPUT CHECK
# -------------------------
print(orderbook[['timestamp','best_bid','best_ask','mid_price','spread','imbalance']].head(51))