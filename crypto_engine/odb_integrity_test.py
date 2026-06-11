# Stage 6: Orderbook Integrity Test

import pandas as pd

df = pd.read_parquet(
    "market_data/crypto/binance/orderbook/BTCUSDT/2026/06/11.parquet"
)





print(df.head())

print(df.iloc[0]["bids"])
print(df.iloc[0]["asks"])