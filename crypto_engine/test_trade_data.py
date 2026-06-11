# test_trade_data.py

import pandas as pd

df = pd.read_parquet(
    r"C:\Users\Arnv\Documents\CRYPTO\crypto_engine\market_data\crypto\binance\trades\BTCUSDT\2026\06\11\140501.parquet"
)

print(df.tail(100))
print(df.shape)