import pandas as pd

df = pd.read_parquet(r"C:\Users\Arnv\Documents\CRYPTO\crypto_engine\market_data\crypto\binance\orderbook\BTCUSDT\2026\06\11\20260611_165824.parquet")

print(df.columns)
print(df.head())
print(df.shape)