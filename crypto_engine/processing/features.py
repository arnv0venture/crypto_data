# processing/features.py

import pandas as pd

df = pd.read_parquet(
    "market_data/crypto/binance/orderbook/BTCUSDT/2026/06/11.parquet"
)

df["best_bid"] = (
    df["bids"]
    .apply(lambda x: x[0][0])
)

df["bid_size"] = (
    df["bids"]
    .apply(lambda x: x[0][1])
)

df["best_ask"] = (
    df["asks"]
    .apply(lambda x: x[0][0])
)

df["ask_size"] = (
    df["asks"]
    .apply(lambda x: x[0][1])
)

df["mid_price"] = (
    df["best_bid"]
    + df["best_ask"]
) / 2

df["spread"] = (
    df["best_ask"]
    - df["best_bid"]
)

df["imbalance"] = (
    (df["bid_size"] - df["ask_size"])
    /
    (df["bid_size"] + df["ask_size"])
)

df["microprice"] = (
    (
        df["best_bid"] * df["ask_size"]
        +
        df["best_ask"] * df["bid_size"]
    )
    /
    (
        df["bid_size"]
        +
        df["ask_size"]
    )
)

print(df.head())