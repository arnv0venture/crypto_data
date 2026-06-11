# collectors/trades.py

import asyncio
import websockets
import json
import pandas as pd
import duckdb

from utils.storage import get_trade_path

SYMBOL = "BTCUSDT"
EXCHANGE = "binance"

WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@trade"

BUFFER_SIZE = 5000

buffer = []

con = duckdb.connect("database/crypto.duckdb")

con.execute("""
CREATE TABLE IF NOT EXISTS trades(
    timestamp BIGINT,
    symbol VARCHAR,
    price DOUBLE,
    qty DOUBLE
)
""")


async def collect_trades():

    async with websockets.connect(WS_URL) as ws:

        while True:

            msg = await ws.recv()

            data = json.loads(msg)

            trade = {
                "timestamp": data["T"],
                "symbol": SYMBOL,
                "price": float(data["p"]),
                "qty": float(data["q"])
            }

            buffer.append(trade)

            if len(buffer) >= BUFFER_SIZE:

                df = pd.DataFrame(buffer)

                con.register("temp_df", df)

                con.execute("""
                    INSERT INTO trades
                    SELECT * FROM temp_df
                """)

                filename = get_trade_path(
                    EXCHANGE,
                    "trades",
                    SYMBOL
                )

                df.to_parquet(
                    filename,
                    compression="snappy",
                    index=False
                )

                print(
                    f"Saved {len(df)} trades"
                )

                buffer.clear()


asyncio.run(collect_trades())