import asyncio
import websockets
import json
import pandas as pd
from datetime import datetime
import os

os.makedirs("data", exist_ok=True)

url = "wss://stream.binance.com:9443/ws/btcusdt@trade"

trades = []

async def collect_trades():
    async with websockets.connect(url) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            trade = {
                "timestamp": data['T'],
                "price": float(data['p']),
                "qty": float(data['q'])
            }

            trades.append(trade)

            # Save every 1000 trades
            if len(trades) >= 1000:
                df = pd.DataFrame(trades)
                filename = f"data/trades_{datetime.now().date()}.parquet"
                if os.path.exists(filename) and os.path.getsize(filename) > 0:
                    existing_df = pd.read_parquet(filename)
                    df = pd.concat([existing_df, df], ignore_index=True)
                df.to_parquet(filename, index=False)
                trades.clear()

asyncio.run(collect_trades())