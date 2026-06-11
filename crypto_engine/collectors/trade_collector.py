# collectors/trade_collector.py

import asyncio
import websockets
import json
import pandas as pd
from config.symbol import SYMBOLS
import time
 

from utils.storage import get_trade_path

FLUSH_INTERVAL = 20

async def collect_trades(symbol, exchange):

    ws_url = (
        f"wss://stream.binance.com:9443/ws/"
        f"{symbol.lower()}@trade"
    )

    buffer = []

    last_flush = time.time()

    async with websockets.connect(ws_url) as ws:

        while True:

            msg = await ws.recv()

            data = json.loads(msg)

            trade = {
                "timestamp": data["T"],
                "symbol": symbol,
                "price": float(data["p"]),
                "qty": float(data["q"])
            }

            buffer.append(trade)

            if time.time() - last_flush >= FLUSH_INTERVAL:

                if buffer:

                    df = pd.DataFrame(buffer)

                    filename = get_trade_path(
                        exchange,
                        "trades",
                        symbol
                    )

                    df.to_parquet(
                        filename,
                        compression="snappy",
                        index=False
                    )           

                    print(
                        f"{symbol}: saved "
                        f"{len(df)} trades"
                    )

                    buffer.clear()

                    last_flush = time.time()