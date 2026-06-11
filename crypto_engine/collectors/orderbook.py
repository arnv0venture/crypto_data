# collectors/orderbook.py

import asyncio
import json
import requests
import pandas as pd
from crypto_engine.config import symbol
import websockets
import time


from utils.storage import get_trade_path

FLUSH_INTERVAL = 20

SYMBOL = "BTCUSDT"
EXCHANGE = "binance"

SNAPSHOT_URL = (
    f"https://api.binance.com/api/v3/depth"
    f"?symbol={SYMBOL}&limit=1000"
)

WS_URL = (
    f"wss://stream.binance.com:9443/ws/"
    f"{SYMBOL.lower()}@depth@100ms"
)

snapshot = requests.get(SNAPSHOT_URL).json()

last_update_id = snapshot["lastUpdateId"]

bids = {
    float(p): float(q)
    for p, q in snapshot["bids"]
}

asks = {
    float(p): float(q)
    for p, q in snapshot["asks"]
}

records = []

last_flush = time.time()

async def collect_orderbook():

    global last_update_id

    async with websockets.connect(WS_URL) as ws:

        while True:

            msg = await ws.recv()

            data = json.loads(msg)

            if data["u"] <= last_update_id:
                continue

            last_update_id = data["u"]

            for p, q in data["b"]:

                p = float(p)
                q = float(q)

                if q == 0:
                    bids.pop(p, None)
                else:
                    bids[p] = q

            for p, q in data["a"]:

                p = float(p)
                q = float(q)

                if q == 0:
                    asks.pop(p, None)
                else:
                    asks[p] = q

            best_bids = sorted(
                bids.items(),
                reverse=True
            )[:10]

            best_asks = sorted(
                asks.items()
            )[:10]

            records.append({
                "timestamp": data["E"],
                "symbol": SYMBOL,
                "bids": best_bids,
                "asks": best_asks
            })

            if time.time() - last_flush >= FLUSH_INTERVAL:

                if records:
                
                    df = pd.DataFrame(records)

                    filename = get_trade_path(
                        EXCHANGE,
                        "orderbook",
                        SYMBOL
                    )

                    df.to_parquet(
                        filename,
                        compression="snappy",
                        index=False
                    )

                    print(
                        f"{SYMBOL}: saved "
                        f"{len(df)} orderbook updates"
                    )

                    records.clear()

                    last_flush = time.time()

asyncio.run(collect_orderbook())