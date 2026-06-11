import asyncio
import websockets
import json
import requests
import pandas as pd
from datetime import datetime
import os

os.makedirs("data", exist_ok=True)

# -----------------------------
# STEP 1: GET SNAPSHOT
# -----------------------------
url = "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
snapshot = requests.get(url).json()

last_update_id = snapshot['lastUpdateId']

bids = {float(price): float(qty) for price, qty in snapshot['bids']}
asks = {float(price): float(qty) for price, qty in snapshot['asks']}

print("Snapshot loaded")

# -----------------------------
# STORAGE
# -----------------------------
records = []

# -----------------------------
# STEP 2: WEBSOCKET
# -----------------------------
WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms"

async def process():
    global last_update_id

    async with websockets.connect(WS_URL) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            # Ignore outdated updates
            if data['u'] <= last_update_id:
                continue

            # Update last_update_id
            last_update_id = data['u']

            # -----------------------------
            # APPLY BIDS
            # -----------------------------
            for price, qty in data['b']:
                price = float(price)
                qty = float(qty)

                if qty == 0:
                    bids.pop(price, None)
                else:
                    bids[price] = qty

            # -----------------------------
            # APPLY ASKS
            # -----------------------------
            for price, qty in data['a']:
                price = float(price)
                qty = float(qty)

                if qty == 0:
                    asks.pop(price, None)
                else:
                    asks[price] = qty

            # -----------------------------
            # EXTRACT TOP 5
            # -----------------------------
            best_bids = sorted(bids.items(), reverse=True)[:5]
            best_asks = sorted(asks.items())[:5]

            record = {
                "timestamp": data['E'],
                "bids": best_bids,
                "asks": best_asks
            }

            records.append(record)

            # -----------------------------
            # SAVE DATA
            # -----------------------------
            if len(records) >= 500:
                df = pd.DataFrame(records)
                filename = f"data/recon_orderbook_{datetime.now().date()}.parquet"
                if os.path.exists(filename) and os.path.getsize(filename) > 0:
                    existing_df = pd.read_parquet(filename)
                    df = pd.concat([existing_df, df], ignore_index=True)
                df.to_parquet(filename, index=False)
                records.clear()

            print("Updated OB", best_bids[0], best_asks[0])

asyncio.run(process())