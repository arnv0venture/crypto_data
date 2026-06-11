import asyncio
import json
import time
import requests
import pandas as pd
import websockets

from utils.storage import get_trade_path

FLUSH_INTERVAL = 20


async def collect_orderbook(symbol, exchange):

    snapshot_url = (
        f"https://api.binance.com/api/v3/depth"
        f"?symbol={symbol}&limit=1000"
    )

    # Use default depth stream (1 second)
    # More stable than @depth@100ms
    ws_url = (
        f"wss://stream.binance.com:9443/ws/"
        f"{symbol.lower()}@depth"
    )

    snapshot = requests.get(snapshot_url).json()

    last_update_id = snapshot["lastUpdateId"]

    bids = {
        float(price): float(qty)
        for price, qty in snapshot["bids"]
    }

    asks = {
        float(price): float(qty)
        for price, qty in snapshot["asks"]
    }

    records = []

    last_flush = time.time()

    while True:

        try:

            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10
            ) as ws:

                print(f"{symbol} connected")

                while True:

                    msg = await ws.recv()

                    data = json.loads(msg)

                    if len(records) % 10 == 0:
                        print(
                            symbol,
                            "updates received:",
                            len(records)
                        )

                    # Ignore old updates
                    if data["u"] <= last_update_id:
                        continue

                    last_update_id = data["u"]

                    # UPDATE BIDS

                    for price, qty in data["b"]:

                        price = float(price)
                        qty = float(qty)

                        if qty == 0:
                            bids.pop(price, None)
                        else:
                            bids[price] = qty

                    # UPDATE ASKS

                    for price, qty in data["a"]:

                        price = float(price)
                        qty = float(qty)

                        if qty == 0:
                            asks.pop(price, None)
                        else:
                            asks[price] = qty

                    # TOP 20 LEVELS

                    best_bids = sorted(
                        bids.items(),
                        reverse=True
                    )[:20]

                    best_asks = sorted(
                        asks.items()
                    )[:20]

                    if not best_bids or not best_asks:
                        continue

                    best_bid = best_bids[0][0]
                    best_ask = best_asks[0][0]

                    # Data integrity check

                    if best_bid >= best_ask:
                        continue

                    mid_price = (
                        best_bid + best_ask
                    ) / 2

                    spread = (
                        best_ask - best_bid
                    )

                    records.append({
                        "timestamp": data["E"],
                        "update_id": data["u"],
                        "symbol": symbol,

                        "best_bid": best_bid,
                        "best_ask": best_ask,

                        "mid_price": mid_price,
                        "spread": spread,

                        "bids": best_bids,
                        "asks": best_asks
                    })

                    # TIME BASED FLUSH

                    if (
                        time.time() - last_flush
                        >= FLUSH_INTERVAL
                    ):

                        if records:

                            print(
                                f"{symbol} records before save:",
                                len(records)
                            )

                            df = pd.DataFrame(records)

                            filename = get_trade_path(
                                exchange,
                                "orderbook",
                                symbol
                            )

                            df.to_parquet(
                                filename,
                                compression="snappy",
                                index=False
                            )

                            print(
                                f"{symbol}: saved "
                                f"{len(df)} orderbook updates"
                            )

                            records.clear()

                            last_flush = time.time()

        except Exception as e:

            print(
                f"{symbol} disconnected:",
                e
            )

            await asyncio.sleep(5)

            print(
                f"{symbol} reconnecting..."
            )