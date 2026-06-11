import asyncio
from collectors.trade_collector import collect_trades
from config.symbol import (
    TRADE_SYMBOLS, 
    EXCHANGE
)


async def main():

    tasks = []

    for symbol in TRADE_SYMBOLS:

        tasks.append(
            collect_trades(
                symbol,
                EXCHANGE
            )
        )

    await asyncio.gather(*tasks)


asyncio.run(main())