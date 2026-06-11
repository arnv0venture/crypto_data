import asyncio

from collectors.orderbook_collector import collect_orderbook

from config.symbol import (
    ORDERBOOK_SYMBOLS,
    EXCHANGE
)


async def main():

    tasks = []

    for symbol in ORDERBOOK_SYMBOLS:

        tasks.append(
            collect_orderbook(
                symbol,
                EXCHANGE
            )
        )

    await asyncio.gather(*tasks)


asyncio.run(main())