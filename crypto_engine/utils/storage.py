from pathlib import Path
from datetime import datetime

BASE_DIR = Path("market_data/crypto")

def get_trade_path(
    exchange,
    data_type,
    symbol
):

    now = datetime.utcnow()

    path = (
        BASE_DIR
        / exchange
        / data_type
        / symbol
        / str(now.year)
        / f"{now.month:02d}"
        / f"{now.day:02d}"
    )

    path.mkdir(
        parents=True,
        exist_ok=True
    )

    timestamp = now.strftime(
        "%H%M%S"
    )

    return path / f"{timestamp}.parquet"