# utils/storage.py

from pathlib import Path
from datetime import datetime

BASE_DIR = Path("market_data/crypto")


def get_path(exchange: str,
             data_type: str,
             symbol: str):

    now = datetime.utcnow()

    path = (
        BASE_DIR
        / exchange.lower()
        / data_type.lower()
        / symbol.upper()
        / str(now.year)
        / f"{now.month:02d}"
    )

    path.mkdir(parents=True, exist_ok=True)

    return path / f"{now.day:02d}.parquet"