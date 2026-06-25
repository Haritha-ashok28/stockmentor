"""
quarantine.py — writes failed validation records to the bronze quarantine layer.

Quarantine path pattern:
    bronze/quarantine/source=<source>/ticker=<ticker>/date=<trading_date>/bad_records.parquet

The trading date (not pipeline run date) is used so bad records can be
correlated directly with the market session that produced them.
"""

from pathlib import Path
from datetime import date
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
QUARANTINE_ROOT = PROJECT_ROOT / "bronze" / "quarantine"


def write_to_quarantine(
    bad_rows: pd.DataFrame,
    source_name: str,
    ticker_symbol: str,
    trading_date: date = None,
) -> Path:
    """
    Persist bad rows to the quarantine layer.

    Parameters
    ----------
    bad_rows      : DataFrame returned by validate_source() — already has metadata columns
    source_name   : "yahoo_finance", "rss_news", "sec_edgar", or "company_info"
    ticker_symbol : e.g. "AAPL"
    trading_date  : the date of the data. Defaults to today if unknown.
    """
    if bad_rows.empty:
        return None

    if trading_date is None:
        trading_date = date.today()

    folder = (
        QUARANTINE_ROOT
        / f"source={source_name}"
        / f"ticker={ticker_symbol}"
        / f"date={trading_date.isoformat()}"
    )
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / "bad_records.parquet"

    if file_path.exists():
        existing = pd.read_parquet(file_path)
        bad_rows = pd.concat([existing, bad_rows], ignore_index=True).drop_duplicates()

    bad_rows.to_parquet(file_path, index=False)

    print(f"  [quarantine] {len(bad_rows)} bad row(s) written to {file_path.relative_to(PROJECT_ROOT)}")

    return file_path
