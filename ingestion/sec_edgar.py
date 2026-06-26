import os
import sys
import re
import time
from pathlib import Path
from datetime import date
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "great_expectations"))
from validate import build_context, validate_source, check_freshness
from quarantine import write_to_quarantine

from utils import setup_logger, load_config
logger = setup_logger()

from edgar import Company
from edgar import set_identity
name = os.getenv("EDGAR_USER_NAME")
email = os.getenv("EDGAR_EMAIL")
set_identity(f"{name} {email}")

def fetch_financial_statements(ticker_symbol, retries = 3, delay = 10):
    for attempt in range(retries):
        try:
            company = Company(ticker_symbol)
            income = company.income_statement().to_dataframe()
            if income.empty:
                logger.warning(f"no data returned for {ticker_symbol}")
                raise ValueError(f"no data for {ticker_symbol}")
            balance = company.balance_sheet().to_dataframe()
            if balance.empty:
                logger.warning(f"no data returned for {ticker_symbol}")
                raise ValueError(f"no data for {ticker_symbol}")
            cash = company.cash_flow_statement().to_dataframe()
            if cash.empty:
                logger.warning(f"no data returned for {ticker_symbol}")
                raise ValueError(f"no data for {ticker_symbol}")
            break
        except Exception as e:
            logger.error(f"{ticker_symbol}'s financial statement failed due to {e}")

            if attempt == retries - 1:
                logger.error("all retires failed")
                raise ValueError(f"all retries failed for {ticker_symbol}")
            time.sleep(delay)
    return income, balance, cash

def melt_statement(df: "pd.DataFrame") -> "pd.DataFrame":
    """Convert a wide financial statement DataFrame to long format."""
    import pandas as pd
    df_long = df.melt(
        id_vars=['label', 'depth', 'is_abstract', 'is_total', 'section', 'confidence'],
        var_name='fiscal_year',
        value_name='value'
    )
    df_long['year'] = (
        df_long['fiscal_year']
        .str.extract(r'(\d{4})')[0]
        .astype(int)
    )
    df_long = df_long.dropna(subset=['value'])
    return df_long


def save_to_parquet(ticker_symbol: str, statement_name: str, df_long: "pd.DataFrame") -> None:
    """Partition and save a pre-melted long-format statement to bronze."""
    folder = Path("bronze") / "financials"
    for year, partition_df in df_long.groupby("year"):
        partition_path = (
            folder
            / f"ticker={ticker_symbol}"
            / f"statement={statement_name}"
            / f"year={year}"
        )
        partition_path.mkdir(parents=True, exist_ok=True)
        file_name = partition_path / f"{statement_name}_{year}.parquet"
        partition_df.to_parquet(file_name, index=False, engine="pyarrow")
        logger.info(f"{ticker_symbol} {year} saved to {file_name}")

def main():
    config = load_config()
    tickers = config["tickers"]
    logger.info(f"Starting ingestion for {len(tickers)} tickers")

    gx_context = build_context()

    successful = []
    quarantined = []

    for ticker_symbol in tickers:
        try:
            income, balance, cash = fetch_financial_statements(ticker_symbol)

            # Melt wide -> long, then validate the shape that lands in bronze
            trading_date = date.today()
            ticker_passed = True

            # Freshness: most recent fiscal quarter must be within 120 days
            # Parse actual dates from fiscal_year strings like "Mar 28, 2026 (Q2)"
            income_long = melt_statement(income)
            fiscal_dates = pd.to_datetime(
                income_long["fiscal_year"].str.extract(r'^([^(]+)')[0].str.strip(),
                errors="coerce"
            )
            most_recent_date = fiscal_dates.max().date()
            if not check_freshness(most_recent_date, "sec_edgar", ticker_symbol, max_age_days=120):
                # 120 days = ~1 quarter — catches companies that stopped filing
                write_to_quarantine(income_long.assign(
                    failed_expectation="freshness",
                    failed_column="year",
                    source="sec_edgar",
                    ticker=ticker_symbol,
                    quarantined_at=pd.Timestamp.now(),
                ), "sec_edgar", ticker_symbol, trading_date)
                quarantined.append(ticker_symbol)
                continue

            statements = {
                "income": income,
                "balance": balance,
                "cash": cash,
            }
            for statement_name, df in statements.items():
                df_long = melt_statement(df)
                passed, bad_rows = validate_source(gx_context, "sec_edgar", df_long, ticker_symbol)
                if not passed:
                    write_to_quarantine(bad_rows, "sec_edgar", ticker_symbol, trading_date)
                    ticker_passed = False
                else:
                    save_to_parquet(ticker_symbol, statement_name, df_long)

            if not ticker_passed:
                logger.warning(f"{ticker_symbol} failed validation -- bad rows quarantined")
                quarantined.append(ticker_symbol)
                continue

            logger.info(f"ingestion for {ticker_symbol} was successful")
            successful.append(ticker_symbol)

        except Exception as e:
            logger.error(f"{ticker_symbol} ingestion failed due to {e}")

    logger.info(f"Ingestion complete: {len(successful)}/{len(tickers)} successful, {len(quarantined)} quarantined")

if __name__ == "__main__":
    main()
