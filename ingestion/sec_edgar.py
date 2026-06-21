import os
import re
import time
from utils import setup_logger, load_config
logger = setup_logger()
from pathlib import Path
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

def save_to_parquet(ticker_symbol, income, balance, cash):
        statements = {
            "income":income,
            "balance":balance,
            "cash":cash
        }
        folder = Path("bronze") / f"financials"
        for statement_name, df in statements.items():
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

            for (year), partition_df in df_long.groupby("year"):   
                partition_path = (
                    folder
                    /f"ticker={ticker_symbol}"
                    /f"statement={statement_name}"
                    /f"year={year}"
                )
                partition_path.mkdir(parents=True, exist_ok=True)
                file_name = (
                    partition_path
                    /f"{statement_name}_{year}.parquet"
                )
                partition_df.to_parquet(
                file_name,
                index=False,
                engine="pyarrow"
                )
                logger.info(f"{ticker_symbol} {year} saved to {folder/file_name}")

def main():
    config = load_config()
    tickers = config["tickers"]
    logger.info(f"Starting ingestion for {len(tickers)} tickers")
    successful = []
    for ticker_symbol in tickers:
        try:
            income, balance, cash = fetch_financial_statements(ticker_symbol)
            save_to_parquet(ticker_symbol, income, balance, cash)
            logger.info(f"ingestion for {ticker_symbol} was successful")
            successful.append(ticker_symbol)
        except Exception as e:
            logger.error(f"{ticker_symbol} ingestion failed due to {e}")
    logger.info(f"Ingestion complete: {len(successful)}/{len(tickers)} tickers successful")

if __name__ == "__main__":
    main()