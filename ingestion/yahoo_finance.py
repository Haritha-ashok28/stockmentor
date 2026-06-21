import yfinance as yf
from dotenv import load_dotenv
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path
from utils import setup_logger, load_config

logger = setup_logger()

load_dotenv()

BACKFILL_PERIOD = "2y"

def get_last_saved_date(ticker_symbol):
    ticker_folder = Path("bronze")/"stock_prices"/f"ticker={ticker_symbol}"
    if not ticker_folder.exists():
        return None
    existing_files = list(ticker_folder.rglob("*.parquet"))
    if not existing_files:
        return None
    dates = []
    for f in existing_files:
        df = pd.read_parquet(f)
        if not df.empty:
            dates.append(df.index.max())
    return max(dates) if dates else None
    
def main():
    config = load_config()
    tickers = config["tickers"]
    logger.info(f"Starting ingestion for {len(tickers)} tickers")
    successful = []
    for ticker_symbol in tickers:
        try:
            hist, info = fetch_stock_data(ticker_symbol)
            save_price_history(hist, ticker_symbol)
            save_company_info(info, ticker_symbol)
            logger.info(f"ingestion for {ticker_symbol} was successful")
            successful.append(ticker_symbol)
        except Exception as e:
            logger.error(f"{ticker_symbol} ingestion failed due to {e}")
    logger.info(f"Ingestion complete: {len(successful)}/{len(tickers)} tickers successful")


def fetch_stock_data(ticker_symbol, retries=3, delay=10):
    last_saved_date = get_last_saved_date(ticker_symbol)
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(ticker_symbol)
            if last_saved_date is None:
                logger.info(f"No existing history for {ticker_symbol}, backfilling {BACKFILL_PERIOD}")
                hist = ticker.history(period=BACKFILL_PERIOD)
            else:
                start_date = last_saved_date + timedelta(days=1)
                if start_date.date() >= datetime.now().date():
                    logger.info(f"{ticker_symbol} already up to date, nothing new to fetch")
                    hist = pd.DataFrame()
                else:
                    hist = ticker.history(start=start_date.strftime("%Y-%m-%d"))
            
            if hist.empty and last_saved_date is not None:
                info = ticker.info
                break

            if hist.empty:
                logger.warning(f"No historical data returned for {ticker_symbol}")
                raise ValueError(f"No historical data for {ticker_symbol}")
            
            info = ticker.info
            break

        except Exception as e:
            logger.error(f"attempt failed due to {e}")
            
            if attempt == retries - 1:
                logger.error("all retries failed")
                raise ValueError(f"all retries failed for {ticker_symbol}")
            
            time.sleep(delay)
    return hist, pd.DataFrame([info])


def save_price_history(hist, ticker_symbol):
    if hist.empty:
        logger.info(f"No new price rows to save for {ticker_symbol}")
        return
    for (year,month,date), daily_data in hist.groupby([hist.index.year,hist.index.month,hist.index.day]):
        folder = (
            Path('bronze')
            /"stock_prices"
            /f"ticker={ticker_symbol}"
            /f"year={year}"
            /f"month={month:02d}"
            /f"date={date:02d}"
        )
        folder.mkdir(parents=True, exist_ok=True)
        file_name = f"prices_{year}_{month:02d}_{date:02d}.parquet"
        daily_data.to_parquet(folder/file_name)
        logger.info(f"{ticker_symbol} {year}-{month:02d}-{date:02d} saved to {folder/file_name}")

def save_company_info(info_df, ticker_symbol):
    today = datetime.now()
    folder = (
        Path('bronze')
        / "company_info"
        / f"ticker={ticker_symbol}"
        / f"year={today.year}"
        / f"month={today.month:02d}"
        / f"date={today.day:02d}"
    )
    folder.mkdir(parents=True, exist_ok=True)
    file_name = f"info_{today.year}_{today.month:02d}_{today.day:02d}.parquet"
    info_df.to_parquet(folder / file_name)
    logger.info(f"{ticker_symbol} company info snapshot saved to {folder/file_name}")

if __name__ == "__main__":
    main()

