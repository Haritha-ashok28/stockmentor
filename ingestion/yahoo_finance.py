import yfinance as yf
import yaml
from dotenv import load_dotenv
import pandas as pd
import time
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logs_folder = Path("logs")
logs_folder.mkdir(parents=True, exist_ok=True)
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler(logs_folder / "ingestion.log")
logger.addHandler(stream_handler)
logger.addHandler(file_handler)


load_dotenv()

def load_config():
    with open("config/tickers.yaml","r") as f:
        return yaml.safe_load(f)
    
def main():
    config = load_config()
    tickers = config["tickers"]
    logger.info(f"Starting ingestion for {len(tickers)} tickers")
    successful = []
    for ticker_symbol in tickers:
        try:
            hist, info = fetch_stock_data(ticker_symbol)
            save_to_parquet(hist, ticker_symbol)
            logger.info(f"ingestion for {ticker_symbol} was successful")
            successful.append(ticker_symbol)
        except Exception as e:
            logger.error(f"{ticker_symbol} ingestion failed due to {e}")
    logger.info(f"Ingestion complete: {len(successful)}/{len(tickers)} tickers successful")


def fetch_stock_data(ticker_symbol, retries=3, delay=10):
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(ticker_symbol)
            hist = ticker.history(period="1y")
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


def save_to_parquet(hist, ticker_symbol):
    for (year,month,date), monthly_data in hist.groupby([hist.index.year,hist.index.month,hist.index.day]):
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
        monthly_data.to_parquet(folder/file_name)
        logger.info(f"{ticker_symbol} {year}-{month:02d}-{date:02d} saved to {folder/file_name}")

if __name__ == "__main__":
    main()

