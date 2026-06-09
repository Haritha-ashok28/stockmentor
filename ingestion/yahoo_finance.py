import yfinance as yf
import yaml
from dotenv import load_dotenv
import pandas as pd
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def load_config():
    with open("config/tickers.yaml","r") as f:
        return yaml.safe_load(f)
    
def main():
    config = load_config()
    tickers = config["tickers"]
    print(tickers)
    for ticker_symbol in tickers:
        hist, info = fetch_stock_data(ticker_symbol)
        print(f"{ticker_symbol}:")
        print(hist.head())
        print(info.head())

def fetch_stock_data(ticker_symbol, retries=3, delay=10):
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(ticker_symbol)
            hist = ticker.history(period="1y")
            if hist.empty:
                logger.warning(f"No historical data returned for {ticker_symbol}")
            info = ticker.info
            break
        except Exception as e:
            logger.error(f"attempt failed due to {e}")
            
            if attempt == retries - 1:
                logger.error("all retries failed")
                raise ValueError(f"all retries failed for {ticker_symbol}")
            
            time.sleep(delay)
    return hist, pd.DataFrame([info])


if __name__ == "__main__":
    main()

