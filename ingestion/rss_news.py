import sys
import feedparser
import pandas as pd
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "great_expectations"))
from validate import build_context, validate_source, check_freshness
from quarantine import write_to_quarantine

from utils import setup_logger, load_config
logger = setup_logger()

def parse_entry(entry, ticker_symbol):
    return {
        "ticker": ticker_symbol,
        "id": entry.get("id"),
        "summary": entry.get("summary"),
        "title": entry.get("title"),
        "link": entry.get("link"),
        "published": entry.get("published"),
    }

def fetch_news(ticker_symbol, retries=3, delay=10):
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker_symbol}&region=US&lang=en-US"
    for attempt in range(retries):
        try:
            feed = feedparser.parse(url)
            if feed.bozo and not feed.entries:
                raise ValueError(f"RSS parse failed: {feed.bozo_exception}")
            parsed_data = []
            for entry in feed.entries:
                parsed_data.append(parse_entry(entry, ticker_symbol))
            if not parsed_data:
                logger.warning(f"no news returned for {ticker_symbol}")
                raise ValueError(f"no news returned for {ticker_symbol}")
            return pd.DataFrame(parsed_data)

        except Exception as e:
            logger.error(f"news feed failed for {ticker_symbol} due to {e}")
            if attempt == retries - 1:
                logger.error("all retries failed")
                raise ValueError(f"all retries failed for {ticker_symbol}")
            time.sleep(delay)

def save_to_parquet(ticker_symbol, news_df):
    news_df["published"] = pd.to_datetime(news_df["published"])
    for (year, month, day), daily_news in news_df.groupby([
        news_df["published"].dt.year,
        news_df["published"].dt.month,
        news_df["published"].dt.day
    ]):
        folder = (
            Path("bronze")
            / "news"
            / f"ticker={ticker_symbol}"
            / f"year={year}"
            / f"month={month:02d}"
            / f"date={day:02d}"
        )
        folder.mkdir(parents=True, exist_ok=True)
        file_name = f"news_{year}_{month:02d}_{day:02d}.parquet"
        file_path = folder / file_name

        if file_path.exists():
            existing_df = pd.read_parquet(file_path)
            combined_df = pd.concat([existing_df, daily_news], ignore_index=True)
        else:
            combined_df = daily_news

        combined_df = combined_df.drop_duplicates(subset=["id"], keep="last")
        combined_df.to_parquet(file_path, index=False)
        logger.info(
            f"Saved {len(daily_news)} new records "
            f"({len(combined_df)} total) to {file_path}"
        )

def main():
    config = load_config()
    tickers = config["tickers"]
    logger.info(f"Starting ingestion for {len(tickers)} tickers")

    gx_context = build_context()

    successful = []
    quarantined = []

    for ticker_symbol in tickers:
        try:
            news_df = fetch_news(ticker_symbol)
            save_to_parquet(ticker_symbol, news_df)

            trading_date = pd.to_datetime(news_df["published"]).max().date()

            if not check_freshness(trading_date, "rss_news", ticker_symbol, max_age_days=3):
                write_to_quarantine(news_df.assign(
                    failed_expectation="freshness",
                    failed_column="published",
                    source="rss_news",
                    ticker=ticker_symbol,
                    quarantined_at=pd.Timestamp.now(),
                ), "rss_news", ticker_symbol, trading_date)
                quarantined.append(ticker_symbol)
                continue

            passed, bad_rows = validate_source(gx_context, "rss_news", news_df, ticker_symbol)
            if not passed:
                write_to_quarantine(bad_rows, "rss_news", ticker_symbol, trading_date)
                logger.warning(f"{ticker_symbol} failed validation — bad rows quarantined")
                quarantined.append(ticker_symbol)
                continue

            logger.info(f"ingestion for {ticker_symbol} was successful")
            successful.append(ticker_symbol)

        except Exception as e:
            logger.error(f"{ticker_symbol} ingestion failed due to {e}")

    logger.info(f"Ingestion complete: {len(successful)}/{len(tickers)} successful, {len(quarantined)} quarantined")

if __name__ == "__main__":
    main()
