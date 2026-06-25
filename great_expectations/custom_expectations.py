"""
Custom Great Expectations for the StockMentor pipeline.

These cover business rules that span multiple columns —
GE's built-in expectations only handle single columns.
"""

from great_expectations.expectations import UnexpectedRowsExpectation


class ExpectOHLCConsistency(UnexpectedRowsExpectation):
    """
    Tier 2 (Business Rule) for yahoo_finance / stock_prices.

    Valid OHLC data satisfies:
      - Low  <= min(Open, Close)
      - High >= max(Open, Close)

    Any row violating these relationships indicates corrupted
    or misaligned price data from the source.
    """

    description = "Low must be <= min(Open, Close) and High must be >= max(Open, Close)"

    unexpected_rows_query = """
        SELECT *
        FROM {batch}
        WHERE "Low"  > "Open"
           OR "Low"  > "Close"
           OR "High" < "Open"
           OR "High" < "Close"
    """
