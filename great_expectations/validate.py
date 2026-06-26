"""
validate.py — importable data quality module for the StockMentor pipeline.

Designed to be called from ingestion scripts (yahoo_finance.py, rss_news.py,
sec_edgar.py) once per ticker, right after data lands in bronze.

Usage from ingestion scripts:
    from great_expectations.validate import build_context, validate_source

Usage as standalone smoke-test:
    python great_expectations/validate.py
"""

from pathlib import Path
from datetime import date
import pandas as pd
import great_expectations as gx
from great_expectations.data_context import FileDataContext
from great_expectations.expectations import UnexpectedRowsExpectation

# Import custom expectations so GE can find them when loading suites
from custom_expectations import ExpectOHLCConsistency  # noqa: F401

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE = PROJECT_ROOT / "bronze"


def build_context() -> FileDataContext:
    """
    Load the GE FileDataContext from great_expectations/.
    Call once at the top of each ingestion script and pass context down.
    """
    return gx.get_context(
        mode="file",
        context_root_dir=str(PROJECT_ROOT / "great_expectations"),
    )


def validate_source(
    context: FileDataContext,
    source_name: str,
    df: pd.DataFrame,
    ticker_symbol: str = None,
) -> tuple[bool, pd.DataFrame]:
    """
    Run the named GE suite against df.

    Parameters
    ----------
    context       : GE context (from build_context())
    source_name   : matches the suite name — "yahoo_finance", "rss_news", "sec_edgar", "company_info"
    df            : the DataFrame to validate (passed directly from ingestion, no disk read)
    ticker_symbol : used for logging and quarantine metadata

    Returns
    -------
    passed    : True if all expectations passed, False otherwise
    bad_rows  : empty DataFrame if passed, else bad rows with failure metadata attached
    """

    data_source = context.data_sources.add_or_update_pandas(f"{source_name}_ds")
    asset = data_source.add_dataframe_asset(f"{source_name}_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("batch")

    suite = context.suites.get(source_name)

    # Attach OHLC custom expectation at runtime for yahoo_finance
    if source_name == "yahoo_finance":
        _ensure_ohlc_expectation(suite, context)

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    result = batch.validate(suite, result_format="COMPLETE")

    label = f"[{source_name}]" + (f"[{ticker_symbol}]" if ticker_symbol else "")
    status = "PASSED" if result.success else "FAILED"
    print(f"{label} {status}")

    if not result.success:
        for er in result.results:
            if not er.success:
                print(f"  -> FAIL: {er.expectation_config.type} | col: {er.expectation_config.kwargs.get('column', 'multi-column')}")

    if result.success:
        return True, pd.DataFrame()

    bad_rows = _collect_bad_rows(df, result, source_name, ticker_symbol)
    return False, bad_rows


def check_freshness(
    max_date: date,
    source_name: str,
    ticker_symbol: str,
    max_age_days: int,
) -> bool:
    """
    Tier 3 freshness check — is the data recent enough to be trusted?

    Parameters
    ----------
    max_date      : most recent date in the fetched data
    source_name   : used for logging
    ticker_symbol : used for logging
    max_age_days  : how many calendar days old is acceptable (covers weekends/holidays)

    Returns
    -------
    True if fresh, False if stale (caller should quarantine and skip)
    """
    days_old = (date.today() - max_date).days
    label = f"[{source_name}][{ticker_symbol}]"

    if days_old > max_age_days:
        print(f"{label} STALE: latest data is {max_date} ({days_old} days old, max allowed: {max_age_days})")
        return False

    print(f"{label} FRESH: latest data is {max_date} ({days_old} days old)")
    return True


def _ensure_ohlc_expectation(suite, context):
    existing_types = [e.type for e in suite.expectations]
    if "expect_ohlc_consistency" not in existing_types:
        suite.add_expectation(ExpectOHLCConsistency())
        context.suites.update(suite)


def _collect_bad_rows(
    df: pd.DataFrame,
    result,
    source_name: str,
    ticker_symbol: str,
) -> pd.DataFrame:
    bad_indices = set()
    failure_reasons = {}

    for er in result.results:
        if er.success:
            continue

        exp_type = er.expectation_config.type
        column = er.expectation_config.kwargs.get("column", "multi-column")

        indices = er.result.get("unexpected_index_list", [])
        for idx in indices:
            bad_indices.add(idx)
            failure_reasons[idx] = {"failed_expectation": exp_type, "failed_column": column}

    if not bad_indices:
        bad_rows = df.copy()
        bad_rows["failed_expectation"] = "expect_ohlc_consistency"
        bad_rows["failed_column"] = "multi-column"
    else:
        bad_rows = df.iloc[list(bad_indices)].copy()
        bad_rows["failed_expectation"] = [failure_reasons[i]["failed_expectation"] for i in bad_indices]
        bad_rows["failed_column"] = [failure_reasons[i]["failed_column"] for i in bad_indices]

    bad_rows["source"] = source_name
    bad_rows["ticker"] = ticker_symbol
    bad_rows["quarantined_at"] = pd.Timestamp.now()

    return bad_rows


def main():
    """Smoke-test — validates one file per source."""
    context = build_context()
    results = {}

    prices_file = next((BRONZE / "stock_prices").rglob("*.parquet"))
    passed, _ = validate_source(context, "yahoo_finance", pd.read_parquet(prices_file), "SMOKE_TEST")
    results["yahoo_finance"] = passed

    news_file = next((BRONZE / "news").rglob("*.parquet"))
    passed, _ = validate_source(context, "rss_news", pd.read_parquet(news_file), "SMOKE_TEST")
    results["rss_news"] = passed

    fin_file = next((BRONZE / "financials").rglob("*.parquet"))
    passed, _ = validate_source(context, "sec_edgar", pd.read_parquet(fin_file), "SMOKE_TEST")
    results["sec_edgar"] = passed

    print("\n-- Summary --")
    for source, ok in results.items():
        print(f"  [{'OK' if ok else 'FAIL'}] {source}")

    if not all(results.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
