{{
    config(
        materialized = 'incremental',
        unique_key = ['ticker', 'snapshot_date']
    )
}}

WITH company_detail as (
    SELECT
        longName as company_name,
        longBusinessSummary as description,
        sectorDisp as sector,
        industryDisp as industry,
        country as country,
        currentPrice as current_price,
        trailingPE as trailing_pe,
        forwardPE as forward_pe,
        CAST(totalRevenue AS BIGINT) as revenue,
        trailingEPS as trailing_eps,
        forwardEPS as forward_eps,
        profitMargins as profit_margin,
        returnOnEquity as return_on_equity,
        returnOnAssets as return_on_assets,
        revenueGrowth as revenue_growth,
        earningsGrowth as earnings_growth,
        debtToEquity as debt_to_equity,
        quickRatio as quick_ratio,
        dividendYield as dividend_yield,
        CAST(marketCap AS BIGINT) as market_cap,
        beta as beta,
        MAKE_DATE(year, CAST(month AS INT), CAST(date AS INT)) as snapshot_date,
        currency as currency,
        ticker as ticker
    FROM
    {{source('bronze','company_info')}}
)
SELECT *
FROM company_detail
{{ incremental_filter('snapshot_date') }}
