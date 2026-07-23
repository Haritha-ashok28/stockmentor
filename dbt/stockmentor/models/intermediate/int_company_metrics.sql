{{
    config(
        materialized = 'incremental',
        unique_key = ['ticker', 'snapshot_date']
    )
}}
SELECT 
    company_name,
    description,
    sector,
    industry,
    country,
    current_price,
    trailing_pe,
    forward_pe,
    revenue,
    trailing_eps,
    forward_eps,
    (profit_margin * 100) as profit_margin_pct,
    (return_on_equity * 100) as return_on_equity_pct,
    (return_on_assets * 100) as return_on_assets_pct, 
    (revenue_growth * 100) as revenue_growth_pct, 
    (earnings_growth * 100) as earnings_growth_pct,
    debt_to_equity,
    quick_ratio,
    dividend_yield,
    market_cap, 
    beta as beta, 
    snapshot_date,
    currency as currency,
    ticker as ticker
FROM
{{ref('stg_company_info')}}
{% if is_incremental() %}
WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{ this }})
{% endif %}