{{
    config(
        materialized = 'incremental',
        unique_key = ['ticker', 'fiscal_year']
    )
}}

SELECT 
    ticker,
    fiscal_year,
    MAX(CASE WHEN label = 'Total Revenue' THEN value END) as total_revenue,
    MAX(CASE WHEN label = 'Net Income (Loss) Attributable to Parent' THEN value END) as net_income,
    MAX(CASE WHEN label = 'Net Cash Provided by (Used in) Operating Activities' THEN value END) as operating_cash_flow,
    MAX(CASE WHEN label = 'Payments to Acquire Property, Plant, and Equipment' THEN value END) as capex,
    (MAX(CASE WHEN label = 'Net Cash Provided by (Used in) Operating Activities' THEN value END) - MAX(CASE WHEN label = 'Payments to Acquire Property, Plant, and Equipment' THEN value END)) as free_cash_flow,
    MAX(CASE WHEN label = 'Assets' THEN value END) as total_assets,
    MAX(CASE WHEN label = 'Liabilities' THEN value END) as total_liabilities
FROM
    {{ref('stg_financials')}}

{% if is_incremental() %}
WHERE fiscal_year > (SELECT MAX(fiscal_year) FROM {{ this }})
{% endif %}

GROUP BY ticker, fiscal_year
