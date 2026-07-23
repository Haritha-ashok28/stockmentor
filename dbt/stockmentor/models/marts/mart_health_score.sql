{{
    config(
        materialized = 'table'
    )
}}

WITH latest_company as(
    SELECT * FROM {{ref('int_company_metrics')}}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY snapshot_date DESC) = 1
),
latest_financials as(
    SELECT * FROM {{ref('int_financials_pivot')}}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY fiscal_year DESC) = 1
),
scores as(
    SELECT
    c.ticker,
    c.sector,
    CASE WHEN c.profit_margin_pct > 10 THEN 1 ELSE 0 END AS profit_margin_score,
    CASE WHEN c.return_on_equity_pct > 15 THEN 1 ELSE 0 END AS return_on_equity_score,
    CASE WHEN c.trailing_pe < 25 THEN 1 ELSE 0 END AS trailing_pe_score,
    CASE WHEN c.debt_to_equity < 100 THEN 1 ELSE 0 END AS debt_to_equity_score,
    CASE WHEN c.quick_ratio > 1.0 THEN 1 ELSE 0 END AS quick_ratio_score,
    CASE WHEN c.dividend_yield > 1 THEN 1 ELSE 0 END AS dividend_yield_score,
    CASE WHEN c.revenue_growth_pct > 5 THEN 1 ELSE 0 END AS revenue_growth_score,
    CASE WHEN c.earnings_growth_pct > 5 THEN 1 ELSE 0 END AS earnings_growth_score,
    CASE WHEN f.free_cash_flow > 0 THEN 1 ELSE 0 END AS free_cash_flow_score
FROM latest_company c
LEFT JOIN latest_financials f 
ON c.ticker = f.ticker
)
SELECT
    ticker,
    CASE
    WHEN s.sector = 'Financial Services' THEN ROUND(((profit_margin_score + return_on_equity_score + trailing_pe_score + dividend_yield_score + revenue_growth_score + earnings_growth_score)/6.0*9))
    WHEN s.sector IN ('Energy', 'Industrials') THEN ROUND(((profit_margin_score + return_on_equity_score + trailing_pe_score + dividend_yield_score + revenue_growth_score + earnings_growth_score + free_cash_flow_score)/7.0*9))
    ELSE ROUND(profit_margin_score + return_on_equity_score + trailing_pe_score + debt_to_equity_score + quick_ratio_score + dividend_yield_score + revenue_growth_score + earnings_growth_score + free_cash_flow_score) 
    END as total_score,
    CASE 
    WHEN total_score >= 7 THEN 'Strong'
    WHEN total_score >= 4 THEN 'Moderate'
    ELSE 'Weak'
END as health_label 
FROM scores s