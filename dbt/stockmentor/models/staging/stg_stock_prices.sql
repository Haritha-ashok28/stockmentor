{{
    config(
        materialized= 'incremental',
        unique_key= ['ticker', 'trade_date']
    )
}}

SELECT 
    CAST(Open AS DOUBLE) as open_price,
    CAST(High AS DOUBLE) as high_price,
    CAST(Low AS DOUBLE) as low_price,
    CAST(Close AS DOUBLE) as close_price,
    CAST(Volume AS BIGINT) as volume,
    CAST(Dividends AS DOUBLE) as dividends,
    CAST("Stock Splits" AS DOUBLE) as stock_splits,
    CAST(ticker AS VARCHAR) as ticker,
    MAKE_DATE(year, CAST(month as INT), CAST(DATE AS INT)) as trade_date
FROM
{{source('bronze', 'stock_prices')}}
{% if is_incremental() %}
WHERE MAKE_DATE(year, CAST(month as INT), CAST(DATE AS INT)) > (SELECT MAX(trade_date) FROM {{ this }})
{% endif %}