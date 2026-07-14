{{
    config(
        materialized = 'incremental',
        unique_key = ['ticker', 'fiscal_year', 'statement', 'label']
    )
}}

SELECT
    label as label,
    section as section,
    confidence as confidence_score,
    CAST(RIGHT(fiscal_year,4) AS INT) as fiscal_year,
    value as value,
    year as ingestion_year,
    statement as statement,
    ticker as ticker
FROM
{{source('bronze','financials')}}
{% if is_incremental() %}
WHERE CAST(RIGHT(fiscal_year,4) AS INT) > (SELECT MAX(fiscal_year) FROM {{ this }})
{% endif %}


