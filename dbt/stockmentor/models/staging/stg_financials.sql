{{
    config(
        materialized = 'incremental',
        unique_key = ['ticker', 'fiscal_year', 'statement', 'label']
    )
}}

WITH financials_detail as (
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
)
SELECT *
FROM financials_detail
{{ incremental_filter('fiscal_year') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ticker, statement, label, fiscal_year
    ORDER BY confidence_score DESC
) = 1
