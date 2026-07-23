{{
    config(
        materialized = "table"
    )
}}
SELECT * FROM {{ref('int_company_metrics')}}
QUALIFY ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY snapshot_date DESC) = 1