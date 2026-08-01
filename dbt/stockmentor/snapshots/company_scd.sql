{% snapshot company_scd %}

{{
    config(
        target_schema='main',
        unique_key='ticker',
        strategy='check',
        check_cols=['company_name', 'description', 'sector', 'industry', 'country'],
    )
}}

SELECT
    ticker,
    company_name,
    description,
    sector,
    industry,
    country
FROM {{ ref('stg_company_info') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY snapshot_date DESC) = 1

{% endsnapshot %}
