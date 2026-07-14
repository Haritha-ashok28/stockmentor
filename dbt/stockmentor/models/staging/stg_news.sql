
{{
    config(
        materialized = 'incremental',
        unique_key = 'article_id'
    )
}}

SELECT
    ticker as ticker,
    title as headline, 
    link as source_link,
    CAST(published AS TIMESTAMPTZ) AT TIME ZONE 'UTC' as published_at,
    source as source_name,
    COALESCE(id, link) as article_id,
    summary as summary
FROM
{{source('bronze','news')}}
{% if is_incremental() %}
WHERE CAST(published AT TIME ZONE 'UTC' AS DATETIME) > (SELECT MAX(published_at) FROM {{ this }})
{% endif %}


