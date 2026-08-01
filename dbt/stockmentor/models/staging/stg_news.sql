{{
    config(
        materialized = 'incremental',
        unique_key = ['ticker', 'article_id']
    )
}}

WITH news_detail as (
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
)
SELECT *
FROM news_detail
{{ incremental_filter('published_at') }}
