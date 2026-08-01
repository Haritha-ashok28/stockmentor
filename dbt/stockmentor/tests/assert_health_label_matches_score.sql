-- Singular test: health_label must always agree with the total_score
-- thresholds used in mart_health_score.sql (>=7 Strong, >=4 Moderate, else Weak).
-- A generic accepted_values/accepted_range test can't catch this class of bug -
-- it needs a business-logic assertion. dbt fails this test if it returns any rows.

SELECT
    ticker,
    total_score,
    health_label
FROM {{ ref('mart_health_score') }}
WHERE
    (total_score >= 7 AND health_label != 'Strong')
    OR (total_score >= 4 AND total_score < 7 AND health_label != 'Moderate')
    OR (total_score < 4 AND health_label != 'Weak')
