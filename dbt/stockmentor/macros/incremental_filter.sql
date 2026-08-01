{#
    Reusable incremental filter for staging models.
    Assumes the calling model has already computed a clean, final column
    (via a CTE) with the given name - this macro does not do any casting
    or date computation itself, it only compares that column against the
    max value already loaded into the target table.
#}
{% macro incremental_filter(column_name) %}
{% if is_incremental() %}
WHERE {{ column_name }} > (SELECT MAX({{ column_name }}) FROM {{ this }})
{% endif %}
{% endmacro %}
