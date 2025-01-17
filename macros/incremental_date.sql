{% macro incremental_date_comparison() %}
    WHERE DATE NOT IN (
        SELECT DATE
        FROM {{ this }}
    )
{% endmacro %}
