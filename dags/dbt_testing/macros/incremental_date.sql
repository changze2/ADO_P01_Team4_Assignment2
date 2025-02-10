{% macro incremental_date_comparison() %} 
    {% if is_incremental() %} 
        WHERE DATE > (SELECT MAX(DATE) FROM {{ this }}) 
    {% endif %} 
{% endmacro %}