{% macro incremental_meeting_key() %}
    WHERE MEETING_KEY NOT IN (
        SELECT MEETING_KEY
        FROM {{ this }}
    )
{% endmacro %}
