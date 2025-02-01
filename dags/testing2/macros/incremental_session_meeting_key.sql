{% macro incremental_session_meeting_key() %}
    WHERE (SESSION_KEY, MEETING_KEY) NOT IN (
        SELECT SESSION_KEY, MEETING_KEY
        FROM {{ this }}
    )
{% endmacro %}
