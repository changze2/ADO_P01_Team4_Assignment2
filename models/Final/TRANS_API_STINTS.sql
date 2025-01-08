SELECT
    COMPOUND,
    DRIVER_NUMBER,
    LAP_START, 
    LAP_END, 
    MEETING_KEY,
    SESSION_KEY,
    STINT_NUMBER,
FROM {{ ref('STG_API_STINTS') }}

{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}