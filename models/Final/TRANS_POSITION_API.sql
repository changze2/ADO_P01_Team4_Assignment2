SELECT
    DATE,
    DRIVER_NUMBER,
    MEETING_KEY,
    POSITION,
    SESSION_KEY
FROM {{ ref('STG_POSITION_API') }}

{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}