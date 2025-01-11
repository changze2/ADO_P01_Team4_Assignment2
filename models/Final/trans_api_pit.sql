
SELECT
    DATE,
    DRIVER_NUMBER,
    COALESCE(LAP_NUMBER, 0) AS LAP_NUMBER,
    MEETING_KEY,
    COALESCE(PIT_DURATION, 0) AS PIT_DURATION,
    SESSION_KEY
FROM {{ ref('stg_api_pit') }}

{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}
