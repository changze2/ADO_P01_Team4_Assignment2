SELECT
    COALESCE(COMPOUND, 'NO_TYRE') AS COMPOUND,
    DRIVER_NUMBER,
    LAP_START, 
    LAP_END, 
    MEETING_KEY,
    SESSION_KEY,
    STINT_NUMBER,
    COALESCE(TYRE_AGE_AT_START, -1) AS TYRE_AGE_AT_START,
FROM {{ ref('STG_API_STINTS') }}
WHERE DRIVER_NUMBER IS NOT NULL

{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}