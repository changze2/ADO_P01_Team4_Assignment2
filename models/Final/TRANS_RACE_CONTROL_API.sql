SELECT
    CATEGORY,
    DATE,
    COALESCE(DRIVER_NUMBER, 0) AS DRIVER_NUMBER,
    COALESCE(FLAG, 'NO FLAG') AS FLAG,
    COALESCE(LAP_NUMBER, 0) AS LAP_NUMBER,
    MEETING_KEY,
    MESSAGE,
    COALESCE(SCOPE, 'NO SCOPE') AS SCOPE,  
    COALESCE(SECTOR, 0) AS SECTOR, 
    SESSION_KEY
FROM {{ ref('STG_RACE_CONTROL_API') }}

{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}