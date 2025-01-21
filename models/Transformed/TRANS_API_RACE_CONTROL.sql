SELECT
    UPPER(CATEGORY) AS CATEGORY,
    DATE,
    DRIVER_NUMBER,
    COALESCE(FLAG, 'NO_FLAG') AS FLAG,
    COALESCE(LAP_NUMBER, 0) AS LAP_NUMBER,
    MEETING_KEY,
    MESSAGE,
    UPPER(COALESCE(SCOPE, 'NO_SCOPE')) AS SCOPE,  
    COALESCE(SECTOR, 0) AS SECTOR, 
    SESSION_KEY
FROM {{ ref('STG_API_RACE_CONTROL') }}

{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}