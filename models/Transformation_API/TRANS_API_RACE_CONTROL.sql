SELECT
    CATEGORY,
    DATE,
    DRIVER_NUMBER,
    COALESCE(FLAG, 'NO FLAG') AS FLAG,
    COALESCE(LAP_NUMBER, 0) AS LAP_NUMBER,
    MEETING_KEY,
    MESSAGE,
    COALESCE(SCOPE, 'NO SCOPE') AS SCOPE,  
    SECTOR, 
    SESSION_KEY
FROM {{ ref('STG_API_RACE_CONTROL') }}

{% if is_incremental() %}
    {{ incremental_date_comparison() }}
{% endif %}