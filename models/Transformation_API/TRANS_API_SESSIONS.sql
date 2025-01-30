-- Dropped circuit_key, circuit_short_name, country_code, country_key, country_name, location 
-- since they can be referenced by joining this table with meetings by meeting_key

SELECT 
    CAST(DATE_END AS TIMESTAMP_NTZ) AS DATE_END,  
    CAST(DATE_START AS TIMESTAMP_NTZ) AS DATE_START,  
    CAST(GMT_OFFSET AS STRING) AS GMT_OFFSET,           
    CAST(MEETING_KEY AS INT) AS MEETING_KEY,  
    CAST(SESSION_KEY AS INT) AS SESSION_KEY,  
    CAST(SESSION_NAME AS STRING) AS SESSION_NAME,  
    CAST(SESSION_TYPE AS STRING) AS SESSION_TYPE,  
    CAST(YEAR AS INT) AS YEAR  
FROM {{ ref('STG_API_SESSIONS') }}

{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}
