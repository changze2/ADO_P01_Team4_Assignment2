-- This query processes pit stop data from F1 races by ensuring proper type casting, handling null values, 
-- It ensures that time-based data is correctly formatted.


SELECT 
    CAST(NULLIF(DATE, '') AS TIMESTAMP_NTZ) AS DATE,  
    CAST(NULLIF(DRIVER_NUMBER, '') AS INT) AS DRIVER_NUMBER,  
    CAST(NULLIF(LAP_NUMBER, '') AS FLOAT) AS LAP_NUMBER,  
    CAST(NULLIF(MEETING_KEY, '') AS INT) AS MEETING_KEY,  
    CAST(NULLIF(PIT_DURATION, '') AS FLOAT) AS PIT_DURATION,  
    CAST(NULLIF(SESSION_KEY, '') AS INT) AS SESSION_KEY  
FROM {{ ref('STG_API_PIT') }}

{% if is_incremental() %}
    {{ incremental_date_comparison() }}
{% endif %}
