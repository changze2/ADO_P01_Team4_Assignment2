-- This query processes position tracking data from Formula 1 races by ensuring proper type casting,
-- handling time-based data correctly, and applying incremental processing for optimized updates.

SELECT 
    CAST(DATE AS TIMESTAMP_NTZ) AS DATE,  
    CAST(DRIVER_NUMBER AS INT) AS DRIVER_NUMBER,  
    CAST(MEETING_KEY AS INT) AS MEETING_KEY,  
    CAST(POSITION AS INT) AS POSITION,  
    CAST(SESSION_KEY AS INT) AS SESSION_KEY  
FROM {{ ref('STG_API_POSITION') }}

{% if is_incremental() %}
    {{ incremental_date_comparison() }}
{% endif %}
