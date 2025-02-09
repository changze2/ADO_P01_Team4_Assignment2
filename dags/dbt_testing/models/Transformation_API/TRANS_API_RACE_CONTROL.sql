-- This query processes race control messages from F1 races, ensuring proper data transformations.
-- It handles missing values with default replacements and applies incremental processing for efficiency.


SELECT 
    UPPER(CAST(CATEGORY AS STRING)) AS CATEGORY,  
    CAST(DATE AS STRING) AS DATE,  
    CAST(DRIVER_NUMBER AS FLOAT) AS DRIVER_NUMBER,  
    COALESCE(CAST(FLAG AS STRING), 'NO_FLAG') AS FLAG,  
    COALESCE(CAST(LAP_NUMBER AS FLOAT), 0) AS LAP_NUMBER,  
    CAST(MEETING_KEY AS INT) AS MEETING_KEY,  
    CAST(MESSAGE AS STRING) AS MESSAGE,  
    UPPER(COALESCE(CAST(SCOPE AS STRING), 'NO_SCOPE')) AS SCOPE,  
    COALESCE(CAST(SECTOR AS FLOAT), 0) AS SECTOR,  
    CAST(SESSION_KEY AS INT) AS SESSION_KEY  
FROM {{ ref('STG_API_RACE_CONTROL') }}

{% if is_incremental() %}
    {{ incremental_date_comparison() }}
{% endif %}
