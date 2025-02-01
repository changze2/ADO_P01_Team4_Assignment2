-- This query processes meeting-related data (race event metadata) by ensuring proper type casting 
-- and data integrity. 
SELECT 
    CAST(CIRCUIT_KEY AS INT) AS CIRCUIT_KEY, 
    CAST(CIRCUIT_SHORT_NAME AS STRING) AS CIRCUIT_SHORT_NAME, 
    CAST(COUNTRY_CODE AS STRING) AS COUNTRY_CODE, 
    CAST(COUNTRY_NAME AS STRING) AS COUNTRY_NAME, 
    CAST(DATE_START AS TIMESTAMP_NTZ) AS DATE_START,  -- Ensure correct timestamp handling
    CAST(GMT_OFFSET AS STRING) AS GMT_OFFSET,  --  Ensure it's a STRING
    CAST(LOCATION AS STRING) AS LOCATION,  --  Ensure it's a STRING
    CAST(MEETING_KEY AS INT) AS MEETING_KEY,  
    CAST(MEETING_NAME AS STRING) AS MEETING_NAME,  
    CAST(MEETING_OFFICIAL_NAME AS STRING) AS MEETING_OFFICIAL_NAME,  
    CAST(YEAR AS INT) AS YEAR  
FROM {{ ref('STG_API_MEETINGS') }}

{% if is_incremental() %}
    {{ incremental_meeting_key() }}

{% endif %}
