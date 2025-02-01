-- --Elvis: Decided to drop first_name / last_name / headshot_url since full_name is already created and headshot_url cannot provide valueable insights

-- This query processes driver-related data by cleaning and converting fields to appropriate data types.
-- It ensures that empty strings are converted to NULL values before applying proper type casting.


SELECT 
    CAST(NULLIF(DRIVER_NUMBER, '') AS INT) AS DRIVER_NUMBER, 
    CAST(NULLIF(BROADCAST_NAME, '') AS STRING) AS BROADCAST_NAME, 
    CAST(NULLIF(FULL_NAME, '') AS STRING) AS FULL_NAME, 
    CAST(NULLIF(NAME_ACRONYM, '') AS STRING) AS NAME_ACRONYM, 
    CAST(NULLIF(TEAM_NAME, '') AS STRING) AS TEAM_NAME, 
    CAST(NULLIF(TEAM_COLOUR, '') AS STRING) AS TEAM_COLOUR, 
    CAST(NULLIF(COUNTRY_CODE, '') AS STRING) AS COUNTRY_CODE, 
    CAST(NULLIF(SESSION_KEY, '') AS INT) AS SESSION_KEY, 
    CAST(NULLIF(MEETING_KEY, '') AS INT) AS MEETING_KEY 
FROM {{ ref('STG_API_DRIVERS') }}

-- incremental processing to handle only new data if applicable.
{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}
