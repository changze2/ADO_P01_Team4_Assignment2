

SELECT
    AIR_TEMPERATURE,
    DATE,
    HUMIDITY,
    MEETING_KEY,
    SESSION_KEY,
    PRESSURE,
    RAINFALL,
    TRACK_TEMPERATURE,
    WIND_DIRECTION,
    WIND_SPEED,
-- Handling missing values: Fill missing AIR_TEMPERATURE with the average value
    COALESCE(AIR_TEMPERATURE, (
        SELECT AVG(AIR_TEMPERATURE) 
        FROM {{ ref('STG_API_WEATHER') }}
    )) AS AIR_TEMPERATURE_FILLED,
    
    -- Create derived features, e.g., Heat Index or Wind Chill
    CASE 
        WHEN AIR_TEMPERATURE IS NOT NULL AND HUMIDITY IS NOT NULL THEN
            AIR_TEMPERATURE + (0.55 * (1 - HUMIDITY / 100) * (AIR_TEMPERATURE - 58))
        ELSE NULL
    END AS HEAT_INDEX,
    
    CASE
        WHEN AIR_TEMPERATURE IS NOT NULL AND WIND_SPEED IS NOT NULL THEN
            AIR_TEMPERATURE - (0.7 * WIND_SPEED) -- Wind Chill simplified formula
        ELSE NULL
    END AS WIND_CHILL
    
FROM {{ ref('STG_API_WEATHER') }}

{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}
