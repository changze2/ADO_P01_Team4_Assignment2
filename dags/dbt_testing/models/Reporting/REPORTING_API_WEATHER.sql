-- Query Purpose:
-- This query combines weather data with meeting and session details to provide 
-- a comprehensive dataset linking weather conditions to specific Formula 1 events.


SELECT 
    -- Weather Information
    weather.AIR_TEMPERATURE_FILLED,
    weather.HUMIDITY,
    weather.WIND_SPEED,
    weather.WIND_DIRECTION,
    weather.RAINFALL,
    weather.HEAT_INDEX,
    weather.WIND_CHILL,

    -- Meeting Information
    meetings.MEETING_KEY,
    sessions.SESSION_KEY,
    meetings.MEETING_NAME,
    meetings.YEAR,
    meetings.COUNTRY_NAME,
    meetings.CIRCUIT_SHORT_NAME,
    meetings.LOCATION,
    meetings.DATE_START AS MEETING_DATE_START,

    -- Session Information
    sessions.DATE_END AS SESSION_DATE_END,
    sessions.DATE_START AS SESSION_DATE_START,
    sessions.SESSION_NAME,
    sessions.SESSION_TYPE


FROM {{ ref('TRANS_API_WEATHER') }} AS weather
-- Join Meeting Information
LEFT JOIN {{ ref('TRANS_API_MEETINGS') }} AS meetings
    ON weather.MEETING_KEY = meetings.MEETING_KEY

-- Join Session Information
LEFT JOIN {{ ref('TRANS_API_SESSIONS') }} AS sessions
    ON weather.SESSION_KEY = sessions.SESSION_KEY

    
{% if is_incremental() %}
    {{ incremental_date_comparison(DATE) }}
{% endif %}
