-- Query Purpose:
-- This query consolidates driver data with multiple race-related tables,
-- including pit stops, positions, stints, race control messages, sessions, and meetings.


SELECT 
    -- Driver Information
    drivers.DRIVER_NUMBER,
    drivers.FULL_NAME,
    drivers.TEAM_NAME,
    drivers.NAME_ACRONYM,

    -- Meeting Information
    meetings.MEETING_KEY,
    meetings.MEETING_NAME,
    meetings.YEAR,
    meetings.COUNTRY_NAME,
    meetings.LOCATION,
    meetings.DATE_START AS MEETING_DATE_START,

    -- Position Information
    positions.POSITION,
    positions.DATE AS POSITION_DATE,

    -- Pit Stop Information
    pit_stops.PIT_DURATION,
    pit_stops.LAP_NUMBER AS PIT_STOP_LAP,

    -- Stint Information
    stints.COMPOUND AS STINT_COMPOUND,
    stints.LAP_START AS STINT_LAPSTART,
    stints.LAP_END AS STINT_LAPEND,
    stints.STINT_NUMBER AS STINT_NUMBER,

    -- Race Control Information
    race_control.CATEGORY AS RACE_CATEGORY,
    race_control.FLAG AS RACE_FLAG,
    race_control.MESSAGE AS RACE_MESSAGE,

    -- Session Information
    sessions.DATE_END AS SESSION_DATE_END,
    sessions.DATE_START AS SESSION_DATE_START,
    sessions.GMT_OFFSET,
    sessions.SESSION_KEY,
    sessions.SESSION_NAME,
    sessions.SESSION_TYPE,

FROM {{ ref('TRANS_API_DRIVERS') }} AS drivers

-- Join Pit Stop Information
INNER JOIN {{ ref('TRANS_API_PIT') }} AS pit_stops
    ON drivers.DRIVER_NUMBER = pit_stops.DRIVER_NUMBER 
    AND drivers.MEETING_KEY = pit_stops.MEETING_KEY
    AND drivers.SESSION_KEY = pit_stops.SESSION_KEY
    AND drivers.DRIVER_NUMBER IS NOT NULL

-- Join Position Information
INNER JOIN {{ ref('TRANS_API_POSITION') }} AS positions
    ON drivers.DRIVER_NUMBER = positions.DRIVER_NUMBER 
    AND drivers.MEETING_KEY = positions.MEETING_KEY
    AND drivers.SESSION_KEY = positions.SESSION_KEY

-- Join Stint Information
LEFT JOIN {{ ref('TRANS_API_STINTS') }} AS stints
    ON drivers.DRIVER_NUMBER = stints.DRIVER_NUMBER 
    AND drivers.MEETING_KEY = stints.MEETING_KEY
    AND drivers.SESSION_KEY = stints.SESSION_KEY

-- Join Race Control Information
LEFT JOIN {{ ref('TRANS_API_RACE_CONTROL') }} AS race_control
    ON drivers.DRIVER_NUMBER = race_control.DRIVER_NUMBER 
    AND drivers.MEETING_KEY = race_control.MEETING_KEY
    AND drivers.SESSION_KEY = race_control.SESSION_KEY

-- Join Session Information
LEFT JOIN {{ ref('TRANS_API_SESSIONS') }} AS sessions
    ON drivers.MEETING_KEY = sessions.MEETING_KEY
    AND drivers.SESSION_KEY = sessions.SESSION_KEY
    
-- Join Meeting Information
LEFT JOIN {{ ref('TRANS_API_MEETINGS') }} AS meetings
    ON drivers.MEETING_KEY = meetings.MEETING_KEY

-- Incremental Processing: Only run on new data if incremental mode is enabled
{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}