-- Query Purpose:
-- This query consolidates Formula 1 sprint race results by joining sprint race performance 
-- with race details, circuit metadata, driver information, and constructor details.

SELECT
    -- Sprint Results Table Fields
    sprint.SPRINT_RESULT_ID,
    sprint.RACE_ID,
    sprint.DRIVER_ID,
    sprint.CONSTRUCTOR_ID,
    sprint.NUMBER,
    sprint.GRID,
    sprint.POSITIONTEXT AS POSITION_TEXT,
    sprint.POSITIONORDER AS POSITION_ORDER,
    sprint.POINTS,
    sprint.LAPS,
    sprint.TIME_INCREMENT,
    sprint.MILLISECONDS,
    sprint.FASTESTLAP AS FASTEST_LAP_NUMBER,
    sprint.FASTESTLAPTIME_SECONDS AS FASTEST_LAP_TIME_SECONDS,
    sprint.STATUS_ID,
    sprint.STATUS,
    sprint.SPRINT_DATE,
    sprint.SPRINT_TIME,

    -- Races Table Fields
    races.YEAR AS RACE_YEAR,
    races.ROUND AS RACE_ROUND,
    races.NAME AS RACE_NAME,
    races.DATE AS RACE_DATE,
    races.TIME AS RACE_TIME,

    -- Circuits Table Fields
    c.CIRCUIT_ID,
    c.NAME AS CIRCUIT_NAME,
    c.LOCATION AS CIRCUIT_LOCATION,
    c.COUNTRY AS CIRCUIT_COUNTRY,
    c.LATITUDE AS CIRCUIT_LATITUDE,
    c.LONGITUDE AS CIRCUIT_LONGITUDE,
    c.ALTITUDE AS CIRCUIT_ALTITUDE,

    -- Drivers Table Fields
    d.DRIVER_REF,
    d.FULL_NAME AS DRIVER_NAME,
    d.DOB AS DRIVER_DOB,
    d.NATIONALITY AS DRIVER_NATIONALITY,

    -- Constructors Table Fields
    con.NAME AS CONSTRUCTOR_NAME,
    con.NATIONALITY AS CONSTRUCTOR_NATIONALITY

FROM {{ ref('TRANS_SPRINT_RESULTS') }} sprint
LEFT JOIN {{ ref('TRANS_RACES') }} races 
    ON sprint.RACE_ID = races.RACE_ID
LEFT JOIN {{ ref('TRANS_CIRCUITS') }} c 
    ON races.CIRCUIT_ID = c.CIRCUIT_ID
LEFT JOIN {{ ref('TRANS_DRIVERS') }} d 
    ON sprint.DRIVER_ID = d.DRIVER_ID
LEFT JOIN {{ ref('TRANS_CONSTRUCTORS') }} con 
    ON sprint.CONSTRUCTOR_ID = con.CONSTRUCTOR_ID
