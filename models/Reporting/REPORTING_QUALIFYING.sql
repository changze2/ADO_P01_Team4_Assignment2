-- Query Purpose:
-- This query combines qualifying data with races, drivers, constructors, and circuits 
-- to provide a comprehensive dataset for analyzing driver qualifying performances.

SELECT
    -- Qualifying Table Fields
    q.QUALIFY_ID,
    q.RACE_ID,
    q.DRIVER_ID,
    q.CONSTRUCTOR_ID,
    q.NUMBER AS DRIVER_NUMBER,
    q.POSITION AS QUALIFYING_POSITION,
    q.Q1 AS QUALIFYING_TIME_Q1,
    q.Q2 AS QUALIFYING_TIME_Q2,
    q.Q3 AS QUALIFYING_TIME_Q3,
    q.QUALIFIED_Q1,
    q.QUALIFIED_Q2,
    q.QUALIFIED_Q3,

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

FROM {{ ref('TRANS_QUALIFYING') }} q
LEFT JOIN {{ ref('TRANS_RACES') }} races ON q.RACE_ID = races.RACE_ID
LEFT JOIN {{ ref('TRANS_DRIVERS') }} d ON q.DRIVER_ID = d.DRIVER_ID
LEFT JOIN {{ ref('TRANS_CONSTRUCTORS') }} con ON q.CONSTRUCTOR_ID = con.CONSTRUCTOR_ID
LEFT JOIN {{ ref('TRANS_CIRCUITS') }} c ON races.CIRCUIT_ID = c.CIRCUIT_ID
