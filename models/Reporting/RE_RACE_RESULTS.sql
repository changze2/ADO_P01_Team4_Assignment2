SELECT
    -- Results Table Fields
    results.RESULT_ID,
    results.RACE_ID,
    results.DRIVER_ID,
    results.CONSTRUCTOR_ID,
    results.NUMBER,
    results.GRID,
    results.POSITIONTEXT AS POSITION_TEXT,
    results.POSITION_ORDER,
    results.POINTS,
    results.LAPS,
    results.TIME_INCREMENT,
    results.MILLISECONDS,
    results.FASTEST_LAP AS FASTEST_LAP_NUMBER,
    results.RANK AS FASTEST_LAP_RANK,
    results.FASTESTLAPTIME_SECONDS AS FASTEST_LAP_TIME_SECONDS,
    results.FASTEST_LAP_SPEED,

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
    con.NAME AS CONSTRUCTOR_N,
    con.NATIONALITY AS CONSTRUCTOR_NATIONALITY

FROM {{ ref('TRANS_RESULTS') }} results
LEFT JOIN {{ ref('TRANS_RACES') }} races 
    ON results.RACE_ID = races.RACE_ID
LEFT JOIN {{ ref('TRANS_CIRCUITS') }} c 
    ON races.CIRCUIT_ID = c.CIRCUIT_ID
LEFT JOIN {{ ref('TRANS_DRIVERS') }} d 
    ON results.DRIVER_ID = d.DRIVER_ID
LEFT JOIN {{ ref('TRANS_CONSTRUCTORS') }} con 
    ON results.CONSTRUCTOR_ID = con.CONSTRUCTOR_ID
