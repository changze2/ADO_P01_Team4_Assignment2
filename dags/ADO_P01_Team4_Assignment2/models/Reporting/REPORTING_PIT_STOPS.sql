-- Query Purpose:
-- This query integrates race results, pit stop data, and aggregated pit stop performance metrics
-- to analyze how pit stops impact driver race performance.


WITH NewResults AS (
    SELECT 
        r.RESULT_ID,
        r.RACE_ID,
        r.DRIVER_ID,
        r.CONSTRUCTOR_ID,
        r.NUMBER,
        r.GRID,
        r.POSITIONTEXT,
        r.POSITION_ORDER,
        r.POINTS,
        r.LAPS AS RESULTS_LAPS,
        r.TIME_INCREMENT,
        r.FASTESTLAPTIME_SECONDS,
        r.MILLISECONDS AS RESULTS_MILLISECONDS,
        r.FASTEST_LAP,
        r.RANK,
        r.FASTEST_LAP_SPEED,
        r.STATUS,
        races.NAME AS RACE_NAME,
        races.DATE AS RACE_DATE,
        races.YEAR AS RACE_YEAR,
        races.ROUND AS RACE_ROUND,
        races.TIME AS RACE_TIME,
        c.NAME AS CIRCUIT_NAME,
        c.LOCATION AS CIRCUIT_LOCATION,
        c.COUNTRY AS CIRCUIT_COUNTRY,
        con.NAME AS CONSTRUCTOR_NAME,
        con.NATIONALITY AS CONSTRUCTOR_NATIONALITY,
        d.DOB AS DRIVER_DOB,
        d.NATIONALITY AS DRIVER_NATIONALITY,
        d.FULL_NAME AS DRIVER_NAME
    FROM 
        {{ ref('TRANS_RESULTS') }} r
    LEFT JOIN {{ ref('TRANS_RACES') }} races 
        ON r.RACE_ID = races.RACE_ID
    LEFT JOIN {{ ref('TRANS_CIRCUITS') }} c 
        ON races.CIRCUIT_ID = c.CIRCUIT_ID
    LEFT JOIN {{ ref('TRANS_CONSTRUCTORS') }} con 
        ON r.CONSTRUCTOR_ID = con.CONSTRUCTOR_ID
    LEFT JOIN {{ ref('TRANS_DRIVERS') }} d 
        ON r.DRIVER_ID = d.DRIVER_ID
    ),
    NewPitStops AS (
        SELECT 
            ps.RACE_ID,
            ps.DRIVER_ID,
            ps.LAP AS PIT_LAPS,
            ps.STOP AS PIT_STOPS,
            ps.TIME AS PIT_TIME,
            ps.DURATION AS PIT_DURATION,
            ps.MILLISECONDS AS PIT_MILLISECONDS,
            (ps.MILLISECONDS / 1000) AS PIT_SECONDS,
            nr.RACE_NAME,
            nr.RACE_DATE,
            nr.CIRCUIT_NAME,
            nr.CIRCUIT_LOCATION,
            nr.DRIVER_NAME,
            nr.CONSTRUCTOR_NAME
        FROM 
            {{ ref('TRANS_PIT_STOPS') }} ps
        LEFT JOIN NewResults nr 
            ON ps.RACE_ID = nr.RACE_ID AND ps.DRIVER_ID = nr.DRIVER_ID
    ),
    PitStopAggregates AS (
        SELECT 
            RACE_ID,
            RACE_NAME,
            CONSTRUCTOR_NAME,
            DRIVER_ID,
            DRIVER_NAME,
            SUM(PIT_MILLISECONDS) AS TOTAL_PIT_MILLISECONDS
        FROM 
            NewPitStops
        GROUP BY 
            RACE_ID, RACE_NAME, CONSTRUCTOR_NAME, DRIVER_ID, DRIVER_NAME
    )
SELECT 
    nr.*,
    nps.PIT_LAPS,
    nps.PIT_STOPS,
    nps.PIT_TIME,
    nps.PIT_DURATION,
    nps.PIT_MILLISECONDS,
    nps.PIT_SECONDS,
    psa.TOTAL_PIT_MILLISECONDS,
    (psa.TOTAL_PIT_MILLISECONDS::FLOAT / nr.RESULTS_MILLISECONDS * 100) AS PIT_PERCENTAGE
FROM 
    NewResults nr
LEFT JOIN PitStopAggregates psa 
    ON nr.RACE_ID = psa.RACE_ID AND nr.DRIVER_ID = psa.DRIVER_ID
LEFT JOIN NewPitStops nps
    ON nr.RACE_ID = nps.RACE_ID AND  nr.DRIVER_ID = nps.DRIVER_ID