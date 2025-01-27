WITH NewResults AS (
    SELECT 
        r.*,
        races.NAME AS RACE_NAME,
        races.DATE AS RACE_DATE,
        c.NAME AS CIRCUIT_NAME,
        c.LOCATION AS CIRCUIT_LOCATION,
        con.CONSTRUCTOR_NAME,
        d.FULL_NAME AS DRIVER_NAME
    FROM 
        {{ ref('TRANS_RESULTS') }} r
    LEFT JOIN {{ ref('TRANS_RACES') }} races ON r.RACE_ID = races.RACE_ID
    LEFT JOIN {{ ref('TRANS_CIRCUITS') }} c ON races.CIRCUIT_ID = c.CIRCUIT_ID
    LEFT JOIN {{ ref('TRANS_CONSTRUCTORS') }} con ON r.CONSTRUCTOR_ID = con.CONSTRUCTOR_ID
    LEFT JOIN {{ ref('TRANS_DRIVERS') }} d ON r.DRIVER_ID = d.DRIVER_ID
),
NewPitStops AS (
    SELECT 
        ps.*,
        races.NAME AS RACE_NAME,
        races.DATE AS RACE_DATE,
        c.NAME AS CIRCUIT_NAME,
        c.LOCATION AS CIRCUIT_LOCATION,
        nr.DRIVER_NAME,
        nr.CONSTRUCTOR_NAME
    FROM 
        {{ ref('TRANS_PIT_STOPS') }} ps
    LEFT JOIN {{ ref('TRANS_RACES') }} races ON ps.RACE_ID = races.RACE_ID
    LEFT JOIN {{ ref('TRANS_CIRCUITS') }} c ON races.CIRCUIT_ID = c.CIRCUIT_ID
    LEFT JOIN (
        SELECT DISTINCT RACE_ID, DRIVER_ID, DRIVER_NAME, CONSTRUCTOR_ID, CONSTRUCTOR_NAME
        FROM NewResults
    ) nr ON ps.RACE_ID = nr.RACE_ID AND ps.DRIVER_ID = nr.DRIVER_ID
),
PitStopAggregates AS (
    SELECT 
        RACE_ID,
        RACE_NAME,
        CONSTRUCTOR_NAME,
        DRIVER_ID,
        DRIVER_NAME,
        SUM(MILLISECONDS) AS TOTAL_PIT_MILLISECONDS
    FROM 
        NewPitStops
    GROUP BY 
        RACE_ID, RACE_NAME, CONSTRUCTOR_NAME, DRIVER_ID, DRIVER_NAME
)
SELECT 
    nr.*,
    psa.TOTAL_PIT_MILLISECONDS,
    (psa.TOTAL_PIT_MILLISECONDS::FLOAT / nr.MILLISECONDS * 100) AS PIT_PERCENTAGE
FROM 
    NewResults nr
LEFT JOIN PitStopAggregates psa 
    ON nr.RACE_ID = psa.RACE_ID AND nr.DRIVER_ID = psa.DRIVER_ID
