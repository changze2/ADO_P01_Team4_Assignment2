SELECT
    RACEID AS RACE_ID,
    DRIVERID AS DRIVER_ID,
    STOP,
    LAP,
    TIME,
    DURATION,
    MILLISECONDS
FROM {{ ref('STG_PIT_STOPS') }} 
