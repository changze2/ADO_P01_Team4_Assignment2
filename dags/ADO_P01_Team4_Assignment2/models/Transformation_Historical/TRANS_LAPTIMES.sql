-- no transformation needed

SELECT
    RACEID as RACE_ID,
    DRIVERID AS DRIVER_ID,
    LAP,
    POSITION,
    TIME,
    MILLISECONDS
FROM {{ ref('STG_LAPTIMES') }}