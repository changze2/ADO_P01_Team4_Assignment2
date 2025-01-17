-- Joining information from Pit_Stops, Drivers, Races and Circuits Tables 

SELECT
    -- Pit_stops Table Fields
    ps.RACEID,
    ps.DRIVERID AS PIT_STOPS_DRIVER_ID,
    ps.STOP AS PIT_STOPS_STOP,
    ps.LAP AS PIT_STOPS_LAP,
    ps.TIME AS PIT_STOP_TIME,
    ps.DURATION AS PIT_STOPS_DURATION,
    ps.MILLISECONDS AS PIT_STOPS_MILLISECONDS, 

    -- Drivers Table Fields 
    d.DRIVER_REF, 
    d.FULL_NAME AS DRIVER_NAME, 
    d.DOB AS DRIVER_DOB, 
    d.NATIONALITY AS DRIVER_NATIONALITY, 

    -- Races Table Fields 
    r.YEAR AS RACE_YEAR, 
    r.ROUND AS RACE_ROUND, 
    r.NAME AS RACE_NAME, 
    r.DATE AS RACE_DATE, 
    r.TIME AS RACE_TIME, 

    -- Circuits Table Fields 
    c.CIRCUIT_ID, 
    c.NAME AS CIRCUIT_NAME, 
    c.LOCATION AS CIRCUIT_LOCATION, 
    c.COUNTRY AS CIRCUIT_COUNTRY, 
    c.LATITUDE AS CIRCUIT_LATITUDE, 
    c.LONGITUDE AS CIRCUIT_LONGITUDE, 
    c.ALTITUDE AS CIRCUIT_ALTITUDE 

FROM 
    {{ ref('TRANS_PIT_STOPS') }} ps
-- Join with Drivers
LEFT JOIN 
    {{ ref('TRANS_DRIVERS') }} d 
    ON ps.DRIVERID = d.DRIVER_ID
-- Join with Races
LEFT JOIN 
    {{ ref('TRANS_RACES') }} r 
    ON ps.RACEID = r.RACE_ID
-- Join with Circuits
LEFT JOIN 
    {{ ref('TRANS_CIRCUITS') }} c 
    ON r.CIRCUIT_ID = c.CIRCUIT_ID