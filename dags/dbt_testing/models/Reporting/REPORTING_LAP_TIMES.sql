-- Query Purpose:
-- This query combines lap time data with driver, race, and circuit details to provide 
-- a comprehensive dataset for analyzing driver performance on different tracks.

SELECT 
    -- Lap Times Table Fields 
    Lap_times.RACE_ID, 
    Lap_times.DRIVER_ID AS LAP_TIMES_DRIVER_ID, -- Aliased to avoid duplication
    Lap_times.LAP, 
    Lap_times.POSITION, 
    Lap_times.TIME AS LAP_TIMES_TIMING, 
    Lap_times.MILLISECONDS AS LAP_TIMES_MILLISECONDS,
    
    -- Drivers Table Fields 
    Driver.DRIVER_REF, 
    Driver.FULL_NAME AS DRIVER_FULL_NAME, 
    Driver.DOB AS DRIVER_DOB, 
    Driver.NATIONALITY AS DRIVER_NATIONALITY,
    
    -- Races Table Fields
    r.YEAR AS RACE_YEAR,
    r.ROUND AS RACE_ROUND,
    r.NAME AS RACE_NAME,
    r.DATE AS RACE_DATE,
    r.TIME AS RACE_TIME,
    
    -- Circuits Table Fields 
    Circuits.CIRCUIT_ID, 
    Circuits.CIRCUIT_REF, 
    Circuits.NAME AS CIRCUIT_NAME, 
    Circuits.LOCATION AS CIRCUIT_LOCATION, 
    Circuits.COUNTRY AS CIRCUIT_COUNTRY, 
    Circuits.LATITUDE AS CIRCUIT_LATITUDE, 
    Circuits.LONGITUDE AS CIRCUIT_LONGITUDE, 
    Circuits.ALTITUDE AS CIRCUIT_ALTITUDE, 
    Circuits.COUNTRY_NATIONALITY 
    
FROM 
    {{ ref('TRANS_LAPTIMES') }} Lap_times 
LEFT JOIN 
    {{ ref('TRANS_DRIVERS') }} Driver  
    ON Lap_times.DRIVER_ID = Driver.DRIVER_ID 
LEFT JOIN 
    {{ ref('TRANS_RACES') }} r  
    ON Lap_times.RACE_ID = r.RACE_ID 
LEFT JOIN 
    {{ ref('TRANS_CIRCUITS') }} Circuits  
    ON r.CIRCUIT_ID = Circuits.CIRCUIT_ID
