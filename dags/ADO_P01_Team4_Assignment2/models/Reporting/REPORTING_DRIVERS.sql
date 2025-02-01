-- Query Purpose:
-- This query combines driver standings data with driver details, linking performance metrics 
-- (e.g., points, position, wins) with personal driver information (e.g., full name, nationality, date of birth).


SELECT
    -- Driver Standings Table Fields
    ds.DRIVER_STANDINGS_ID,
    ds.RACE_ID,
    ds.DRIVER_ID,
    ds.POINTS,
    ds.POSITION,
    ds.WINS,

    -- Drivers Table Fields
    d.DRIVER_REF, 
    d.FULL_NAME AS DRIVER_FULL_NAME, 
    d.DOB AS DRIVER_DOB, 
    d.NATIONALITY AS DRIVER_NATIONALITY

FROM 
    {{ ref('TRANS_DRIVER_STANDINGS') }} ds
LEFT JOIN 
    {{ ref('TRANS_DRIVERS') }} d
    ON ds.DRIVER_ID = d.DRIVER_ID
