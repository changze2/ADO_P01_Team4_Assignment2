-- Merge the drivers and results table on DRIVERID
with drivers as (
    select 
        DRIVERID,
        DRIVERREF,
        FORENAME,
        SURNAME,
        DOB,
        NATIONALITY
    from {{ ref('STG_DRIVERS') }} 
),
results as (
    select
        RESULTID, 
        RACEID,
        DRIVERID,
        CONSTRUCTORID,
        CASE WHEN NUMBER = '\\N' THEN NULL ELSE NUMBER END AS NUMBER,
        GRID,
        CASE WHEN POSITION = '\\N' THEN NULL ELSE POSITION END AS POSITION,
        POSITIONTEXT,
        POSITIONORDER,
        POINTS,
        LAPS,
        CASE WHEN TIME = '\\N' THEN NULL ELSE TIME END AS TIME,
        CASE WHEN MILLISECONDS = '\\N' THEN NULL ELSE MILLISECONDS END AS MILLISECONDS,
        CASE WHEN FASTESTLAP = '\\N' THEN NULL ELSE FASTESTLAP END AS FASTESTLAP,
        CASE WHEN RANK = '\\N' THEN NULL ELSE RANK END AS RANK,
        CASE WHEN FASTESTLAPTIME = '\\N' THEN NULL ELSE FASTESTLAPTIME END AS FASTESTLAPTIME,
        CASE WHEN FASTESTLAPSPEED = '\\N' THEN NULL ELSE FASTESTLAPSPEED END AS FASTESTLAPSPEED,
        STATUSID
    from {{ ref('STG_RESULTS') }}
)

select
    d.DRIVERID,
    concat(d.FORENAME, ' ', d.SURNAME) as driver_name,
    r.POSITION
from drivers d
join results r on d.DRIVERID = r.DRIVERID
