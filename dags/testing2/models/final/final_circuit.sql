{{ config (materialized='table')}}

-- Final circuit analysis query using dbt references with INNER JOIN
SELECT
    r.RACEID,
    r.YEAR,
    r.ROUND,
    r.CIRCUIT_ID,
    r.NAME AS RACE_NAME,
    r.DATE AS RACE_DATE,
    r.TIME AS RACE_TIME,
    r.URL AS RACE_URL,
    r.FP1_DATE,
    r.FP1_TIME,
    r.FP2_DATE,
    r.FP2_TIME,
    r.FP3_DATE,
    r.FP3_TIME,
    r.QUALI_DATE,
    r.QUALI_TIME,
    r.SPRINT_DATE,
    r.SPRINT_TIME,
    
    -- Circuit Information from STG_CIRCUITS
    c.CIRCUIT_REF,
    c.CIRCUIT_NAME,
    c.CIRCUIT_LOCATION,
    c.CIRCUIT_COUNTRY,
    c.LATITUDE,
    c.LONGITUDE,
    c.ALTITUDE,
    c."url" AS CIRCUIT_URL,
    
    -- Results Information from STG_RESULTS
    res.RESULTID,
    res.DRIVERID,
    res.CONSTRUCTORID,
    res.NUMBER,
    res.GRID,
    res.POSITION,
    res.POSITIONTEXT,
    res.POSITIONORDER,
    res.POINTS,
    res.LAPS,
    res.TIME AS RESULT_TIME,
    res.MILLISECONDS,
    res.FASTESTLAP,
    res.RANK,
    res.FASTESTLAPTIME,
    res.FASTESTLAPSPEED,
    res.STATUSID
    
FROM
    {{ ref('transf_races') }} r

-- Inner Join STG_CIRCUITS on CIRCUIT_ID
INNER JOIN
    {{ ref('stg_circuits') }} c
    ON r.CIRCUIT_ID = c.CIRCUIT_ID

-- Inner Join STG_RESULTS on RACEID
INNER JOIN
    {{ ref('stg_results') }} res
    ON r.RACEID = res.RACEID