-- Removed 'url','fp1_date', 'fp1_time', 'fp2_date','fp2_time','fp3_time','fp3_date','quali_date','quali_time','sprint_date','sprint_time' due to high % of NULL 

SELECT
    RACEID as RACE_ID,
    YEAR,
    ROUND,
    CIRCUITID as CIRCUIT_ID,
    NAME,
    DATE,
    TIME 
FROM {{ ref('STG_RACES') }}




