-- Query Purpose:
-- This query combines constructor-related data from multiple tables, 
-- including constructor standings, results, constructor metadata, and race details.


SELECT
    -- Constructors Table Fields
    c.CONSTRUCTOR_ID,
    c.CONSTRUCTOR_REF,
    c.NAME AS CONSTRUCTOR_NAME,
    c.NATIONALITY AS CONSTRUCTOR_NATIONALITY,

    -- Constructor Results Table Fields
    cr.CONSTRUCTOR_RESULTS_ID,
    cr.RACE_ID AS CONSTRUCTOR_RESULTS_RACE_ID,
    cr.POINTS AS CONSTRUCTOR_RESULTS_POINTS,
    cr.STATUS AS CONSTRUCTOR_RESULTS_STATUS,

    -- Constructor Standings Table Fields
    cs.CONSTRUCTOR_STANDINGS_ID,
    cs.RACE_ID AS CONSTRUCTOR_STANDINGS_RACE_ID,
    cs.POINTS AS CONSTRUCTOR_STANDINGS_POINTS,
    cs.POSITION AS CONSTRUCTOR_STANDINGS_POSITION,
    cs.WINS AS CONSTRUCTOR_STANDINGS_WINS,

    -- Races Table Fields
    r.YEAR AS RACE_YEAR,
    r.ROUND AS RACE_ROUND,
    r.NAME AS RACE_NAME,
    r.DATE AS RACE_DATE,
    r.TIME AS RACE_TIME

FROM 
    {{ ref('TRANS_CONSTRUCTOR_STANDINGS') }} cs
LEFT JOIN 
    {{ ref('TRANS_CONSTRUCTORS') }} c
    ON c.CONSTRUCTOR_ID = cs.CONSTRUCTOR_ID
LEFT JOIN 
    {{ ref('TRANS_CONSTRUCTOR_RESULTS') }} cr
    ON cr.CONSTRUCTOR_ID = cs.CONSTRUCTOR_ID
    AND cr.RACE_ID = cs.RACE_ID -- Composite Key Join
LEFT JOIN 
    {{ ref('TRANS_RACES') }} r
    ON cs.RACE_ID = r.RACE_ID
