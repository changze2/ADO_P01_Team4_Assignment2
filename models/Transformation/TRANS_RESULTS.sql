-- Replace values in POSITIONTEXT using the updated mapping
-- Convert TIME to seconds and rename column to TIME_INCREMENT
-- Convert FASTESTLAPTIME to seconds and rename column
-- Include STATUS column from the STATUS table
-- Join the STATUS table to get the status column


SELECT
    r.RESULTID AS RESULT_ID,
    r.RACEID AS RACE_ID,
    r.DRIVERID AS DRIVER_ID,
    r.CONSTRUCTORID AS CONSTRUCTOR_ID,
    r.NUMBER,
    r.GRID,
    CASE 
        WHEN r.POSITIONTEXT = 'R' THEN 'Retired'
        WHEN r.POSITIONTEXT = 'D' THEN 'Disqualified'
        WHEN r.POSITIONTEXT = 'N' THEN 'Not Classified'
        WHEN r.POSITIONTEXT = 'W' THEN 'Withdrawn'
        WHEN r.POSITIONTEXT = 'E' THEN 'Excluded'
        ELSE r.POSITIONTEXT
    END AS POSITIONTEXT,
    r.POSITIONORDER AS POSITION_ORDER,
    r.POINTS,
    r.LAPS,
    CASE 
        WHEN r.TIME IS NOT NULL AND r.TIME LIKE '%:%' THEN
            TRY_CAST(SPLIT_PART(r.TIME, ':', 1) AS FLOAT) * 60 +
            TRY_CAST(SPLIT_PART(r.TIME, ':', 2) AS FLOAT)
        ELSE NULL
    END AS TIME_INCREMENT,
    CASE 
        WHEN r.FASTESTLAPTIME IS NOT NULL AND r.FASTESTLAPTIME LIKE '%:%' THEN
            TRY_CAST(SPLIT_PART(r.FASTESTLAPTIME, ':', 1) AS FLOAT) * 60 +
            TRY_CAST(SPLIT_PART(r.FASTESTLAPTIME, ':', 2) AS FLOAT)
        ELSE NULL
    END AS FASTESTLAPTIME_SECONDS,
    r.MILLISECONDS,
    r.FASTESTLAP AS FASTEST_LAP,
    r.RANK,
    r.FASTESTLAPSPEED AS FASTEST_LAP_SPEED,
    s.STATUS AS STATUS
FROM {{ ref('STG_RESULTS') }} AS r
INNER JOIN {{ ref('STG_STATUS') }} AS s
    ON r.STATUSID = s.STATUSID
