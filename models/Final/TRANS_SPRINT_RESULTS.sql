-- Remove positions
-- Map short form values in positionText to full form 
-- Merge status to sprint_results
-- Convert fastest lap time to seconds 

SELECT
    sr.SPRINT_RESULT_ID,
    sr.RACE_ID,
    sr.DRIVER_ID,
    sr.CONSTRUCTOR_ID,
    sr.NUMBER,
    sr.GRID,
    -- Replace positionText values
    CASE
        WHEN sr.POSITIONTEXT = 'R' THEN 'Retired'
        WHEN sr.POSITIONTEXT = 'D' THEN 'Disqualified'
        WHEN sr.POSITIONTEXT = 'N' THEN 'Not Classified'
        WHEN sr.POSITIONTEXT = 'W' THEN 'Withdrawn'
        WHEN sr.POSITIONTEXT = 'E' THEN 'Excluded'
        ELSE sr.POSITIONTEXT
    END AS POSITIONTEXT,
    sr.POSITIONORDER,
    sr.POINTS,
    sr.LAPS,
    sr.TIME,
    sr.MILLISECONDS,
    sr.FASTESTLAP,
    sr.FASTESTLAPTIME,
    -- Convert MM:SS.SSS to seconds
    CASE
        WHEN sr.FASTESTLAPTIME IS NULL THEN NULL  -- Handle NULL values
        WHEN POSITION(':', sr.FASTESTLAPTIME) = 0 THEN NULL  -- Handle unexpected formats
        ELSE
            CAST(SPLIT_PART(sr.FASTESTLAPTIME, ':', 1) AS FLOAT) * 60 +  -- Extract minutes and convert to seconds
            CAST(SPLIT_PART(sr.FASTESTLAPTIME, ':', 2) AS FLOAT)         -- Extract seconds
    END AS FASTESTLAPTIME_SECONDS,

    sr.STATUS_ID,
    s.STATUS
FROM {{ ref('STG_SPRINT_RESULTS') }} sr
LEFT JOIN {{ ref('STG_STATUS') }} s
    ON sr.STATUS_ID = s.STATUS_ID;
