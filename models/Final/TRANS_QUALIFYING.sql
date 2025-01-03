-- Create 'qualified_q2' column: 1 if Q2 is not '\N' and not NULL, else 0
-- Create 'qualified_q3' column: 1 if Q3 is not '\N' and not NULL, else 0


SELECT
    QUALIFYID as QUALIFY_ID,
    RACEID as RACE_ID,
    DRIVERID as DRIVER_ID,
    CONSTRUCTORID as CONSTRUCTOR_ID,
    NUMBER,
    POSITION,
    Q1,
    Q2,
    Q3,
    CASE 
        WHEN Q1 != '\\N' AND Q1 IS NOT NULL THEN 1
        ELSE 0
    END AS QUALIFIED_Q1,
    CASE 
        WHEN Q2 != '\\N' AND Q2 IS NOT NULL THEN 1
        ELSE 0
    END AS QUALIFIED_Q2,
    CASE 
        WHEN Q3 != '\\N' AND Q3 IS NOT NULL THEN 1
        ELSE 0
    END AS QUALIFIED_Q3
FROM {{ ref('STG_QUALIFYING') }}
