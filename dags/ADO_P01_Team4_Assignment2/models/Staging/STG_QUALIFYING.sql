SELECT
    QUALIFYID,
    RACEID ,
    DRIVERID,
    CONSTRUCTORID,
    NUMBER,
    POSITION,
    CASE WHEN Q1 = '\\N' THEN NULL ELSE Q1 END AS Q1, -- convert the special characters to string
    CASE WHEN Q2 = '\\N' THEN NULL ELSE Q2 END AS Q2,
    CASE WHEN Q3 = '\\N' THEN NULL ELSE Q3 END AS Q3,
FROM {{ source('ASTON_MARTIN_DATA', 'QUALIFYING') }}
