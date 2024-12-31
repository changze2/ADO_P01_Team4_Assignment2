-- replaced 'D' value in status with 'Disqualified'

SELECT
    CONSTRUCTORRESULTSID,
    RACEID,
    CONSTRUCTORID,
    POINTS,
    CASE
        WHEN STATUS = 'D' THEN 'Disqualified'
        ELSE STATUS
    END AS STATUS
FROM {{ ref('STG_CONSTRUCTOR_RESULTS') }}
