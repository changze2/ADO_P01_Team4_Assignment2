-- replaced 'D' value in status with 'Disqualified'

SELECT
    CONSTRUCTORRESULTSID as CONSTRUCTOR_RESULTS_ID,
    RACEID as RACE_ID,
    CONSTRUCTORID as CONSTRUCTOR_ID,
    POINTS,
    CASE
        WHEN STATUS = 'D' THEN 'Disqualified'
        ELSE STATUS
    END AS STATUS
FROM {{ ref('STG_CONSTRUCTOR_RESULTS') }}
