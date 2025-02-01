-- no transformation needed

SELECT
    STATUSID AS STATUS_ID,  
    STATUS
FROM {{ ref('STG_STATUS') }} 