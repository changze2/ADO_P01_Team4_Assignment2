-- Removed URL column
SELECT
    constructorId,
    constructorRef,
    name,
    nationality
FROM {{ ref('STG_CONSTRUCTORS') }}
