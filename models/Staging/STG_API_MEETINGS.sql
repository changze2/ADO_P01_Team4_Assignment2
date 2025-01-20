SELECT
    CIRCUIT_KEY,
    CIRCUIT_SHORT_NAME,
    COUNTRY_CODE,
    COUNTRY_KEY,  
    COUNTRY_NAME,
    DATE_START,
    GMT_OFFSET,
    LOCATION,
    MEETING_KEY,
    MEETING_NAME,
    MEETING_OFFICIAL_NAME,
    YEAR
FROM {{ source('ASTON_MARTIN_DATA', 'MEETINGS_API') }}