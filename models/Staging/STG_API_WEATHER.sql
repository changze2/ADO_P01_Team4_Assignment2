SELECT *
FROM {{ source('ASTON_MARTIN_DATA', 'WEATHER_API') }}
