SELECT
    CONSTRUCTORSTANDINGSID,
    RACEID,
    CONSTRUCTORID,
    POINTS,
    POSITION,
    WINS
FROM {{ source('ASTON_MARTIN_DATA', 'CONSTRUCTOR_STANDINGS') }}