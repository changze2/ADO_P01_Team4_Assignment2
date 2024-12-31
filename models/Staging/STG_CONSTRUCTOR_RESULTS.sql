SELECT
	CONSTRUCTORRESULTSID,
	RACEID,
	CONSTRUCTORID,
	POINTS,
    CASE WHEN STATUS = '\\N' THEN NULL ELSE STATUS END AS STATUS,

FROM {{ source('ASTON_MARTIN_DATA', 'CONSTRUCTOR_RESULTS') }}