-- Variable 'POSITION' was dropped 
-- NULL values are handled by checking for '\\N' and replacing them with NULL.

SELECT
    RESULTID,
	RACEID,
	DRIVERID,
	CONSTRUCTORID,
    NUMBER,
	GRID,
	CASE WHEN POSITION = '\\N' THEN NULL ELSE POSITION END AS POSITION, -- convert the special characters to string
	CASE WHEN POSITIONTEXT = '\\N' THEN NULL ELSE POSITIONTEXT END AS POSITIONTEXT, 
	POSITIONORDER,
	POINTS,
	LAPS,
	CASE WHEN TIME = '\\N' THEN NULL ELSE TIME END AS TIME,
	CASE WHEN MILLISECONDS = '\\N' THEN NULL ELSE MILLISECONDS END AS MILLISECONDS,
    CASE WHEN FASTESTLAP = '\\N' THEN NULL ELSE FASTESTLAP END AS FASTESTLAP,
	CASE WHEN FASTESTLAPTIME = '\\N' THEN NULL ELSE FASTESTLAPTIME END AS FASTESTLAPTIME,
	STATUSID,

FROM {{ source('ASTON_MARTIN_DATA', 'SPRINT_RESULTS') }}