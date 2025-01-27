SELECT
   RACEID AS RACE_ID,
	DRIVERID AS DRIVER_ID,
	STOP NUMBER(38,0),
	LAP NUMBER(38,0),
	TIME VARCHAR(255),
	DURATION VARCHAR(255),
	MILLISECONDS NUMBER(38,0)
FROM {{ ref('STG_PIT_STOPS') }} 
