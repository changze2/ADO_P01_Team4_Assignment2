-- •	Attributes ‘forename’ and ‘surname’ was joined together to form the ‘full_name’ attribute
-- •	‘URL’ attribute was removed 
-- •	‘code’ and ‘number’ attribute are also removed as they contain a lot of NULL values 
-- o	‘number’ stands for Permanent driver number that divers have. Since it serves as identification purpose, it is not very important since the drivers can already be identified with their ‘driverid’ and ‘names’
-- o	Based on the data, ‘code’ is derived from the first 3 letters of ‘driverRef’, although NULL values can be referenced from the ‘driverRef’ column, this attribute is not very important since it also serves as an identifier for the drivers. 


SELECT
    DRIVERID as DRIVER_ID,
    DRIVERREF as DRIVER_REF,
    INITCAP(CONCAT(FORENAME, ' ', SURNAME)) AS FULL_NAME,
    DOB,
    NATIONALITY
FROM {{ ref('STG_DRIVERS') }}