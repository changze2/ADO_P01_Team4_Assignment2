-- Excluded url column in circuits table 
-- Create a new column called country_nationality to find out the natioality
SELECT
    CIRCUITID as CIRCUIT_ID,
    CIRCUITREF as CIRCUIT_REF,
    NAME,
    LOCATION,
    COUNTRY,
    LAT AS LATITUDE,
    LNG AS LONGITUDE,
    ALT AS ALTITUDE,
    CASE
        WHEN COUNTRY IN ('Australia') THEN 'Australian'
        WHEN COUNTRY IN ('Malaysia') THEN 'Malaysian'
        WHEN COUNTRY IN ('Bahrain') THEN 'Bahraini'
        WHEN COUNTRY IN ('Spain') THEN 'Spanish'
        WHEN COUNTRY IN ('Turkey') THEN 'Turkish'
        WHEN COUNTRY IN ('Monaco') THEN 'Monegasque'
        WHEN COUNTRY IN ('Canada') THEN 'Canadian'
        WHEN COUNTRY IN ('France') THEN 'French'
        WHEN COUNTRY IN ('UK') THEN 'British'
        WHEN COUNTRY IN ('Germany') THEN 'German'
        WHEN COUNTRY IN ('Hungary') THEN 'Hungarian'
        WHEN COUNTRY IN ('Belgium') THEN 'Belgian'
        WHEN COUNTRY IN ('Italy') THEN 'Italian'
        WHEN COUNTRY IN ('Singapore') THEN 'Singaporean'
        WHEN COUNTRY IN ('Japan') THEN 'Japanese'
        WHEN COUNTRY IN ('China') THEN 'Chinese'
        WHEN COUNTRY IN ('Brazil') THEN 'Brazilian'
        WHEN COUNTRY IN ('USA', 'United States') THEN 'American'
        WHEN COUNTRY IN ('UAE') THEN 'Emirati'
        WHEN COUNTRY IN ('Korea') THEN 'South Korean'
        WHEN COUNTRY IN ('India') THEN 'Indian'
        WHEN COUNTRY IN ('Austria') THEN 'Austrian'
        WHEN COUNTRY IN ('Russia') THEN 'Russian'
        WHEN COUNTRY IN ('Mexico') THEN 'Mexican'
        WHEN COUNTRY IN ('Azerbaijan') THEN 'Azerbaijani'
        WHEN COUNTRY IN ('Argentina') THEN 'Argentine'
        WHEN COUNTRY IN ('Portugal') THEN 'Portuguese'
        WHEN COUNTRY IN ('South Africa') THEN 'South African'
        WHEN COUNTRY IN ('Netherlands') THEN 'Dutch'
        WHEN COUNTRY IN ('Sweden') THEN 'Swedish'
        WHEN COUNTRY IN ('Qatar') THEN 'Qatari'
        WHEN COUNTRY IN ('Saudi Arabia') THEN 'Saudi'
        WHEN COUNTRY IN ('Switzerland') THEN 'Swiss'
        WHEN COUNTRY IN ('Morocco') THEN 'Moroccan'
        WHEN COUNTRY IN ('Vietnam') THEN 'Vietnamese'
        ELSE 'Unknown'
    END AS COUNTRY_NATIONALITY
FROM {{ ref('STG_CIRCUITS') }}
