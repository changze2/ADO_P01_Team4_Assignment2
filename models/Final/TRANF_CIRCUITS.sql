-- Excluded url column in circuits table 
-- Create a new column called country_nationality to find out the natioality
SELECT
    circuitId,
    circuitRef,
    name,
    location,
    country,
    lat AS Latitude,
    lng AS Longitude,
    alt AS Altitude,
    CASE
        WHEN country IN ('Australia') THEN 'Australian'
        WHEN country IN ('Malaysia') THEN 'Malaysian'
        WHEN country IN ('Bahrain') THEN 'Bahraini'
        WHEN country IN ('Spain') THEN 'Spanish'
        WHEN country IN ('Turkey') THEN 'Turkish'
        WHEN country IN ('Monaco') THEN 'Monegasque'
        WHEN country IN ('Canada') THEN 'Canadian'
        WHEN country IN ('France') THEN 'French'
        WHEN country IN ('UK') THEN 'British'
        WHEN country IN ('Germany') THEN 'German'
        WHEN country IN ('Hungary') THEN 'Hungarian'
        WHEN country IN ('Belgium') THEN 'Belgian'
        WHEN country IN ('Italy') THEN 'Italian'
        WHEN country IN ('Singapore') THEN 'Singaporean'
        WHEN country IN ('Japan') THEN 'Japanese'
        WHEN country IN ('China') THEN 'Chinese'
        WHEN country IN ('Brazil') THEN 'Brazilian'
        WHEN country IN ('USA', 'United States') THEN 'American'
        WHEN country IN ('UAE') THEN 'Emirati'
        WHEN country IN ('Korea') THEN 'South Korean'
        WHEN country IN ('India') THEN 'Indian'
        WHEN country IN ('Austria') THEN 'Austrian'
        WHEN country IN ('Russia') THEN 'Russian'
        WHEN country IN ('Mexico') THEN 'Mexican'
        WHEN country IN ('Azerbaijan') THEN 'Azerbaijani'
        WHEN country IN ('Argentina') THEN 'Argentine'
        WHEN country IN ('Portugal') THEN 'Portuguese'
        WHEN country IN ('South Africa') THEN 'South African'
        WHEN country IN ('Netherlands') THEN 'Dutch'
        WHEN country IN ('Sweden') THEN 'Swedish'
        WHEN country IN ('Qatar') THEN 'Qatari'
        WHEN country IN ('Saudi Arabia') THEN 'Saudi'
        WHEN country IN ('Switzerland') THEN 'Swiss'
        WHEN country IN ('Morocco') THEN 'Moroccan'
        WHEN country IN ('Vietnam') THEN 'Vietnamese'
        ELSE 'Unknown'
    END AS country_nationality
FROM {{ ref('STG_CIRCUITS') }}
