SELECT
    "circuitId" AS circuit_id,
    "circuitRef" AS circuit_ref,
    "name" AS circuit_name,
    "location" AS circuit_location,
    "country" AS circuit_country,
    "lat" AS latitude,
    "lng" AS longitude,
    "alt" AS altitude,
    "url"
FROM
    {{ source('aston_martin_data', 'CIRCUITS2') }}
