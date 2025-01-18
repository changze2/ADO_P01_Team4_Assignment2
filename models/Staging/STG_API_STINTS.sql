SELECT *
FROM {{ source(target.schema, 'STINTS_API') }}