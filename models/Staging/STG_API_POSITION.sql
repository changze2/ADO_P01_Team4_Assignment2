SELECT *
FROM {{ source(target.schema, 'POSITION_API') }}