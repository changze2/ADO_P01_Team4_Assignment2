SELECT *
FROM {{ source(target.schema, 'RACE_CONTROL_API') }}