SELECT *
FROM {{ source(target.schema, 'MEETINGS_API') }}