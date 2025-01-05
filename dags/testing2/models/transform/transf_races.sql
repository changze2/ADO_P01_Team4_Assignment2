with renamed_races as (
    {{ rename_circuit_id('stg_races') }}  -- Apply the macro on the transform model
)

select * from renamed_races