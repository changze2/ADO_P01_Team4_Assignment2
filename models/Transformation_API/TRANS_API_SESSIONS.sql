-- Elvis: Dropped circuit_key, circuit_short_name, country_code, country_key, country_name, location 
-- since they can be referenced by joining this table with meetings by meeting_key


SELECT
    date_end,
    date_start,
    gmt_offset,           
    meeting_key,
    session_key,
    session_name,
    session_type,
    year,
FROM {{ ref('stg_sessions_api') }}
{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}