--Elvis: Decided to drop first_name / last_name / headshot_url since full_name is already created and headshot_url cannot provide valueable insights
SELECT
    COALESCE(broadcast_name, '') AS broadcast_name,
    COALESCE(country_code, NULL) AS country_code,
    COALESCE(driver_number, 0) AS driver_number,
    COALESCE(full_name, '') AS full_name,
    COALESCE(meeting_key, '') AS meeting_key,
    COALESCE(name_acronym, '') AS name_acronym,
    COALESCE(session_key, '') AS session_key,
    COALESCE(team_colour, '') AS team_colour,
    COALESCE(team_name, '') AS team_name
FROM {{ ref('stg_api_drivers') }}
{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}
