SELECT
    CIRCUIT_KEY,
    CIRCUIT_SHORT_NAME,
    COUNTRY_CODE,
    COUNTRY_NAME,
    DATE_START,
    GMT_OFFSET,
    LOCATION,
    MEETING_KEY,
    MEETING_NAME,
    MEETING_OFFICIAL_NAME,
    YEAR
FROM {{ ref('STG_API_MEETINGS') }}

{% if is_incremental() %}
    {{ incremental_session_meeting_key() }}
{% endif %}