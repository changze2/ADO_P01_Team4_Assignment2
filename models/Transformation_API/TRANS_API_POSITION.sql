SELECT
    DATE,
    DRIVER_NUMBER,
    MEETING_KEY,
    POSITION,
    SESSION_KEY
FROM {{ ref('STG_API_POSITION') }}

{% if is_incremental() %}
    {{ incremental_date_comparison() }}
{% endif %}