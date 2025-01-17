
SELECT
    DATE,
    DRIVER_NUMBER,
    COALESCE(LAP_NUMBER, 0) AS LAP_NUMBER,
    MEETING_KEY,
    COALESCE(PIT_DURATION, 0) AS PIT_DURATION,
    SESSION_KEY
FROM {{ ref('STG_API_PIT') }}

{% if is_incremental() %}
    {{ incremental_date_comparison() }}
{% endif %}
