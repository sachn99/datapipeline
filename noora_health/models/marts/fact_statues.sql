WITH non_duplicates AS (
    SELECT *
    FROM {{ ref('staging_messages') }}
    WHERE is_duplicate = FALSE
)
SELECT
    s.status,
    COUNT(*) AS status_count
FROM {{ ref('staging_statuses') }} s
JOIN non_duplicates m ON s.message_uuid = m.uuid
GROUP BY s.status