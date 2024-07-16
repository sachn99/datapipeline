WITH latest_status AS (
    SELECT
        message_uuid,
        status AS last_status,
        timestamp AS last_status_timestamp
    FROM {{ ref('staging_statuses') }}
    WHERE (message_uuid, timestamp) IN (
        SELECT 
            message_uuid, 
            MAX(timestamp)
        FROM {{ ref('staging_statuses') }}
        GROUP BY message_uuid
    )
),
non_duplicates AS (
    SELECT *
    FROM {{ ref('staging_messages') }}
    WHERE is_duplicate = FALSE
)
SELECT
    m.id,
    m.message_type,
    m.masked_addressees,
    m.masked_author,
    m.content,
    m.author_type,
    m.direction,
    m.external_id,
    m.external_timestamp,
    m.masked_from_addr,
    m.is_deleted,
    m.rendered_content,
    m.source_type,
    m.uuid,
    COALESCE(ls.last_status, 'unknown') AS last_status,
    COALESCE(ls.last_status_timestamp, m.updated_at) AS last_status_timestamp,
    m.inserted_at,
    m.updated_at
FROM non_duplicates m
LEFT JOIN latest_status ls ON m.uuid = ls.message_uuid
