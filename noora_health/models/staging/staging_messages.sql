WITH ranked_messages AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY content ORDER BY inserted_at) AS row_num
    FROM
        {{ ref('raw_messages') }}
),
flagged_duplicates AS (
    SELECT
        *,
        CASE
            WHEN row_num > 1 AND 
                 EXTRACT(EPOCH FROM (inserted_at - LAG(inserted_at) OVER (PARTITION BY content ORDER BY inserted_at))) < 60
            THEN TRUE
            ELSE FALSE
        END AS is_duplicate
    FROM
        ranked_messages
)
SELECT
    id,
    message_type,
    masked_addressees,
    masked_author,
    content,
    author_type,
    direction,
    external_id,
    external_timestamp,
    masked_from_addr,
    is_deleted,
    rendered_content,
    source_type,
    uuid,
    last_status,
    last_status_timestamp,
    inserted_at,
    updated_at,
    is_duplicate
FROM
    flagged_duplicates
