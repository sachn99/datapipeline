with valid_statuses as (
    select *,
           case
               when uuid is null then 'invalid'
               when status not in ('sent', 'delivered', 'read', 'failed', 'deleted') then 'invalid'
               else 'valid'
           end as validation_status
    from {{ ref('raw_statues') }}
),

filtered_statuses as (
    select *
    from valid_statuses
    where validation_status = 'valid'
)

select
    id,
    status,
    timestamp,
    uuid,
    message_uuid,
    number_id,
    inserted_at,
    updated_at,
    validation_status
from filtered_statuses
