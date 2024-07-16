-- duplicate_checks.sql

with base as (
    select *,
        lag(inserted_at) over (partition by content order by inserted_at) as previous_inserted_at
    from {{ ref('stg_messages') }}
)

select content, count(*) as duplicate_count
from base
where previous_inserted_at is not null and
      abs(extract(epoch from (inserted_at - previous_inserted_at))) < 5
group by content
having count(*) > 1;
