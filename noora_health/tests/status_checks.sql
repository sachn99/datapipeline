-- status_checks.sql
select *
from {{ ref('stg_statuses') }}
where uuid is null or status is null or timestamp is null;
