-- raw source, no transformations

select *
from {{ source('openf1_raw', 'sessions') }}
