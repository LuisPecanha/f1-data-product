-- raw source, no transformations

select *
from {{ source('openf1_raw', 'race_control') }}
