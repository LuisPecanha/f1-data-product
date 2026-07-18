select
    session_key,
    session_name,
    session_type,
    date_start,
    date_end,
    year,
    circuit_short_name,
    country_name,
    location
from {{ ref('silver_sessions') }}
