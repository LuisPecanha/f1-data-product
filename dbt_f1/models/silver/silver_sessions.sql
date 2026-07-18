with deduplicated as (

    select
        cast(session_key as integer) as session_key,
        cast(session_name as varchar) as session_name,
        cast(session_type as varchar) as session_type,
        cast(date_start as timestamp) as date_start,
        cast(date_end as timestamp) as date_end,
        cast(year as integer) as year,
        coalesce(cast(circuit_short_name as varchar), 'Unknown') as circuit_short_name,
        cast(country_name as varchar) as country_name,
        coalesce(cast(location as varchar), 'Unknown') as location,
        row_number() over (
            partition by session_key
            order by date_start desc
        ) as rn
    from {{ ref('bronze_sessions') }}
    where session_key is not null

)

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
from deduplicated
where rn = 1
