with drivers as (

    select
        driver_number,
        full_name,
        team_name,
        session_key
    from {{ ref('bronze_drivers') }}
    where driver_number is not null

),

drivers_with_year as (

    -- OpenF1 /v1/drivers has no year field of its own; the season is
    -- resolved via the session_key join to gold.sessions, per the
    -- contract's driverNameResolution / teamNameResolution method
    select
        drivers.driver_number,
        drivers.full_name,
        drivers.team_name,
        drivers.session_key,
        sessions.year
    from drivers
    inner join {{ ref('gold_sessions') }} as sessions
        on drivers.session_key = sessions.session_key

),

deduplicated as (

    select
        cast(driver_number as integer) as driver_number,
        cast(year as integer) as year,
        cast(full_name as varchar) as driver_name,
        cast(team_name as varchar) as team_name,
        row_number() over (
            partition by driver_number, year
            order by session_key desc
        ) as rn
    from drivers_with_year

)

select
    driver_number,
    year,
    driver_name,
    team_name
from deduplicated
where rn = 1
