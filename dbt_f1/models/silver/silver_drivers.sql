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
        -- prefer a record with a resolvable team_name over the merely
        -- most-recent one: some sessions (e.g. a young-driver test outing)
        -- return a driver record with team_name null, and picking that as
        -- "most recent" would surface a null team_name even when an
        -- earlier session in the same year has the real team on file
        row_number() over (
            partition by driver_number, year
            order by (team_name is not null) desc, session_key desc
        ) as rn
    from drivers_with_year

)

-- per the contract's fallback rule for driver_name/team_name resolution,
-- a driver-year with no resolvable team_name anywhere in the source data
-- (not just on the most recent record) must not be emitted with a null
-- team_name — excluded here rather than passed through, since building
-- a full quarantine table is out of scope for this model
select
    driver_number,
    year,
    driver_name,
    team_name
from deduplicated
where rn = 1
  and team_name is not null
