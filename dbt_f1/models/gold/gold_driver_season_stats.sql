with race_laps as (

    select
        lap_times.driver_number,
        sessions.year,
        lap_times.lap_duration,
        lap_times.pit_duration,
        lap_times.under_safety_car,
        lap_times.under_vsc,
        lap_times.compound
    from {{ ref('gold_lap_times') }} as lap_times
    inner join {{ ref('gold_sessions') }} as sessions
        on lap_times.session_key = sessions.session_key
    where sessions.session_type = 'Race'

),

all_laps as (

    select
        lap_times.driver_number,
        sessions.year
    from {{ ref('gold_lap_times') }} as lap_times
    inner join {{ ref('gold_sessions') }} as sessions
        on lap_times.session_key = sessions.session_key

),

driver_meta as (

    select
        driver_number,
        year,
        driver_name,
        team_name
    from {{ ref('silver_drivers') }}

),

aggregated as (

    select
        driver_number,
        year,
        count(*) as total_race_laps,
        avg(lap_duration) as avg_lap_duration_seconds,
        min(lap_duration) as fastest_lap_seconds,
        count(pit_duration) as total_pit_stops,
        avg(pit_duration) as avg_pit_duration_seconds,
        sum(case when under_safety_car then 1 else 0 end) as laps_under_safety_car,
        sum(case when under_vsc then 1 else 0 end) as laps_under_vsc,
        sum(case when compound = 'SOFT' then 1 else 0 end) as soft_lap_count,
        sum(case when compound = 'MEDIUM' then 1 else 0 end) as medium_lap_count,
        sum(case when compound = 'HARD' then 1 else 0 end) as hard_lap_count
    from race_laps
    group by driver_number, year

),

total_laps_cte as (

    select
        driver_number,
        year,
        count(*) as total_laps
    from all_laps
    group by driver_number, year

)

-- anchored on driver_meta (not an inner join across all four CTEs): a
-- driver with zero race laps in a season is a valid state per the
-- contract (total_race_laps = 0 is explicitly allowed), so aggregated
-- and total_laps_cte are left-joined and their required count columns
-- coalesced to 0 rather than dropping the driver's row entirely
select
    driver_meta.driver_number,
    driver_meta.year,
    driver_meta.driver_name,
    driver_meta.team_name,
    cast(coalesce(total_laps_cte.total_laps, 0) as integer) as total_laps,
    cast(coalesce(aggregated.total_race_laps, 0) as integer) as total_race_laps,
    cast(aggregated.avg_lap_duration_seconds as decimal(10, 3)) as avg_lap_duration_seconds,
    cast(aggregated.fastest_lap_seconds as decimal(10, 3)) as fastest_lap_seconds,
    cast(coalesce(aggregated.total_pit_stops, 0) as integer) as total_pit_stops,
    cast(aggregated.avg_pit_duration_seconds as decimal(10, 3)) as avg_pit_duration_seconds,
    cast(coalesce(aggregated.laps_under_safety_car, 0) as integer) as laps_under_safety_car,
    cast(coalesce(aggregated.laps_under_vsc, 0) as integer) as laps_under_vsc,
    cast(coalesce(aggregated.soft_lap_count, 0) as integer) as soft_lap_count,
    cast(coalesce(aggregated.medium_lap_count, 0) as integer) as medium_lap_count,
    cast(coalesce(aggregated.hard_lap_count, 0) as integer) as hard_lap_count
from driver_meta
left join total_laps_cte
    on driver_meta.driver_number = total_laps_cte.driver_number
    and driver_meta.year = total_laps_cte.year
left join aggregated
    on driver_meta.driver_number = aggregated.driver_number
    and driver_meta.year = aggregated.year
