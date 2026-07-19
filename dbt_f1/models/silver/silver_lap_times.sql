with laps as (

    select
        session_key,
        driver_number,
        lap_number,
        date_start,
        lap_duration,
        duration_sector_1,
        duration_sector_2,
        duration_sector_3,
        is_pit_out_lap
    from {{ ref('bronze_laps') }}

),

lap_windows as (

    -- per the lap_times contract's derivation logic: a lap's time window
    -- runs from its own date_start up to the next lap's date_start for the
    -- same driver, or the session's date_end for the final lap of the
    -- session (bronze_laps carries no per-lap date_end field)
    select
        laps.session_key,
        laps.driver_number,
        laps.lap_number,
        laps.date_start,
        laps.lap_duration,
        laps.duration_sector_1,
        laps.duration_sector_2,
        laps.duration_sector_3,
        laps.is_pit_out_lap,
        coalesce(
            lead(laps.date_start) over (
                partition by laps.session_key, laps.driver_number
                order by laps.lap_number
            ),
            sessions.date_end
        ) as lap_window_end
    from laps
    left join {{ ref('gold_sessions') }} as sessions
        on laps.session_key = sessions.session_key

),

stints as (

    select
        session_key,
        driver_number,
        compound,
        lap_start,
        lap_end
    from {{ ref('bronze_stints') }}

),

pit_stops as (

    select
        session_key,
        driver_number,
        lap_number,
        lane_duration as pit_duration
    from {{ ref('bronze_pit') }}

),

safety_car_events as (

    select
        session_key,
        date
    from {{ ref('bronze_race_control') }}
    where category = 'SafetyCar'

),

vsc_events as (

    select
        session_key,
        date
    from {{ ref('bronze_race_control') }}
    where category = 'VSC'

),

safety_car_flags as (

    select
        lap_windows.session_key,
        lap_windows.driver_number,
        lap_windows.lap_number,
        bool_or(safety_car_events.date is not null) as under_safety_car,
        bool_or(vsc_events.date is not null) as under_vsc
    from lap_windows
    left join safety_car_events
        on lap_windows.session_key = safety_car_events.session_key
        and safety_car_events.date >= lap_windows.date_start
        and safety_car_events.date < lap_windows.lap_window_end
    left join vsc_events
        on lap_windows.session_key = vsc_events.session_key
        and vsc_events.date >= lap_windows.date_start
        and vsc_events.date < lap_windows.lap_window_end
    group by
        lap_windows.session_key,
        lap_windows.driver_number,
        lap_windows.lap_number

),

joined as (

    select
        lap_windows.session_key,
        lap_windows.driver_number,
        lap_windows.lap_number,
        lap_windows.lap_duration,
        lap_windows.duration_sector_1,
        lap_windows.duration_sector_2,
        lap_windows.duration_sector_3,
        lap_windows.is_pit_out_lap,
        stints.compound,
        pit_stops.pit_duration,
        safety_car_flags.under_safety_car,
        safety_car_flags.under_vsc
    from lap_windows
    left join stints
        on lap_windows.session_key = stints.session_key
        and lap_windows.driver_number = stints.driver_number
        and lap_windows.lap_number between stints.lap_start and stints.lap_end
    left join pit_stops
        on lap_windows.session_key = pit_stops.session_key
        and lap_windows.driver_number = pit_stops.driver_number
        and lap_windows.lap_number = pit_stops.lap_number
    left join safety_car_flags
        on lap_windows.session_key = safety_car_flags.session_key
        and lap_windows.driver_number = safety_car_flags.driver_number
        and lap_windows.lap_number = safety_car_flags.lap_number
    where lap_windows.session_key is not null
      and lap_windows.driver_number is not null
      and lap_windows.lap_number is not null
      and lap_windows.lap_number > 0

),

casted as (

    select
        cast(session_key as integer) as session_key,
        cast(driver_number as integer) as driver_number,
        cast(lap_number as integer) as lap_number,
        cast(lap_duration as decimal(10, 3)) as lap_duration,
        cast(duration_sector_1 as decimal(10, 3)) as duration_sector_1,
        cast(duration_sector_2 as decimal(10, 3)) as duration_sector_2,
        cast(duration_sector_3 as decimal(10, 3)) as duration_sector_3,
        -- any raw compound outside the accepted enum (e.g. a testing
        -- compound like 'TEST_UNKNOWN') must fall back to UNKNOWN, not
        -- pass through unrecognised — see the compound field in the
        -- lap_times contract
        case
            when upper(cast(compound as varchar)) in ('SOFT', 'MEDIUM', 'HARD', 'INTERMEDIATE', 'WET')
                then upper(cast(compound as varchar))
            else 'UNKNOWN'
        end as compound,
        coalesce(cast(is_pit_out_lap as boolean), false) as is_pit_out_lap,
        cast(pit_duration as decimal(10, 3)) as pit_duration,
        coalesce(cast(under_safety_car as boolean), false) as under_safety_car,
        coalesce(cast(under_vsc as boolean), false) as under_vsc
    from joined

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by session_key, driver_number, lap_number
            order by lap_duration asc nulls last
        ) as rn
    from casted

)

select
    session_key,
    driver_number,
    lap_number,
    lap_duration,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    compound,
    is_pit_out_lap,
    pit_duration,
    under_safety_car,
    under_vsc
from deduplicated
where rn = 1
