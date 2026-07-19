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
from {{ ref('silver_lap_times') }}
