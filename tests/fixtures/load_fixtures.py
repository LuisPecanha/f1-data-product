"""Loads hardcoded sample data into the bronze layer for CI.

No API calls, no dlt — just direct INSERTs into a DuckDB file, so dbt and
datacontract can be exercised end-to-end without hitting the live OpenF1 API.
See README.md in this directory for the fixture design and coverage.
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

# Defaults to a dedicated fixtures DB, not the real f1.duckdb used by the
# ingestion pipeline — this script drops and recreates bronze tables, which
# would destroy real ingested data if pointed at that file. Override via
# FIXTURES_DB_PATH (e.g. in CI, which starts with no f1.duckdb at all).
DB_PATH = Path(
    os.environ.get(
        "FIXTURES_DB_PATH",
        str(Path(__file__).resolve().parent.parent.parent / "f1_fixtures.duckdb"),
    )
)

BRONZE_TABLES = ["sessions", "drivers", "laps", "stints", "pit", "race_control"]


def _sector_splits(duration):
    if duration is None:
        return None, None, None
    sector_1 = round(duration * 0.32, 3)
    sector_2 = round(duration * 0.35, 3)
    sector_3 = round(duration - sector_1 - sector_2, 3)
    return sector_1, sector_2, sector_3


def _build_laps(session_key, driver_number, base_start, durations, pit_out_laps):
    """durations: list of lap_duration values (or None), one per lap, 1-indexed."""
    rows = []
    t = base_start
    for lap_number, duration in enumerate(durations, start=1):
        sector_1, sector_2, sector_3 = _sector_splits(duration)
        rows.append(
            (
                session_key,
                driver_number,
                lap_number,
                duration,
                sector_1,
                sector_2,
                sector_3,
                t,
                lap_number in pit_out_laps,
            )
        )
        # advance the clock by the lap's own duration; pit-out laps have no
        # duration, so fall back to a typical lap-length spacing
        t = t + timedelta(seconds=(duration if duration is not None else 90.0))
    return rows


def _drop_and_create_bronze(con):
    con.execute("create schema if not exists bronze")
    for table in BRONZE_TABLES:
        con.execute(f"drop table if exists bronze.{table}")


def _load_sessions(con):
    con.execute("""
        create table bronze.sessions (
            session_key integer,
            session_name varchar,
            session_type varchar,
            date_start timestamptz,
            date_end timestamptz,
            year integer,
            circuit_short_name varchar,
            country_name varchar,
            location varchar
        )
    """)
    rows = [
        (1, "Practice 1", "Practice", "2024-05-24 12:30:00+00", "2024-05-24 13:30:00+00", 2024, "Monaco", "Monaco", "Monte Carlo"),
        (2, "Qualifying", "Qualifying", "2024-05-25 14:00:00+00", "2024-05-25 15:00:00+00", 2024, "Monaco", "Monaco", "Monte Carlo"),
        (3, "Sprint", "Sprint", "2024-05-25 16:30:00+00", "2024-05-25 17:30:00+00", 2024, "Monaco", "Monaco", "Monte Carlo"),
        (4, "Race", "Race", "2024-05-26 13:00:00+00", "2024-05-26 15:00:00+00", 2024, "Monaco", "Monaco", "Monte Carlo"),
        (5, "Race", "Race", "2023-07-09 14:00:00+00", "2023-07-09 16:00:00+00", 2023, "Silverstone", "United Kingdom", "Silverstone"),
    ]
    con.executemany("insert into bronze.sessions values (?,?,?,?,?,?,?,?,?)", rows)


def _load_drivers(con):
    con.execute("""
        create table bronze.drivers (
            session_key integer,
            driver_number integer,
            full_name varchar,
            team_name varchar
        )
    """)
    rows = [
        # 2024: sessions 1 and 3
        (1, 1, "Max VERSTAPPEN", "Red Bull Racing"),
        (1, 44, "Lewis HAMILTON", "Mercedes"),
        (3, 1, "Max VERSTAPPEN", "Red Bull Racing"),
        (3, 44, "Lewis HAMILTON", "Mercedes"),
        # 2023: session 5 — team_name differs from the 2024 rows above for
        # both drivers, to exercise the year-over-year team name dedup
        (5, 1, "Max VERSTAPPEN", "Oracle Red Bull Racing"),
        (5, 44, "Lewis HAMILTON", "Mercedes-AMG Petronas F1 Team"),
    ]
    con.executemany("insert into bronze.drivers values (?,?,?,?)", rows)


def _load_laps(con):
    con.execute("""
        create table bronze.laps (
            session_key integer,
            driver_number integer,
            lap_number integer,
            lap_duration double,
            duration_sector_1 double,
            duration_sector_2 double,
            duration_sector_3 double,
            date_start timestamptz,
            is_pit_out_lap boolean
        )
    """)

    # session 4 (Race): 12 laps each driver. Lap 1 is a pit-out lap (null
    # duration); lap 7 is a second pit stop (longer, anomalous duration).
    race_durations = [
        None, 89.456, 90.123, 91.234, 88.789, 92.045,
        115.234, 89.901, 91.033, 93.267, 90.045, 89.012,
    ]
    race_base = datetime(2024, 5, 26, 13, 0, 0, tzinfo=timezone.utc)
    race_pit_out_laps = {1, 7}

    # session 3 (Sprint): 6 laps each driver, no pit stops.
    sprint_durations = [88.012, 89.234, 90.056, 91.345, 88.678, 90.234]
    sprint_base = datetime(2024, 5, 25, 16, 30, 0, tzinfo=timezone.utc)

    rows = []
    for driver_number in (1, 44):
        rows += _build_laps(4, driver_number, race_base, race_durations, race_pit_out_laps)
        rows += _build_laps(3, driver_number, sprint_base, sprint_durations, set())

    con.executemany(
        "insert into bronze.laps values (?,?,?,?,?,?,?,?,?)",
        rows,
    )


def _load_stints(con):
    con.execute("""
        create table bronze.stints (
            session_key integer,
            driver_number integer,
            stint_number integer,
            lap_start integer,
            lap_end integer,
            compound varchar
        )
    """)
    rows = [
        # session 4 (Race), 12 laps
        (4, 1, 1, 1, 6, "SOFT"),
        (4, 1, 2, 7, 12, "MEDIUM"),
        (4, 44, 1, 1, 6, "MEDIUM"),
        (4, 44, 2, 7, 12, "HARD"),
        # session 3 (Sprint), 6 laps — TEST_UNKNOWN exercises the compound
        # fallback-to-UNKNOWN mapping in silver_lap_times
        (3, 1, 1, 1, 3, "SOFT"),
        (3, 1, 2, 4, 6, "TEST_UNKNOWN"),
        (3, 44, 1, 1, 3, "MEDIUM"),
        (3, 44, 2, 4, 6, "HARD"),
    ]
    con.executemany("insert into bronze.stints values (?,?,?,?,?,?)", rows)


def _load_pit(con):
    con.execute("""
        create table bronze.pit (
            session_key integer,
            driver_number integer,
            lap_number integer,
            lane_duration double
        )
    """)
    rows = [
        (4, 1, 1, 23.456),
        (4, 1, 7, 24.789),
        (4, 44, 1, 22.901),
        (4, 44, 7, 26.345),
    ]
    con.executemany("insert into bronze.pit values (?,?,?,?)", rows)


def _load_race_control(con):
    con.execute("""
        create table bronze.race_control (
            session_key integer,
            category varchar,
            date timestamptz
        )
    """)
    rows = [
        # session 4 (Race): two SafetyCar periods and one VSC period,
        # timed to fall within specific lap windows (see README.md)
        (4, "SafetyCar", "2024-05-26 13:06:30+00"),
        (4, "SafetyCar", "2024-05-26 13:13:00+00"),
        (4, "VSC", "2024-05-26 13:14:00+00"),
        # session 3 (Sprint): one SafetyCar period
        (3, "SafetyCar", "2024-05-25 16:33:20+00"),
    ]
    con.executemany("insert into bronze.race_control values (?,?,?)", rows)


def load_fixtures():
    con = duckdb.connect(str(DB_PATH))
    try:
        _drop_and_create_bronze(con)
        _load_sessions(con)
        _load_drivers(con)
        _load_laps(con)
        _load_stints(con)
        _load_pit(con)
        _load_race_control(con)
    finally:
        con.close()
    print(f"Loaded fixtures into {DB_PATH}")


if __name__ == "__main__":
    load_fixtures()
