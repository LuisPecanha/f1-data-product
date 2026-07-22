# Fixtures

`load_fixtures.py` populates the `bronze` schema of a DuckDB file with
hardcoded sample data — no OpenF1 API calls, no dlt. It exists so CI can run
`dbt run`, `dbt test`, and `datacontract test` against deterministic,
known-good data instead of depending on a live API call, which would make CI
slow, flaky, and non-reproducible.

By default it writes to `f1_fixtures.duckdb` at the project root (not the
real `f1.duckdb` the ingestion pipeline populates — running this script drops
and recreates the bronze tables, which would destroy real ingested data).
Override the target file with the `FIXTURES_DB_PATH` environment variable
(this is what CI does, pointing it at a fresh `f1.duckdb` since CI starts
with no database file at all).

## Coverage

The fixture is designed to exercise every gold model and every contract
quality check across all three data products:

- **`bronze.sessions`** (5 rows) — one row per `session_type`
  (Practice/Qualifying/Sprint/Race), plus a second `Race` row in a different
  year (2023) and circuit, to exercise the `year`/`session_type` filtering
  used throughout the pipeline.
- **`bronze.drivers`** (6 rows) — 2 drivers across 3 sessions each. Both
  drivers have a different `team_name` string in 2023 vs. 2024, exercising
  the year-over-year team-name dedup logic in `silver_drivers`.
- **`bronze.laps`** (36 rows) — 2 drivers, 12 laps in the Race session and 6
  laps in the Sprint session each. Includes 2 null-`lap_duration` pit-out
  laps and a second, longer pit-stop lap, so `is_pit_out_lap` and null
  handling are exercised.
- **`bronze.stints`** (8 rows) — 2 stints per driver per session, covering
  every lap. Includes `SOFT`, `MEDIUM`, `HARD`, and a `TEST_UNKNOWN` value to
  exercise the compound-fallback-to-`UNKNOWN` mapping in `silver_lap_times`.
- **`bronze.pit`** (4 rows) — 2 pit stops per driver in the Race session.
- **`bronze.race_control`** (4 rows) — 2 `SafetyCar` and 1 `VSC` event in
  the Race session, 1 `SafetyCar` event in the Sprint session, timed to fall
  within specific lap time windows so `under_safety_car` and `under_vsc`
  both resolve `true` for at least one lap each.

## Keeping this up to date

If the bronze schema changes (a column is renamed, added, or dropped in the
dlt ingestion pipeline, or a contract adds a new required field), this
fixture must be updated to match — otherwise CI will pass against a stale
shape that no longer reflects what the real pipeline loads. Cross-check
against `ingestion/pipeline.py` and the relevant contract in `contracts/`
before changing table shapes here.
