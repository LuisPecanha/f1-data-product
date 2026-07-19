from pathlib import Path

import dlt
import requests

BASE_URL = "https://api.openf1.org"

# f1.duckdb lives at the project root, one level up from this ingestion
# directory — resolved from the script location so the path is correct
# regardless of the caller's current working directory.
DB_PATH = Path(__file__).resolve().parent.parent / "f1.duckdb"


@dlt.source
def openf1_source():

    @dlt.resource(primary_key="session_key", write_disposition="replace")
    def sessions():
        response = requests.get(f"{BASE_URL}/v1/sessions")
        response.raise_for_status()
        yield response.json()

    return sessions


@dlt.source
def openf1_lap_times_source():

    # /v1/laps and /v1/pit return the full historical dataset when called
    # without query params (laps alone is ~110MB). Fine for now; in future
    # these should be filtered by session_key to avoid a full reload on
    # every run.
    @dlt.resource(
        primary_key=["session_key", "driver_number", "lap_number"],
        write_disposition="replace",
    )
    def laps():
        response = requests.get(f"{BASE_URL}/v1/laps", timeout=120)
        response.raise_for_status()
        yield response.json()

    @dlt.resource(
        primary_key=["session_key", "driver_number", "lap_number"],
        write_disposition="replace",
    )
    def pit():
        response = requests.get(f"{BASE_URL}/v1/pit", timeout=120)
        response.raise_for_status()
        yield response.json()

    @dlt.resource(
        primary_key=["session_key", "driver_number", "stint_number"],
        write_disposition="replace",
    )
    def stints():
        response = requests.get(f"{BASE_URL}/v1/stints", timeout=120)
        response.raise_for_status()
        yield response.json()

    @dlt.resource(
        primary_key=["session_key", "date"],
        write_disposition="replace",
    )
    def race_control():
        response = requests.get(f"{BASE_URL}/v1/race_control", timeout=120)
        response.raise_for_status()
        yield response.json()

    return laps, pit, stints, race_control


def run_sessions_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="f1_sessions",
        destination=dlt.destinations.duckdb(credentials=str(DB_PATH)),
        dataset_name="bronze",
    )
    load_info = pipeline.run(openf1_source())
    print(load_info)
    return load_info


def run_lap_times_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="f1_lap_times",
        destination=dlt.destinations.duckdb(credentials=str(DB_PATH)),
        dataset_name="bronze",
    )
    load_info = pipeline.run(openf1_lap_times_source())
    print(load_info)
    return load_info


if __name__ == "__main__":
    run_sessions_pipeline()
    run_lap_times_pipeline()
