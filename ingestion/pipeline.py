import time
from pathlib import Path

import dlt
import duckdb
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


# OpenF1 supports range filters (session_key>=X&session_key<=Y) that return
# data for every session in that range in a single call. Fetching strictly
# one request per session (490 sessions x 4 endpoints = ~1960 requests) was
# far too slow and tripped rate limiting; batching session_keys into ranges
# cuts that to ~20 requests per endpoint.
SESSION_BATCH_SIZE = 25


def _chunk(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _fetch_for_session_range(endpoint, min_key, max_key, max_retries=5):
    # OpenF1 returns 422 on these endpoints when called without filters, so
    # they must be filtered. It also returns 404 (rather than 200 with an
    # empty list) for a range with no matching records — treat both
    # empty-list and 404 as "nothing in this batch" rather than an error.
    # Retries/backoff cover rate limiting (429) and occasional transient
    # connection drops under load.
    last_error = None
    for attempt in range(max_retries):
        try:
            response = requests.get(
                f"{BASE_URL}{endpoint}?session_key>={min_key}&session_key<={max_key}",
                timeout=120,
            )
        except requests.exceptions.RequestException as exc:
            last_error = exc
            time.sleep(2**attempt)
            continue
        if response.status_code == 404:
            return []
        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 2**attempt))
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        return response.json()
    raise last_error or RuntimeError(
        f"Failed to fetch {endpoint} for session_key range [{min_key},{max_key}]"
    )


@dlt.source
def openf1_lap_times_source(session_keys):
    batches = list(_chunk(sorted(session_keys), SESSION_BATCH_SIZE))

    @dlt.resource(
        primary_key=["session_key", "driver_number", "lap_number"],
        write_disposition="replace",
    )
    def laps():
        for batch in batches:
            data = _fetch_for_session_range("/v1/laps", batch[0], batch[-1])
            if not data:
                continue
            yield data

    @dlt.resource(
        primary_key=["session_key", "driver_number", "lap_number"],
        write_disposition="replace",
    )
    def pit():
        for batch in batches:
            data = _fetch_for_session_range("/v1/pit", batch[0], batch[-1])
            if not data:
                continue
            yield data

    @dlt.resource(
        primary_key=["session_key", "driver_number", "stint_number"],
        write_disposition="replace",
    )
    def stints():
        for batch in batches:
            data = _fetch_for_session_range("/v1/stints", batch[0], batch[-1])
            if not data:
                continue
            yield data

    @dlt.resource(
        primary_key=["session_key", "date"],
        write_disposition="replace",
    )
    def race_control():
        for batch in batches:
            data = _fetch_for_session_range("/v1/race_control", batch[0], batch[-1])
            if not data:
                continue
            yield data

    return laps, pit, stints, race_control


@dlt.source
def openf1_drivers_source():

    @dlt.resource(
        primary_key=["session_key", "driver_number"],
        write_disposition="replace",
    )
    def drivers():
        response = requests.get(f"{BASE_URL}/v1/drivers", timeout=120)
        response.raise_for_status()
        yield response.json()

    return drivers


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
    con = duckdb.connect(str(DB_PATH), read_only=True)
    session_keys = [
        row[0] for row in con.execute("select distinct session_key from bronze.sessions").fetchall()
    ]
    con.close()

    pipeline = dlt.pipeline(
        pipeline_name="f1_lap_times",
        destination=dlt.destinations.duckdb(credentials=str(DB_PATH)),
        dataset_name="bronze",
    )
    load_info = pipeline.run(openf1_lap_times_source(session_keys))
    print(load_info)
    return load_info


def run_drivers_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="f1_drivers",
        destination=dlt.destinations.duckdb(credentials=str(DB_PATH)),
        dataset_name="bronze",
    )
    load_info = pipeline.run(openf1_drivers_source())
    print(load_info)
    return load_info


if __name__ == "__main__":
    run_sessions_pipeline()
    run_lap_times_pipeline()
    run_drivers_pipeline()
