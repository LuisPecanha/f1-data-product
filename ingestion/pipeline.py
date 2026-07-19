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


def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="f1_sessions",
        destination=dlt.destinations.duckdb(credentials=str(DB_PATH)),
        dataset_name="bronze",
    )
    load_info = pipeline.run(openf1_source())
    print(load_info)
    return load_info


if __name__ == "__main__":
    run_pipeline()
