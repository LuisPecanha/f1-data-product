import argparse
import os
from pathlib import Path

import duckdb

# datacontract-cli has no support for servers.type: duckdb as a live
# database connection — it only executes quality checks against local
# file servers (csv/json/parquet/delta) or named DB connectors (postgres,
# snowflake, etc). Gold tables are exported to Parquet here so the
# contracts' servers.local block can point at real files datacontract
# test can actually read.
DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "f1.duckdb"
EXPORT_DIR = Path(__file__).resolve().parent.parent / "exports"

# contract model name -> gold table name (dbt model file names are
# prefixed with gold_, the contracts use the bare model name)
GOLD_MODELS = {
    "sessions": "gold_sessions",
    "lap_times": "gold_lap_times",
    "driver_season_stats": "gold_driver_season_stats",
}


def export_gold_to_parquet(db_path: Path):
    EXPORT_DIR.mkdir(exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    for contract_model, table_name in GOLD_MODELS.items():
        target = EXPORT_DIR / f"{contract_model}.parquet"
        con.execute(f"copy gold.{table_name} to '{target}' (format parquet)")
        print(f"Exported gold.{table_name} -> {target}")
    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-path",
        default=os.environ.get("DB_PATH", str(DEFAULT_DB_PATH)),
        help="Path to the DuckDB file to export gold tables from "
        "(defaults to $DB_PATH env var, then f1.duckdb at the project root)",
    )
    args = parser.parse_args()
    export_gold_to_parquet(Path(args.db_path))
